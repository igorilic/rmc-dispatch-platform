//! Reconciling a `CustomerIntegration` (SPEC §7).
//!
//! The operator reconciles an integration whenever it changes, whenever the ConfigMap or Secret it
//! names changes, whenever its Deployment or CronJob changes, and every 30 s besides, because the
//! dead letters and the last ingest change without Kubernetes noticing. Each time it:
//! 1. checks the mapping and the credentials (`workload::check`);
//! 2. if they pass, applies the integration's objects with server-side apply. If they don't, it
//!    leaves the running workload alone, which keeps its last valid mapping;
//! 3. writes the status: the phase from `workload::phase`, and the dead letters and the last
//!    ingest from NATS. If NATS can't be read, the last known values stay and the message says so.
//!
//! Deleting an integration goes through a finalizer. Owner references would delete its objects
//! anyway, but only after the integration is gone. The finalizer stops the workload first, in the
//! foreground, and waits until it is gone. Then it removes the integration's last ingest from
//! NATS, the one thing Kubernetes can't garbage-collect, and only then lets the integration go.
//! That last step is best effort: if NATS is down, the stale entry is just a timestamp, and the
//! deletion shouldn't hang on it. Dead letters stay, so a re-created integration can replay them.

use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use ingest::nats::StatusReader;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::batch::v1::CronJob;
use k8s_openapi::api::core::v1::{ConfigMap, Secret, Service};
use k8s_openapi::api::networking::v1::Ingress;
use kube::api::{DeleteParams, PartialObjectMeta, Patch, PatchParams};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::finalizer::{Event, finalizer};
use kube::runtime::reflector::{ObjectRef, Store};
use kube::runtime::watcher;
use kube::{Api, Client, Resource, ResourceExt};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;
use tracing::{info, warn};

use crate::crd::{Channel, CustomerIntegration, CustomerIntegrationStatus, FINALIZER};
use crate::workload::{self, Checked, Observed, Settings};

/// ponytail: how often the status picks up new dead letters and ingests. Watching NATS instead
/// would make it immediate, at the cost of a second event source.
const STATUS_REFRESH: Duration = Duration::from_secs(30);
/// How often cleanup looks again while the workload is still stopping, and for how long.
/// ponytail: waits inline, since the finalizer helper releases the integration as soon as cleanup
/// returns Ok. A minute covers ingest's pods, which stop at once on SIGTERM; after that, cleanup
/// fails and the controller retries.
const CLEANUP_POLL: Duration = Duration::from_secs(2);
const CLEANUP_WAIT: Duration = Duration::from_secs(60);
const MANAGER: &str = "pourline-operator";

/// What every reconcile needs.
pub struct Context {
    pub client: Client,
    pub settings: Settings,
    pub nats: StatusReader,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Kube(#[from] kube::Error),
    #[error("the integration has no namespace")]
    NoNamespace,
    #[error("{0} is still stopping")]
    StillStopping(String),
    #[error("finalizer: {0}")]
    Finalizer(#[source] Box<kube::runtime::finalizer::Error<Error>>),
}

/// Runs the controller for every integration in `namespace` until `shutdown` completes.
pub async fn run(
    client: Client,
    namespace: &str,
    settings: Settings,
    nats: StatusReader,
    shutdown: impl Future<Output = ()> + Send + Sync + 'static,
) {
    let integrations = Api::<CustomerIntegration>::namespaced(client.clone(), namespace);
    let config = watcher::Config::default();
    let controller = Controller::new(integrations, config.clone());
    let store = controller.store();
    let by_config_map = referencing(store.clone(), |ci| &ci.spec.mapping_config_map);
    let by_secret = referencing(store, |ci| &ci.spec.credentials_secret);
    let context = Arc::new(Context {
        client: client.clone(),
        settings,
        nats,
    });
    controller
        .owns(
            Api::<Deployment>::namespaced(client.clone(), namespace),
            config.clone(),
        )
        .owns(
            Api::<CronJob>::namespaced(client.clone(), namespace),
            config.clone(),
        )
        .watches(
            Api::<PartialObjectMeta<ConfigMap>>::namespaced(client.clone(), namespace),
            config.clone(),
            by_config_map,
        )
        // Metadata only: the operator never holds the Secrets' values.
        .watches(
            Api::<PartialObjectMeta<Secret>>::namespaced(client, namespace),
            config,
            by_secret,
        )
        .graceful_shutdown_on(shutdown)
        .run(reconcile, error_policy, context)
        .for_each(|result| async move {
            if let Err(error) = result {
                warn!(%error, "reconcile failed");
            }
        })
        .await;
}

/// Maps a ConfigMap or Secret to the integrations that name it.
fn referencing<K: Resource>(
    store: Store<CustomerIntegration>,
    field: fn(&CustomerIntegration) -> &String,
) -> impl Fn(K) -> Vec<ObjectRef<CustomerIntegration>> + Send + Sync + 'static {
    move |object: K| {
        let name = object.meta().name.clone().unwrap_or_default();
        store
            .state()
            .iter()
            .filter(|ci| *field(ci) == name && ci.namespace() == object.meta().namespace)
            .map(|ci| ObjectRef::from_obj(ci.as_ref()))
            .collect()
    }
}

/// Reconciles one integration, with the finalizer around it.
pub async fn reconcile(ci: Arc<CustomerIntegration>, ctx: Arc<Context>) -> Result<Action, Error> {
    let namespace = ci.namespace().ok_or(Error::NoNamespace)?;
    let integrations = Api::<CustomerIntegration>::namespaced(ctx.client.clone(), &namespace);
    finalizer(&integrations, FINALIZER, ci, |event| async {
        match event {
            Event::Apply(ci) => apply(&ci, &ctx, &namespace).await,
            Event::Cleanup(ci) => cleanup(&ci, &ctx, &namespace).await,
        }
    })
    .await
    .map_err(|e| Error::Finalizer(Box::new(e)))
}

fn error_policy(ci: Arc<CustomerIntegration>, error: &Error, _: Arc<Context>) -> Action {
    warn!(integration = %ci.name_any(), %error, "reconcile failed, retrying in 10 s");
    Action::requeue(Duration::from_secs(10))
}

async fn apply(ci: &CustomerIntegration, ctx: &Context, namespace: &str) -> Result<Action, Error> {
    let client = &ctx.client;
    let config_map = Api::<ConfigMap>::namespaced(client.clone(), namespace)
        .get_opt(&ci.spec.mapping_config_map)
        .await?;
    let secret = Api::<Secret>::namespaced(client.clone(), namespace)
        .get_opt(&ci.spec.credentials_secret)
        .await?;
    let problem = match workload::check(ci, config_map.as_ref(), secret.as_ref()) {
        Ok(checked) => apply_objects(ci, ctx, namespace, &checked)
            .await
            .err()
            .map(|e| format!("the API server refused the workload: {e}")),
        Err(message) => Some(message),
    };
    let name = workload::name(ci);
    let (deployment, cron_job) = match ci.spec.channel {
        Channel::Webhook => (
            Api::<Deployment>::namespaced(client.clone(), namespace)
                .get_opt(&name)
                .await?,
            None,
        ),
        Channel::Sftp => (
            None,
            Api::<CronJob>::namespaced(client.clone(), namespace)
                .get_opt(&name)
                .await?,
        ),
    };
    let observed = match ci.spec.channel {
        Channel::Webhook => Observed::Deployment(deployment.as_ref()),
        Channel::Sftp => Observed::CronJob(cron_job.as_ref()),
    };
    let (phase, mut message) = workload::phase(ci, problem.as_deref(), observed);

    let previous = ci.status.clone().unwrap_or_default();
    let mut status = CustomerIntegrationStatus {
        phase,
        message: None,
        last_successful_ingest: previous.last_successful_ingest,
        dead_letter_count: previous.dead_letter_count,
    };
    match ctx.nats.status(&ci.name_any()).await {
        Ok(recorded) => {
            status.last_successful_ingest = recorded.last_successful_ingest;
            status.dead_letter_count = recorded.dead_letters;
        }
        Err(error) => {
            let nats = format!("can't read the dead letters and last ingest from NATS: {error}");
            message = Some(message.map_or(nats.clone(), |m| format!("{m}; {nats}")));
        }
    }
    status.message = message;
    if ci.status.as_ref() != Some(&status) {
        info!(integration = %ci.name_any(), phase = ?status.phase, message = ?status.message, "status");
        // Server-side apply, so fields that are now empty, such as an old message, disappear.
        let patch = json!({
            "apiVersion": CustomerIntegration::api_version(&()),
            "kind": CustomerIntegration::kind(&()),
            "status": status,
        });
        Api::<CustomerIntegration>::namespaced(ctx.client.clone(), namespace)
            .patch_status(
                &ci.name_any(),
                &PatchParams::apply(MANAGER).force(),
                &Patch::Apply(patch),
            )
            .await?;
    }
    Ok(Action::requeue(STATUS_REFRESH))
}

async fn apply_objects(
    ci: &CustomerIntegration,
    ctx: &Context,
    namespace: &str,
    checked: &Checked,
) -> Result<(), kube::Error> {
    let client = &ctx.client;
    let settings = &ctx.settings;
    ssa(
        client,
        namespace,
        &workload::mapping_config_map(ci, checked),
    )
    .await?;
    match ci.spec.channel {
        Channel::Webhook => {
            ssa(
                client,
                namespace,
                &workload::deployment(ci, settings, checked),
            )
            .await?;
            ssa(client, namespace, &workload::service(ci)).await?;
            ssa(client, namespace, &workload::ingress(ci)).await?;
        }
        Channel::Sftp => {
            ssa(
                client,
                namespace,
                &workload::cron_job(ci, settings, checked),
            )
            .await?;
        }
    }
    Ok(())
}

/// Server-side apply: the operator owns exactly the fields it sets.
async fn ssa<K>(client: &Client, namespace: &str, object: &K) -> Result<(), kube::Error>
where
    K: Resource<DynamicType = (), Scope = k8s_openapi::NamespaceResourceScope>
        + Clone
        + Debug
        + DeserializeOwned
        + Serialize,
{
    let name = object.meta().name.clone().unwrap_or_default();
    Api::<K>::namespaced(client.clone(), namespace)
        .patch(
            &name,
            &PatchParams::apply(MANAGER).force(),
            &Patch::Apply(object),
        )
        .await?;
    Ok(())
}

async fn cleanup(
    ci: &CustomerIntegration,
    ctx: &Context,
    namespace: &str,
) -> Result<Action, Error> {
    let name = workload::name(ci);
    let client = &ctx.client;
    // Both kinds, in case the channel's objects were never created or a deletion was interrupted.
    let deadline = tokio::time::Instant::now() + CLEANUP_WAIT;
    while still_there::<Deployment>(client, namespace, &name).await?
        | still_there::<CronJob>(client, namespace, &name).await?
    {
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::StillStopping(name));
        }
        tokio::time::sleep(CLEANUP_POLL).await;
    }
    // These run nothing, so owner references may delete them afterwards; doing it now is tidier.
    still_there::<Service>(client, namespace, &name).await?;
    still_there::<Ingress>(client, namespace, &name).await?;
    still_there::<ConfigMap>(client, namespace, &name).await?;
    if let Err(error) = ctx.nats.forget(&ci.name_any()).await {
        warn!(integration = %ci.name_any(), %error, "couldn't remove the last ingest from NATS, leaving it");
    }
    info!(integration = %ci.name_any(), "cleaned up");
    Ok(Action::await_change())
}

/// Deletes an object in the foreground, so its dependants (a Deployment's pods, a CronJob's Jobs)
/// go first. True while it still exists.
async fn still_there<K>(client: &Client, namespace: &str, name: &str) -> Result<bool, kube::Error>
where
    K: Resource<DynamicType = (), Scope = k8s_openapi::NamespaceResourceScope>
        + Clone
        + Debug
        + DeserializeOwned,
{
    let api = Api::<K>::namespaced(client.clone(), namespace);
    match api.delete(name, &DeleteParams::foreground()).await {
        Ok(_) => {}
        Err(kube::Error::Api(status)) if status.is_not_found() => return Ok(false),
        Err(error) => return Err(error),
    }
    // Asked, not inferred from the answer: deleting an object that is already terminating
    // doesn't return it, though it lingers until its dependants are gone.
    Ok(api.get_opt(name).await?.is_some())
}
