//! The controller against a real API server (k3s) and NATS, both in testcontainers.
//!
//! k3s runs without an agent, so no pod ever starts. These tests check the objects and the
//! statuses the operator writes; the e2e test (`make e2e`) checks that the workloads run.

// Integration tests are test code; SPEC §15 allows unwrap there.
#![allow(clippy::unwrap_used)]

use std::future::Future;
use std::path::Path;
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use domain::Seed;
use ingest::mapping::{Delivery, Mapping};
use ingest::nats::{Bus, StatusReader};
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::batch::v1::CronJob;
use k8s_openapi::api::core::v1::{ConfigMap, Secret, Service};
use k8s_openapi::api::networking::v1::Ingress;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::api::{DeleteParams, Patch, PatchParams, PostParams};
use kube::config::{KubeConfigOptions, Kubeconfig};
use kube::{Api, Client, CustomResourceExt, ResourceExt};
use operator::crd::{CustomerIntegration, CustomerIntegrationStatus, Phase};
use operator::workload::Settings;
use serde_json::{Value, json};
use testcontainers::core::{ExecCommand, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};

/// The k3s that k3d runs, as its API server only.
async fn k3s() -> (ContainerAsync<GenericImage>, Client) {
    let container = GenericImage::new("rancher/k3s", "v1.37.2-k3s1")
        .with_exposed_port(6443.tcp())
        .with_wait_for(WaitFor::message_on_stderr("k3s is up and running"))
        .with_cmd([
            "server",
            "--disable-agent",
            "--disable=traefik,metrics-server,servicelb,local-storage,coredns",
        ])
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(6443).await.unwrap();
    let mut cat = container
        .exec(ExecCommand::new(["cat", "/etc/rancher/k3s/k3s.yaml"]))
        .await
        .unwrap();
    let yaml = String::from_utf8(cat.stdout_to_vec().await.unwrap())
        .unwrap()
        .replace("127.0.0.1:6443", &format!("127.0.0.1:{port}"));
    let options = KubeConfigOptions::default();
    let mut config =
        kube::Config::from_custom_kubeconfig(Kubeconfig::from_yaml(&yaml).unwrap(), &options)
            .await
            .unwrap();
    // The API server is on 127.0.0.1, so a proxy from the environment never applies.
    config.proxy_url = None;
    let client = Client::try_from(config).unwrap();

    // The API server takes a moment after k3s says it is up.
    let crds = Api::<CustomResourceDefinition>::all(client.clone());
    let crd = CustomerIntegration::crd();
    let name = crd.name_any();
    eventually("the CRD to be applied", || async {
        crds.patch(&name, &PatchParams::apply("test"), &Patch::Apply(&crd))
            .await
            .ok()
    })
    .await;
    eventually("the CRD to be established", || async {
        let crd = crds.get(&name).await.ok()?;
        let conditions = crd.status?.conditions?;
        conditions
            .iter()
            .any(|c| c.type_ == "Established" && c.status == "True")
            .then_some(())
    })
    .await;
    (container, client)
}

/// Same image as deploy/compose.
async fn nats() -> (ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("nats", "2.15.2-alpine")
        .with_exposed_port(4222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
        .with_cmd(["--jetstream"])
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(4222).await.unwrap();
    (container, format!("nats://127.0.0.1:{port}"))
}

/// The operator, until the guard is dropped.
struct Operator(JoinHandle<()>);

impl Drop for Operator {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn start_operator(client: &Client, nats_url: &str) -> Operator {
    let settings = Settings {
        ingest_image: "pourline/ingest:test".into(),
        env: vec![("NATS_URL".into(), nats_url.into())],
    };
    let nats = StatusReader::connect(nats_url).await.unwrap();
    let run = operator::run(
        client.clone(),
        "default",
        settings,
        nats,
        std::future::pending(),
    );
    Operator(tokio::spawn(run))
}

async fn eventually<T, F, Fut>(what: &str, mut probe: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(value) = probe().await {
            return value;
        }
        assert!(Instant::now() < deadline, "gave up waiting for {what}");
        sleep(Duration::from_millis(200)).await;
    }
}

fn customers() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../customers")
}

fn mapping(customer: &str) -> String {
    std::fs::read_to_string(customers().join(customer).join("mapping.yaml")).unwrap()
}

async fn put_config_map(client: &Client, name: &str, yaml: &str) {
    let config_map = json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": { "name": name },
        "data": { "mapping.yaml": yaml },
    });
    Api::<ConfigMap>::default_namespaced(client.clone())
        .patch(name, &PatchParams::apply("test"), &Patch::Apply(config_map))
        .await
        .unwrap();
}

async fn put_secret(client: &Client, name: &str, keys: &[&str]) {
    let data: serde_json::Map<String, Value> = keys
        .iter()
        .map(|k| ((*k).to_owned(), json!("dev")))
        .collect();
    let secret = json!({
        "apiVersion": "v1", "kind": "Secret",
        "metadata": { "name": name },
        "stringData": data,
    });
    Api::<Secret>::default_namespaced(client.clone())
        .patch(name, &PatchParams::apply("test"), &Patch::Apply(secret))
        .await
        .unwrap();
}

fn integration(name: &str, spec: Value) -> CustomerIntegration {
    serde_json::from_value(json!({
        "apiVersion": "pourline.dev/v1alpha1", "kind": "CustomerIntegration",
        "metadata": { "name": name },
        "spec": spec,
    }))
    .unwrap()
}

async fn set_spec(integrations: &Api<CustomerIntegration>, name: &str, spec: Value) {
    integrations
        .patch(
            name,
            &PatchParams::default(),
            &Patch::Merge(json!({ "spec": spec })),
        )
        .await
        .unwrap();
}

/// Waits until the integration's status satisfies `wanted`, and returns it.
async fn status_where(
    integrations: &Api<CustomerIntegration>,
    name: &str,
    what: &str,
    wanted: impl Fn(&CustomerIntegrationStatus) -> bool,
) -> CustomerIntegrationStatus {
    eventually(what, || async {
        let status = integrations.get(name).await.ok()?.status?;
        wanted(&status).then_some(status)
    })
    .await
}

fn at(minute: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 9, 9, minute, 0).unwrap()
}

/// One ingested order and one dead letter for bauwerk-ag, so the status has something to report.
async fn seed_nats(nats_url: &str) {
    let bus = Bus::connect(nats_url, Duration::from_secs(3600))
        .await
        .unwrap();
    let mapping = Mapping::load(&customers(), "bauwerk-ag").unwrap();
    let seed = Seed::rhine_neckar().unwrap();
    for (file, minute) in [("order.json", 10), ("broken.json", 20)] {
        let body = std::fs::read(customers().join("bauwerk-ag/samples").join(file)).unwrap();
        let delivery = Delivery {
            body: &body,
            channel: mapping.channel,
            reference: file,
            received_at: at(minute),
        };
        bus.deliver(&mapping, &seed, &delivery).await.unwrap();
    }
}

fn template_annotations(deployment: &Deployment) -> Value {
    json!(
        deployment
            .spec
            .template
            .metadata
            .as_ref()
            .unwrap()
            .annotations
    )
}

#[tokio::test]
async fn a_webhook_integration_is_applied_paused_guarded_and_cleaned_up() {
    let ((_k3s, client), (_nats, nats_url)) = tokio::join!(k3s(), nats());
    seed_nats(&nats_url).await;
    let _operator = start_operator(&client, &nats_url).await;
    let integrations = Api::<CustomerIntegration>::default_namespaced(client.clone());
    let deployments = Api::<Deployment>::default_namespaced(client.clone());
    let config_maps = Api::<ConfigMap>::default_namespaced(client.clone());
    let name = "bauwerk-ag";

    put_config_map(&client, "bauwerk-ag-mapping", &mapping(name)).await;
    put_secret(&client, "bauwerk-ag-credentials", &["token"]).await;
    let spec = json!({
        "channel": "webhook", "mappingConfigMap": "bauwerk-ag-mapping",
        "credentialsSecret": "bauwerk-ag-credentials", "replicas": 2,
    });
    integrations
        .create(&PostParams::default(), &integration(name, spec))
        .await
        .unwrap();

    let deployment = eventually("the Deployment", || async {
        deployments.get_opt("ingest-bauwerk-ag").await.unwrap()
    })
    .await;
    assert_eq!(deployment.spec.replicas, Some(2));
    Api::<Service>::default_namespaced(client.clone())
        .get("ingest-bauwerk-ag")
        .await
        .unwrap();
    Api::<Ingress>::default_namespaced(client.clone())
        .get("ingest-bauwerk-ag")
        .await
        .unwrap();
    let copy = config_maps.get("ingest-bauwerk-ag").await.unwrap();
    assert_eq!(copy.data.unwrap()["mapping.yaml"], mapping(name));

    // No pod starts here, so it stays Pending; NATS has one ingest and one dead letter.
    let status = status_where(&integrations, name, "the first status", |s| {
        s.last_successful_ingest.is_some()
    })
    .await;
    assert_eq!(
        status,
        CustomerIntegrationStatus {
            phase: Phase::Pending,
            message: Some("0 of 2 replicas ready".into()),
            last_successful_ingest: Some(at(10)),
            dead_letter_count: 1,
        }
    );
    let finalizers = integrations.get(name).await.unwrap().finalizers().to_vec();
    assert_eq!(finalizers, ["pourline.dev/cleanup"]);

    set_spec(&integrations, name, json!({ "paused": true })).await;
    status_where(&integrations, name, "Paused", |s| s.phase == Phase::Paused).await;
    let paused = deployments.get("ingest-bauwerk-ag").await.unwrap();
    assert_eq!(paused.spec.replicas, Some(0));
    assert_eq!(
        status_where(&integrations, name, "no message", |s| s.message.is_none())
            .await
            .phase,
        Phase::Paused
    );

    set_spec(&integrations, name, json!({ "paused": false })).await;
    status_where(&integrations, name, "Pending again", |s| {
        s.phase == Phase::Pending
    })
    .await;
    let resumed = deployments.get("ingest-bauwerk-ag").await.unwrap();
    assert_eq!(resumed.spec.replicas, Some(2));

    // A broken edit fails the integration and reaches nothing that runs.
    put_config_map(&client, "bauwerk-ag-mapping", "channel: [").await;
    let failed = status_where(&integrations, name, "Failed", |s| s.phase == Phase::Failed).await;
    let message = failed.message.unwrap();
    assert!(
        message.starts_with("mapping.yaml in ConfigMap bauwerk-ag-mapping: "),
        "{message}"
    );
    let copy = config_maps.get("ingest-bauwerk-ag").await.unwrap();
    assert_eq!(copy.data.unwrap()["mapping.yaml"], mapping(name));
    let still = deployments.get("ingest-bauwerk-ag").await.unwrap();
    assert_eq!(template_annotations(&still), template_annotations(&resumed));

    // The fix rolls out, and the pods roll with it.
    let fixed = format!("{}# reviewed\n", mapping(name));
    put_config_map(&client, "bauwerk-ag-mapping", &fixed).await;
    status_where(&integrations, name, "recovery", |s| {
        s.phase == Phase::Pending
    })
    .await;
    let copy = config_maps.get("ingest-bauwerk-ag").await.unwrap();
    assert_eq!(copy.data.unwrap()["mapping.yaml"], fixed);
    let rolled = deployments.get("ingest-bauwerk-ag").await.unwrap();
    assert_ne!(
        template_annotations(&rolled),
        template_annotations(&resumed)
    );

    // Deleting it stops the workload, clears the last ingest, keeps the dead letters.
    integrations
        .delete(name, &DeleteParams::default())
        .await
        .unwrap();
    eventually("the integration to go", || async {
        integrations
            .get_opt(name)
            .await
            .unwrap()
            .is_none()
            .then_some(())
    })
    .await;
    assert!(
        deployments
            .get_opt("ingest-bauwerk-ag")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        config_maps
            .get_opt("ingest-bauwerk-ag")
            .await
            .unwrap()
            .is_none()
    );
    let reader = StatusReader::connect(&nats_url).await.unwrap();
    let left = reader.status(name).await.unwrap();
    assert_eq!((left.last_successful_ingest, left.dead_letters), (None, 1));
}

#[tokio::test]
async fn an_sftp_integration_waits_for_its_mapping_then_polls_on_its_schedule() {
    let ((_k3s, client), (_nats, nats_url)) = tokio::join!(k3s(), nats());
    let _operator = start_operator(&client, &nats_url).await;
    let integrations = Api::<CustomerIntegration>::default_namespaced(client.clone());
    let cron_jobs = Api::<CronJob>::default_namespaced(client.clone());
    let name = "rheinbau-gmbh";

    put_secret(
        &client,
        "rheinbau-gmbh-credentials",
        &["username", "password"],
    )
    .await;
    let spec = json!({
        "channel": "sftp", "mappingConfigMap": "rheinbau-gmbh-mapping",
        "credentialsSecret": "rheinbau-gmbh-credentials", "schedule": "*/5 * * * *",
    });
    integrations
        .create(&PostParams::default(), &integration(name, spec))
        .await
        .unwrap();
    let failed = status_where(&integrations, name, "Failed", |s| s.phase == Phase::Failed).await;
    assert_eq!(
        failed.message.as_deref(),
        Some("ConfigMap rheinbau-gmbh-mapping not found")
    );
    assert!(
        cron_jobs
            .get_opt("ingest-rheinbau-gmbh")
            .await
            .unwrap()
            .is_none()
    );

    // The operator watches the ConfigMap, so creating it is enough.
    put_config_map(&client, "rheinbau-gmbh-mapping", &mapping(name)).await;
    let running = status_where(&integrations, name, "Running", |s| {
        s.phase == Phase::Running
    })
    .await;
    assert_eq!(running.message, None);
    let cron_job = cron_jobs.get("ingest-rheinbau-gmbh").await.unwrap();
    assert_eq!(cron_job.spec.schedule, "*/5 * * * *");
    assert_eq!(cron_job.spec.suspend, Some(false));

    set_spec(
        &integrations,
        name,
        json!({ "paused": true, "schedule": "0 3 * * *" }),
    )
    .await;
    status_where(&integrations, name, "Paused", |s| s.phase == Phase::Paused).await;
    let cron_job = cron_jobs.get("ingest-rheinbau-gmbh").await.unwrap();
    assert_eq!(
        (cron_job.spec.schedule.as_str(), cron_job.spec.suspend),
        ("0 3 * * *", Some(true))
    );
}

#[tokio::test]
async fn the_api_server_refuses_an_inconsistent_integration() {
    let (_k3s, client) = k3s().await;
    let integrations = Api::<CustomerIntegration>::default_namespaced(client.clone());
    let refused = |spec: Value| {
        let integrations = integrations.clone();
        async move {
            let error = integrations
                .create(&PostParams::default(), &integration("x", spec))
                .await
                .unwrap_err();
            error.to_string()
        }
    };
    let base = |extra: Value| {
        let mut spec = json!({ "mappingConfigMap": "m", "credentialsSecret": "s" });
        spec.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        spec
    };

    let error = refused(base(json!({ "channel": "sftp" }))).await;
    assert!(
        error.contains("an sftp integration needs a schedule"),
        "{error}"
    );
    let error = refused(base(
        json!({ "channel": "webhook", "schedule": "* * * * *" }),
    ))
    .await;
    assert!(
        error.contains("a webhook integration takes none"),
        "{error}"
    );
    let error = refused(base(
        json!({ "channel": "sftp", "schedule": "* * * * *", "replicas": 2 }),
    ))
    .await;
    assert!(error.contains("polls with one replica"), "{error}");

    integrations
        .create(
            &PostParams::default(),
            &integration("y", base(json!({ "channel": "webhook" }))),
        )
        .await
        .unwrap();
    let error = integrations
        .patch(
            "y",
            &PatchParams::default(),
            &Patch::Merge(json!({ "spec": { "channel": "sftp", "schedule": "* * * * *" } })),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("the channel can't change"), "{error}");
}
