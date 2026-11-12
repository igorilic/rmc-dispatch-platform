//! What a `CustomerIntegration` becomes, as pure functions, so the tests can check every object
//! without a cluster.
//!
//! Every object is named `ingest-<name>`, lives in the integration's namespace and is owned by
//! it, so Kubernetes deletes it along with the integration, even while the operator is down:
//! - a ConfigMap with the last valid mapping, mounted at `/customers/<name>/`, where ingest's
//!   `MAPPINGS_DIR` expects it. A broken edit of your ConfigMap never reaches a pod;
//! - for a webhook, a Deployment running `INGEST_MODE=webhooks`, a Service, and an Ingress for
//!   `/webhooks/<name>`;
//! - for sftp, a CronJob running `INGEST_MODE=sftp-once`.
//!
//! The pod template carries the mapping's hash and the credentials Secret's `resourceVersion`, so
//! a change to either rolls the pods. It is the version rather than a hash of the values, so no
//! hash of a password ends up in an object that more people can read than the Secret.

use std::collections::BTreeMap;

use ingest::mapping::Mapping;
use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec};
use k8s_openapi::api::batch::v1::{CronJob, CronJobSpec, JobSpec, JobTemplateSpec};
use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, ContainerPort, EnvVar, EnvVarSource,
    HTTPGetAction, PodSecurityContext, PodSpec, PodTemplateSpec, Probe, Secret, SecretKeySelector,
    Service, ServicePort, ServiceSpec, Volume, VolumeMount,
};
use k8s_openapi::api::networking::v1::{
    HTTPIngressPath, HTTPIngressRuleValue, Ingress, IngressBackend, IngressRule,
    IngressServiceBackend, IngressSpec, ServiceBackendPort,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::{Resource, ResourceExt};
use sha2::{Digest, Sha256};

use crate::crd::{Channel, CustomerIntegration, Phase};

const HTTP_PORT: i32 = 8080;
const MAPPING_KEY: &str = "mapping.yaml";
const MAPPINGS_DIR: &str = "/customers";
/// ponytail: a poll that hangs (a stuck SFTP server) would block every later run, since runs
/// never overlap; 5 minutes is plenty for the CSV drops we have. Make it a field if a customer's
/// files ever take longer.
const POLL_DEADLINE_SECS: i64 = 300;

/// What the operator passes on to every workload.
#[derive(Debug, Clone, Default)]
pub struct Settings {
    pub ingest_image: String,
    /// `NATS_URL`, `SFTP_HOST` and the like, from the operator's own environment.
    pub env: Vec<(String, String)>,
}

/// The inputs that passed the checks: the mapping to roll out, and the credentials' version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checked {
    pub mapping: String,
    pub credentials_version: String,
}

/// The checks before a rollout. The error is the status message.
pub fn check(
    ci: &CustomerIntegration,
    config_map: Option<&ConfigMap>,
    secret: Option<&Secret>,
) -> Result<Checked, String> {
    let spec = &ci.spec;
    let cm = &spec.mapping_config_map;
    let mapping = config_map
        .ok_or_else(|| format!("ConfigMap {cm} not found"))?
        .data
        .as_ref()
        .and_then(|data| data.get(MAPPING_KEY))
        .ok_or_else(|| format!("ConfigMap {cm} has no {MAPPING_KEY}"))?;
    let parsed = Mapping::parse(&ci.name_any(), mapping)
        .map_err(|e| format!("{MAPPING_KEY} in ConfigMap {cm}: {e}"))?;
    let mapping_channel = match parsed.channel {
        domain::Channel::Webhook => "webhook",
        domain::Channel::Sftp => "sftp",
        domain::Channel::Mqtt => "mqtt",
    };
    if mapping_channel != spec.channel.as_str() {
        return Err(format!(
            "the mapping in ConfigMap {cm} is for {mapping_channel}, but spec.channel is {}",
            spec.channel.as_str()
        ));
    }
    let name = &spec.credentials_secret;
    let secret = secret.ok_or_else(|| format!("Secret {name} not found"))?;
    for (key, _) in credentials(ci) {
        if !secret.data.as_ref().is_some_and(|d| d.contains_key(key)) {
            return Err(format!("Secret {name} has no key {key}"));
        }
    }
    Ok(Checked {
        mapping: mapping.clone(),
        credentials_version: secret.resource_version().unwrap_or_default(),
    })
}

/// The Secret's keys, and the variables ingest reads them from.
fn credentials(ci: &CustomerIntegration) -> Vec<(&'static str, String)> {
    match ci.spec.channel {
        // The name ingest's `webhook_token` looks up: bauwerk-ag → WEBHOOK_TOKEN_BAUWERK_AG.
        Channel::Webhook => vec![(
            "token",
            format!(
                "WEBHOOK_TOKEN_{}",
                ci.name_any().to_uppercase().replace('-', "_")
            ),
        )],
        Channel::Sftp => vec![
            ("username", "SFTP_USER".into()),
            ("password", "SFTP_PASSWORD".into()),
        ],
    }
}

/// The name of every object the integration owns.
pub fn name(ci: &CustomerIntegration) -> String {
    format!("ingest-{}", ci.name_any())
}

fn selector(ci: &CustomerIntegration) -> BTreeMap<String, String> {
    BTreeMap::from([("app".to_owned(), name(ci))])
}

fn meta(ci: &CustomerIntegration) -> ObjectMeta {
    let mut labels = selector(ci);
    labels.insert("pourline.dev/integration".into(), ci.name_any());
    ObjectMeta {
        name: Some(name(ci)),
        namespace: ci.namespace(),
        labels: Some(labels),
        owner_references: ci.controller_owner_ref(&()).map(|owner| vec![owner]),
        ..Default::default()
    }
}

/// The operator's copy of the last valid mapping, which the workload mounts.
pub fn mapping_config_map(ci: &CustomerIntegration, checked: &Checked) -> ConfigMap {
    ConfigMap {
        metadata: meta(ci),
        data: Some(BTreeMap::from([(
            MAPPING_KEY.to_owned(),
            checked.mapping.clone(),
        )])),
        ..Default::default()
    }
}

fn env(name: &str, value: &str) -> EnvVar {
    EnvVar {
        name: name.into(),
        value: Some(value.into()),
        ..Default::default()
    }
}

fn pod_template(
    ci: &CustomerIntegration,
    settings: &Settings,
    checked: &Checked,
) -> PodTemplateSpec {
    let webhook = ci.spec.channel == Channel::Webhook;
    let mode = if webhook { "webhooks" } else { "sftp-once" };
    let mut vars = vec![env("INGEST_MODE", mode), env("MAPPINGS_DIR", MAPPINGS_DIR)];
    vars.extend(settings.env.iter().map(|(k, v)| env(k, v)));
    vars.extend(credentials(ci).into_iter().map(|(key, var)| EnvVar {
        name: var,
        value_from: Some(EnvVarSource {
            secret_key_ref: Some(SecretKeySelector {
                name: ci.spec.credentials_secret.clone(),
                key: key.into(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }));
    let mapping_hash: String = Sha256::digest(checked.mapping.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let annotations = BTreeMap::from([
        ("pourline.dev/mapping-sha256".to_owned(), mapping_hash),
        (
            "pourline.dev/credentials-version".to_owned(),
            checked.credentials_version.clone(),
        ),
    ]);
    PodTemplateSpec {
        metadata: Some(ObjectMeta {
            labels: Some(selector(ci)),
            annotations: Some(annotations),
            ..Default::default()
        }),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "ingest".into(),
                image: Some(settings.ingest_image.clone()),
                env: Some(vars),
                ports: webhook.then(|| {
                    vec![ContainerPort {
                        name: Some("http".into()),
                        container_port: HTTP_PORT,
                        ..Default::default()
                    }]
                }),
                readiness_probe: webhook.then(|| Probe {
                    http_get: Some(HTTPGetAction {
                        path: Some("/healthz".into()),
                        port: IntOrString::String("http".into()),
                        ..Default::default()
                    }),
                    period_seconds: Some(2),
                    ..Default::default()
                }),
                volume_mounts: Some(vec![VolumeMount {
                    name: "mapping".into(),
                    mount_path: format!("{MAPPINGS_DIR}/{}", ci.name_any()),
                    read_only: Some(true),
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            volumes: Some(vec![Volume {
                name: "mapping".into(),
                config_map: Some(ConfigMapVolumeSource {
                    name: name(ci),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            restart_policy: (!webhook).then(|| "Never".into()),
            security_context: Some(PodSecurityContext {
                run_as_non_root: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        }),
    }
}

/// A webhook integration's pods: `spec.replicas`, or none while paused.
pub fn deployment(ci: &CustomerIntegration, settings: &Settings, checked: &Checked) -> Deployment {
    Deployment {
        metadata: meta(ci),
        spec: DeploymentSpec {
            replicas: Some(if ci.spec.paused { 0 } else { ci.spec.replicas }),
            selector: LabelSelector {
                match_labels: Some(selector(ci)),
                ..Default::default()
            },
            template: pod_template(ci, settings, checked),
            ..Default::default()
        },
        ..Default::default()
    }
}

pub fn service(ci: &CustomerIntegration) -> Service {
    Service {
        metadata: meta(ci),
        spec: Some(ServiceSpec {
            selector: Some(selector(ci)),
            ports: Some(vec![ServicePort {
                name: Some("http".into()),
                port: HTTP_PORT,
                target_port: Some(IntOrString::String("http".into())),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// `/webhooks/<name>` on the cluster's ingress. The ingress controller merges it with the
/// chart's Ingress, which sends everything else to dispatch.
pub fn ingress(ci: &CustomerIntegration) -> Ingress {
    Ingress {
        metadata: meta(ci),
        spec: Some(IngressSpec {
            rules: Some(vec![IngressRule {
                http: Some(HTTPIngressRuleValue {
                    paths: vec![HTTPIngressPath {
                        path: Some(format!("/webhooks/{}", ci.name_any())),
                        path_type: "Exact".into(),
                        backend: IngressBackend {
                            service: Some(IngressServiceBackend {
                                name: name(ci),
                                port: Some(ServiceBackendPort {
                                    number: Some(HTTP_PORT),
                                    ..Default::default()
                                }),
                            }),
                            ..Default::default()
                        },
                    }],
                }),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// An sftp integration's polls: one Job per scheduled run, never two at once, none while paused.
pub fn cron_job(ci: &CustomerIntegration, settings: &Settings, checked: &Checked) -> CronJob {
    CronJob {
        metadata: meta(ci),
        spec: CronJobSpec {
            schedule: ci.spec.schedule.clone().unwrap_or_default(),
            suspend: Some(ci.spec.paused),
            // Two polls at once would race to move the same files.
            concurrency_policy: Some("Forbid".into()),
            successful_jobs_history_limit: Some(1),
            failed_jobs_history_limit: Some(1),
            job_template: JobTemplateSpec {
                spec: Some(JobSpec {
                    // A failed poll isn't retried: the next scheduled run polls again.
                    backoff_limit: Some(0),
                    active_deadline_seconds: Some(POLL_DEADLINE_SECS),
                    template: pod_template(ci, settings, checked),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    }
}

/// What the operator found of the integration's workload.
#[derive(Debug, Clone, Copy)]
pub enum Observed<'a> {
    Deployment(Option<&'a Deployment>),
    CronJob(Option<&'a CronJob>),
}

/// The phase and its message. `problem` is a failed check, or the API server refusing an object.
pub fn phase(
    ci: &CustomerIntegration,
    problem: Option<&str>,
    observed: Observed<'_>,
) -> (Phase, Option<String>) {
    if let Some(problem) = problem {
        return (Phase::Failed, Some(problem.to_owned()));
    }
    if ci.spec.paused {
        return (Phase::Paused, None);
    }
    match observed {
        Observed::Deployment(None) | Observed::CronJob(None) => {
            (Phase::Pending, Some("waiting for the workload".into()))
        }
        Observed::Deployment(Some(deployment)) => {
            let wanted = ci.spec.replicas;
            let status = deployment.status.clone().unwrap_or_default();
            let current = status.observed_generation >= deployment.metadata.generation;
            let ready = status.ready_replicas.unwrap_or(0);
            if current && ready >= wanted && status.updated_replicas.unwrap_or(0) >= wanted {
                (Phase::Running, None)
            } else {
                (
                    Phase::Pending,
                    Some(format!("{ready} of {wanted} replicas ready")),
                )
            }
        }
        Observed::CronJob(Some(cron_job)) => {
            let status = cron_job.status.clone().unwrap_or_default();
            let running = status.active.is_some_and(|active| !active.is_empty());
            match status.last_schedule_time {
                // lastSuccessfulTime is when a run finished, so a run that succeeded is later
                // than its own schedule time.
                Some(scheduled)
                    if !running
                        && status
                            .last_successful_time
                            .is_none_or(|done| done < scheduled) =>
                {
                    (
                        Phase::Failed,
                        Some(format!(
                            "the last poll, at {}, failed; its Job's log says why",
                            scheduled.0
                        )),
                    )
                }
                _ => (Phase::Running, None),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::path::Path;

    use k8s_openapi::ByteString;
    use k8s_openapi::api::apps::v1::DeploymentStatus;
    use k8s_openapi::api::batch::v1::CronJobStatus;
    use k8s_openapi::api::core::v1::ObjectReference;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;

    use super::*;
    use crate::crd::CustomerIntegrationSpec;

    fn integration(name: &str, channel: Channel) -> CustomerIntegration {
        let mut ci = CustomerIntegration::new(
            name,
            CustomerIntegrationSpec {
                channel,
                mapping_config_map: format!("{name}-mapping"),
                credentials_secret: format!("{name}-credentials"),
                schedule: (channel == Channel::Sftp).then(|| "*/5 * * * *".into()),
                replicas: 1,
                paused: false,
            },
        );
        ci.metadata.namespace = Some("pourline".into());
        ci.metadata.uid = Some("0b5c6f0e-1d2a-4c8e-9f3b-2a7d1e4c5b60".into());
        ci
    }

    fn mapping(customer: &str) -> String {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../customers");
        std::fs::read_to_string(dir.join(customer).join("mapping.yaml")).unwrap()
    }

    fn config_map(yaml: &str) -> ConfigMap {
        ConfigMap {
            data: Some(BTreeMap::from([(MAPPING_KEY.into(), yaml.into())])),
            ..Default::default()
        }
    }

    fn secret(keys: &[&str], version: &str) -> Secret {
        Secret {
            metadata: ObjectMeta {
                resource_version: Some(version.into()),
                ..Default::default()
            },
            data: Some(
                keys.iter()
                    .map(|k| ((*k).to_owned(), ByteString(b"dev".to_vec())))
                    .collect(),
            ),
            ..Default::default()
        }
    }

    fn checked(ci: &CustomerIntegration) -> Checked {
        let (cm, keys) = match ci.spec.channel {
            Channel::Webhook => (config_map(&mapping("bauwerk-ag")), &["token"][..]),
            Channel::Sftp => (
                config_map(&mapping("rheinbau-gmbh")),
                &["username", "password"][..],
            ),
        };
        check(ci, Some(&cm), Some(&secret(keys, "41"))).unwrap()
    }

    fn settings() -> Settings {
        Settings {
            ingest_image: "registry/ingest@sha256:abc".into(),
            env: vec![("NATS_URL".into(), "nats://nats:4222".into())],
        }
    }

    fn container(template: &PodTemplateSpec) -> &Container {
        &template.spec.as_ref().unwrap().containers[0]
    }

    fn var<'a>(container: &'a Container, name: &str) -> &'a EnvVar {
        let vars = container.env.as_ref().unwrap();
        vars.iter().find(|v| v.name == name).unwrap()
    }

    fn secret_ref(var: &EnvVar) -> (&str, &str) {
        let selector = var
            .value_from
            .as_ref()
            .unwrap()
            .secret_key_ref
            .as_ref()
            .unwrap();
        (selector.name.as_str(), selector.key.as_str())
    }

    #[test]
    fn a_valid_mapping_and_credentials_pass_the_checks() {
        let ci = integration("bauwerk-ag", Channel::Webhook);
        assert_eq!(
            checked(&ci),
            Checked {
                mapping: mapping("bauwerk-ag"),
                credentials_version: "41".into()
            }
        );
    }

    #[test]
    fn each_failed_check_says_what_is_wrong() {
        let ci = integration("bauwerk-ag", Channel::Webhook);
        let cm = config_map(&mapping("bauwerk-ag"));
        let token = secret(&["token"], "1");
        let fails =
            |cm: Option<&ConfigMap>, secret: Option<&Secret>| check(&ci, cm, secret).unwrap_err();

        assert_eq!(
            fails(None, Some(&token)),
            "ConfigMap bauwerk-ag-mapping not found"
        );
        assert_eq!(
            fails(Some(&ConfigMap::default()), Some(&token)),
            "ConfigMap bauwerk-ag-mapping has no mapping.yaml"
        );
        let broken = fails(Some(&config_map("channel: [")), Some(&token));
        assert!(
            broken.starts_with("mapping.yaml in ConfigMap bauwerk-ag-mapping: "),
            "{broken}"
        );
        assert_eq!(
            fails(Some(&config_map(&mapping("rheinbau-gmbh"))), Some(&token)),
            "the mapping in ConfigMap bauwerk-ag-mapping is for sftp, but spec.channel is webhook"
        );
        assert_eq!(
            fails(Some(&cm), None),
            "Secret bauwerk-ag-credentials not found"
        );
        assert_eq!(
            fails(Some(&cm), Some(&secret(&["password"], "1"))),
            "Secret bauwerk-ag-credentials has no key token"
        );

        let sftp = integration("rheinbau-gmbh", Channel::Sftp);
        let cm = config_map(&mapping("rheinbau-gmbh"));
        assert_eq!(
            check(&sftp, Some(&cm), Some(&secret(&["username"], "1"))).unwrap_err(),
            "Secret rheinbau-gmbh-credentials has no key password"
        );
    }

    #[test]
    fn every_object_is_named_after_and_owned_by_its_integration() {
        let ci = integration("bauwerk-ag", Channel::Webhook);
        let checked = checked(&ci);
        let metas = [
            mapping_config_map(&ci, &checked).metadata,
            deployment(&ci, &settings(), &checked).metadata,
            service(&ci).metadata,
            ingress(&ci).metadata,
            cron_job(
                &integration("bauwerk-ag", Channel::Sftp),
                &settings(),
                &checked,
            )
            .metadata,
        ];
        for meta in metas {
            assert_eq!(meta.name.as_deref(), Some("ingest-bauwerk-ag"));
            assert_eq!(meta.namespace.as_deref(), Some("pourline"));
            let owner = &meta.owner_references.unwrap()[0];
            assert_eq!(
                (owner.kind.as_str(), owner.name.as_str()),
                ("CustomerIntegration", "bauwerk-ag")
            );
            assert_eq!(owner.controller, Some(true));
        }
        let cm = mapping_config_map(&ci, &checked);
        assert_eq!(cm.data.unwrap()["mapping.yaml"], mapping("bauwerk-ag"));
    }

    #[test]
    fn a_webhook_integration_runs_its_webhooks_behind_its_own_path() {
        let mut ci = integration("bauwerk-ag", Channel::Webhook);
        ci.spec.replicas = 3;
        let deployment = deployment(&ci, &settings(), &checked(&ci));
        let spec = deployment.spec;
        assert_eq!(spec.replicas, Some(3));
        assert_eq!(spec.selector.match_labels, Some(selector(&ci)));
        assert_eq!(
            spec.template.metadata.as_ref().unwrap().labels,
            Some(selector(&ci))
        );

        let ingest = container(&spec.template);
        assert_eq!(ingest.image.as_deref(), Some("registry/ingest@sha256:abc"));
        assert_eq!(
            var(ingest, "INGEST_MODE").value.as_deref(),
            Some("webhooks")
        );
        assert_eq!(
            var(ingest, "MAPPINGS_DIR").value.as_deref(),
            Some("/customers")
        );
        assert_eq!(
            var(ingest, "NATS_URL").value.as_deref(),
            Some("nats://nats:4222")
        );
        assert_eq!(
            secret_ref(var(ingest, "WEBHOOK_TOKEN_BAUWERK_AG")),
            ("bauwerk-ag-credentials", "token")
        );
        let mount = &ingest.volume_mounts.as_ref().unwrap()[0];
        assert_eq!(mount.mount_path, "/customers/bauwerk-ag");
        let volume = &spec
            .template
            .spec
            .as_ref()
            .unwrap()
            .volumes
            .as_ref()
            .unwrap()[0];
        assert_eq!(volume.name, mount.name);
        assert_eq!(
            volume.config_map.as_ref().unwrap().name,
            "ingest-bauwerk-ag"
        );
        assert!(ingest.readiness_probe.is_some());

        let service = service(&ci).spec.unwrap();
        assert_eq!(service.selector, Some(selector(&ci)));
        assert_eq!(service.ports.unwrap()[0].port, 8080);

        let rule = &ingress(&ci).spec.unwrap().rules.unwrap()[0];
        let path = &rule.http.as_ref().unwrap().paths[0];
        assert_eq!(path.path.as_deref(), Some("/webhooks/bauwerk-ag"));
        let backend = path.backend.service.as_ref().unwrap();
        assert_eq!(backend.name, "ingest-bauwerk-ag");
        assert_eq!(backend.port.as_ref().unwrap().number, Some(8080));

        ci.spec.paused = true;
        let paused = super::deployment(&ci, &settings(), &checked(&ci));
        assert_eq!(paused.spec.replicas, Some(0));
    }

    #[test]
    fn an_sftp_integration_polls_on_its_schedule_one_run_at_a_time() {
        let mut ci = integration("rheinbau-gmbh", Channel::Sftp);
        let cron_job = cron_job(&ci, &settings(), &checked(&ci));
        let spec = cron_job.spec;
        assert_eq!(spec.schedule, "*/5 * * * *");
        assert_eq!(spec.suspend, Some(false));
        assert_eq!(spec.concurrency_policy.as_deref(), Some("Forbid"));
        let job = spec.job_template.spec.unwrap();
        assert_eq!(
            job.template
                .spec
                .as_ref()
                .unwrap()
                .restart_policy
                .as_deref(),
            Some("Never")
        );

        let ingest = container(&job.template);
        assert_eq!(
            var(ingest, "INGEST_MODE").value.as_deref(),
            Some("sftp-once")
        );
        assert_eq!(
            secret_ref(var(ingest, "SFTP_USER")),
            ("rheinbau-gmbh-credentials", "username")
        );
        assert_eq!(
            secret_ref(var(ingest, "SFTP_PASSWORD")),
            ("rheinbau-gmbh-credentials", "password")
        );
        assert!(ingest.ports.is_none() && ingest.readiness_probe.is_none());

        ci.spec.paused = true;
        let paused = super::cron_job(&ci, &settings(), &checked(&ci));
        assert_eq!(paused.spec.suspend, Some(true));
    }

    #[test]
    fn the_pods_roll_when_the_mapping_or_the_credentials_change_and_only_then() {
        let ci = integration("bauwerk-ag", Channel::Webhook);
        let template = |checked: &Checked| deployment(&ci, &settings(), checked).spec.template;
        let base = checked(&ci);
        assert_eq!(template(&base), template(&base.clone()));

        let edited = Checked {
            mapping: format!("{}\n# a comment\n", base.mapping),
            ..base.clone()
        };
        assert_ne!(template(&edited).metadata, template(&base).metadata);
        let rotated = Checked {
            credentials_version: "42".into(),
            ..base.clone()
        };
        assert_ne!(template(&rotated).metadata, template(&base).metadata);
    }

    fn time(rfc3339: &str) -> Time {
        Time(rfc3339.parse().unwrap())
    }

    #[test]
    fn the_phase_follows_the_checks_the_pause_switch_and_the_workload() {
        let ci = integration("bauwerk-ag", Channel::Webhook);
        let deployment = |ready, generation, observed| Deployment {
            metadata: ObjectMeta {
                generation: Some(generation),
                ..Default::default()
            },
            status: Some(DeploymentStatus {
                ready_replicas: Some(ready),
                updated_replicas: Some(ready),
                observed_generation: Some(observed),
                ..Default::default()
            }),
            ..Default::default()
        };
        fn phase_of(
            ci: &CustomerIntegration,
            problem: Option<&str>,
            observed: Observed<'_>,
        ) -> Phase {
            phase(ci, problem, observed).0
        }

        assert_eq!(
            phase(&ci, Some("Secret x not found"), Observed::Deployment(None)),
            (Phase::Failed, Some("Secret x not found".into()))
        );
        let mut paused = ci.clone();
        paused.spec.paused = true;
        assert_eq!(
            phase_of(&paused, None, Observed::Deployment(None)),
            Phase::Paused
        );
        assert_eq!(
            phase_of(&ci, None, Observed::Deployment(None)),
            Phase::Pending
        );
        assert_eq!(
            phase(&ci, None, Observed::Deployment(Some(&deployment(0, 1, 1)))),
            (Phase::Pending, Some("0 of 1 replicas ready".into()))
        );
        assert_eq!(
            phase_of(&ci, None, Observed::Deployment(Some(&deployment(1, 1, 1)))),
            Phase::Running
        );
        // Ready, but the Deployment hasn't seen the latest spec yet.
        assert_eq!(
            phase_of(&ci, None, Observed::Deployment(Some(&deployment(1, 2, 1)))),
            Phase::Pending
        );

        let sftp = integration("rheinbau-gmbh", Channel::Sftp);
        let cron_job = |scheduled: Option<&str>, succeeded: Option<&str>, active: bool| CronJob {
            status: Some(CronJobStatus {
                last_schedule_time: scheduled.map(time),
                last_successful_time: succeeded.map(time),
                active: active.then(|| vec![ObjectReference::default()]),
            }),
            ..Default::default()
        };
        let of = |c: &CronJob| phase(&sftp, None, Observed::CronJob(Some(c)));
        assert_eq!(
            phase_of(&sftp, None, Observed::CronJob(None)),
            Phase::Pending
        );
        assert_eq!(
            of(&cron_job(None, None, false)).0,
            Phase::Running,
            "before the first run"
        );
        assert_eq!(
            of(&cron_job(
                Some("2026-11-11T10:05:00Z"),
                Some("2026-11-11T10:05:04Z"),
                false
            ))
            .0,
            Phase::Running
        );
        assert_eq!(
            of(&cron_job(
                Some("2026-11-11T10:10:00Z"),
                Some("2026-11-11T10:05:04Z"),
                true
            ))
            .0,
            Phase::Running,
            "a run in progress"
        );
        assert_eq!(
            of(&cron_job(
                Some("2026-11-11T10:10:00Z"),
                Some("2026-11-11T10:05:04Z"),
                false
            )),
            (
                Phase::Failed,
                Some(
                    "the last poll, at 2026-11-11T10:10:00Z, failed; its Job's log says why".into()
                )
            )
        );
        assert_eq!(
            of(&cron_job(Some("2026-11-11T10:10:00Z"), None, false)).0,
            Phase::Failed
        );
    }
}
