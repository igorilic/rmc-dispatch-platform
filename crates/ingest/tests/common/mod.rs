//! Shared by the integration tests: a throwaway NATS JetStream container.

use std::path::{Path, PathBuf};

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// Same image as deploy/compose. Keep the container alive for as long as the test uses it.
pub async fn nats() -> (ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("nats", "2.15.3-alpine")
        .with_exposed_port(4222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
        .with_cmd(["--jetstream"])
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(4222).await.unwrap();
    (container, format!("nats://127.0.0.1:{port}"))
}

pub fn customers() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../customers")
}

pub fn sample(integration: &str, file: &str) -> Vec<u8> {
    std::fs::read(customers().join(integration).join("samples").join(file)).unwrap()
}
