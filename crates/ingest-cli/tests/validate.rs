//! Runs the real binary: the report is the contract with whoever onboards a customer.

// Integration tests are test code; SPEC §15 allows unwrap there.
#![allow(clippy::unwrap_used)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn customers() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../customers")
}

fn validate(customer: &str, sample: &str) -> Output {
    let file = customers().join(customer).join("samples").join(sample);
    Command::new(env!("CARGO_BIN_EXE_ingest-cli"))
        .args(["validate", "--customer", customer, "--file"])
        .arg(file)
        .arg("--mappings-dir")
        .arg(customers())
        .output()
        .unwrap()
}

#[test]
fn a_broken_file_reports_every_problem_and_fails() {
    let out = validate("rheinbau-gmbh", "broken.csv");
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(
        String::from_utf8(out.stdout).unwrap(),
        "broken.csv: 3 records, 2 valid, 1 invalid\n\
         \x20 line 4 (RB-7790)\n\
         \x20   Warenempfänger: ship-to 50001007 belongs to customer 10001001, not 10001002\n\
         \x20   Menge: \"12.5\" is not a decimal-comma number (e.g. 12,5)\n\
         \x20   Liefertermin: \"29.03.2026 02:30\" does not exist in Europe/Berlin (clocks change)\n"
    );
}

#[test]
fn good_samples_pass() {
    for (customer, sample) in [
        ("bauwerk-ag", "order.json"),
        ("rheinbau-gmbh", "orders.csv"),
        ("neckar-infra", "order-v1.xml"),
        ("neckar-infra", "order-v2.xml"),
    ] {
        let out = validate(customer, sample);
        let stdout = String::from_utf8(out.stdout).unwrap();
        assert_eq!(out.status.code(), Some(0), "{customer}/{sample}: {stdout}");
        assert!(stdout.ends_with("all valid\n"), "{stdout}");
    }
}

#[test]
fn an_unknown_customer_is_a_clear_error() {
    let out = validate("nobody-gmbh", "order.json");
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains("nobody-gmbh/mapping.yaml"), "{stderr}");
}
