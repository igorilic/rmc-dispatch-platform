//! `operator`: runs the controller. `operator crd` prints the CRD instead.

fn main() -> anyhow::Result<()> {
    match std::env::args().nth(1).as_deref() {
        Some("crd") => print!("{}", operator::crd::crd_yaml()?),
        _ => anyhow::bail!("usage: operator crd"),
    }
    Ok(())
}
