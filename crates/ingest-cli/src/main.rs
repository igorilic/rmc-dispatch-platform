//! Tools for operating ingest:
//! - `validate` checks a customer's sample file against its mapping before go-live (SPEC §5).
//! - `dlq list` and `dlq replay` show and re-process dead letters after a mapping fix.
//!
//! Exit codes: 0 when everything is fine, 1 when a record is invalid or still fails, and 2 when
//! the tool itself cannot run (missing mapping, unreadable file, NATS unreachable).

use std::fmt::Write as _;
use std::io::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::Context;
use chrono::{SecondsFormat, Utc};
use clap::{Parser, Subcommand};
use domain::Seed;
use ingest::mapping::{Delivery, Mapping};
use ingest::nats::Bus;

#[derive(Parser)]
#[command(name = "ingest-cli", about = "Tools for operating Pourline's ingest")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Map a file with a customer's mapping and report every problem, row by row.
    Validate {
        /// The customer integration, e.g. rheinbau-gmbh.
        #[arg(long)]
        customer: String,
        #[arg(long)]
        file: PathBuf,
        #[arg(long, env = "MAPPINGS_DIR", default_value = "customers")]
        mappings_dir: PathBuf,
    },
    /// Inspect or replay the dead-letter queue.
    Dlq {
        #[command(subcommand)]
        action: DlqAction,
        #[arg(
            long,
            global = true,
            env = "NATS_URL",
            default_value = "nats://localhost:4222"
        )]
        nats_url: String,
        /// Must match the ingest service's setting, since connecting applies it to the dedup bucket.
        #[arg(long, global = true, env = "DEDUP_TTL_DAYS", default_value_t = 7)]
        dedup_ttl_days: u64,
    },
}

#[derive(Subcommand)]
enum DlqAction {
    /// Show every dead letter with all its errors.
    List {
        /// Only this integration's letters.
        #[arg(long)]
        customer: Option<String>,
    },
    /// Re-map dead letters with the current mappings; publish what works and keep the rest.
    Replay {
        /// Only this integration's letters.
        #[arg(long)]
        customer: Option<String>,
        #[arg(long, env = "MAPPINGS_DIR", default_value = "customers")]
        mappings_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(code) => code,
        Err(error) => {
            eprintln!("ingest-cli: {error:#}");
            ExitCode::from(2)
        }
    }
}

/// Writes everything at once; a reader that went away (e.g. `| head`) is not an error.
fn emit(text: &str) {
    let _ = std::io::stdout().write_all(text.as_bytes());
}

fn failed_if(failed: bool) -> ExitCode {
    if failed {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

async fn run(cli: Cli) -> anyhow::Result<ExitCode> {
    match cli.command {
        Command::Validate {
            customer,
            file,
            mappings_dir,
        } => {
            let mapping = Mapping::load(&mappings_dir, &customer)?;
            let seed = Seed::rhine_neckar()?;
            let body =
                std::fs::read(&file).with_context(|| format!("reading {}", file.display()))?;
            let name = file.file_name().map_or_else(
                || file.display().to_string(),
                |n| n.to_string_lossy().into_owned(),
            );
            let delivery = Delivery {
                body: &body,
                channel: mapping.channel,
                reference: &name,
                received_at: Utc::now(),
            };
            let outcomes = mapping.map(&delivery, &seed);
            emit(&ingest::report(&name, &outcomes));
            Ok(failed_if(outcomes.iter().any(|o| o.result.is_err())))
        }
        Command::Dlq {
            action,
            nats_url,
            dedup_ttl_days,
        } => {
            let bus =
                Bus::connect(&nats_url, Duration::from_secs(dedup_ttl_days * 24 * 3600)).await?;
            match action {
                DlqAction::List { customer } => {
                    let letters: Vec<_> = bus
                        .dead_letters()
                        .await?
                        .into_iter()
                        .filter(|s| customer.as_ref().is_none_or(|c| *c == s.letter.integration))
                        .collect();
                    let mut out = String::new();
                    let plural = if letters.len() == 1 { "" } else { "s" };
                    writeln!(out, "{} dead letter{plural}", letters.len())?;
                    for stored in letters {
                        let letter = stored.letter;
                        let at = letter
                            .received_at
                            .to_rfc3339_opts(SecondsFormat::Secs, true);
                        let (sequence, integration) = (stored.sequence, &letter.integration);
                        writeln!(
                            out,
                            "  #{sequence} {integration} {} {} ({at})",
                            letter.reference, letter.record
                        )?;
                        for error in &letter.errors {
                            writeln!(out, "    {error}")?;
                        }
                    }
                    emit(&out);
                    Ok(ExitCode::SUCCESS)
                }
                DlqAction::Replay {
                    customer,
                    mappings_dir,
                } => {
                    let mappings = Mapping::load_all(&mappings_dir)?;
                    let seed = Seed::rhine_neckar()?;
                    let report =
                        ingest::replay(&bus, &mappings, &seed, customer.as_deref()).await?;
                    let mut out = String::new();
                    let failing = &report.still_failing;
                    writeln!(
                        out,
                        "replayed {}, still failing {}",
                        report.replayed,
                        failing.len()
                    )?;
                    for letter in failing {
                        writeln!(
                            out,
                            "  #{} {} {}",
                            letter.sequence, letter.integration, letter.record
                        )?;
                        for error in &letter.errors {
                            writeln!(out, "    {error}")?;
                        }
                    }
                    emit(&out);
                    Ok(failed_if(!failing.is_empty()))
                }
            }
        }
    }
}
