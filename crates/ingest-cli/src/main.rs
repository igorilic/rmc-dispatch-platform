//! Tools for operating ingest. `validate` checks a customer's sample file against its mapping
//! before go-live (SPEC §5).
//!
//! Exit codes: 0 when everything is valid, 1 when a record is invalid, 2 when the tool itself
//! cannot run (missing mapping, unreadable file).

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Context;
use chrono::Utc;
use clap::{Parser, Subcommand};
use domain::Seed;
use ingest::mapping::{Delivery, Mapping};

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
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(code) => code,
        Err(error) => {
            eprintln!("ingest-cli: {error:#}");
            ExitCode::from(2)
        }
    }
}

fn run(cli: Cli) -> anyhow::Result<ExitCode> {
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
            print!("{}", ingest::report(&name, &outcomes));
            let valid = outcomes.iter().all(|o| o.result.is_ok());
            Ok(if valid {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            })
        }
    }
}
