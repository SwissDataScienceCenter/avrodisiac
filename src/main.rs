use std::{
    ffi::OsStr,
    fs::{self},
    path::{Path, PathBuf},
};

use anyhow::Result;
use apache_avro::{schema_compatibility::SchemaCompatibility, Schema};
use clap::{arg, command, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "avrodisiac")]
#[command(about="an Avro schema linter", long_about=None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[command(arg_required_else_help = true)]
    Lint {
        #[arg(required = true)]
        path: PathBuf,
    },
    Compat {
        #[arg(required = true)]
        old: PathBuf,
        #[arg(required = true)]
        new: PathBuf,
    },
}

fn visit_dirs(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut result = Vec::new();
    if dir.is_dir() {
        if dir.file_name() == Some(OsStr::new(".git")) {
            return Ok(result);
        }
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                result.extend(visit_dirs(&path)?);
            } else {
                result.push(entry.path());
            }
        }
    } else {
        result.push(dir.to_path_buf());
    }
    Ok(result)
}

fn validate_schemas(path: &Path) -> Result<()> {
    let files = visit_dirs(path)?;
    let schemas: Vec<_> = files
        .iter()
        .filter(|f| f.extension().is_some_and(|e| e == "avsc"))
        .map(|f| String::from_utf8_lossy(&fs::read(f).expect("Unable to read file")).into_owned())
        .collect();
    let schemas: Vec<&str> = schemas.iter().map(String::as_str).collect();
    let _ = Schema::parse_list(&schemas)?;
    Ok(())
}

fn compare_schemas(old: &Path, new: &Path) -> Result<()> {
    let old_schema = Schema::parse_str(
        String::from_utf8_lossy(
            &fs::read(old)
                .unwrap_or_else(|e| panic!("Unable to read file {}: {}", old.display(), e)),
        )
        .as_ref(),
    )
    .unwrap_or_else(|e| panic!("Couldn't parse schema {}:{}", old.display(), e));
    let new_schema = Schema::parse_str(
        String::from_utf8_lossy(
            &fs::read(new)
                .unwrap_or_else(|e| panic!("Unable to read file {}: {}", new.display(), e)),
        )
        .as_ref(),
    )
    .unwrap_or_else(|e| panic!("Couldn't parse schema {}:{}", new.display(), e));
    SchemaCompatibility::mutual_read(&new_schema, &old_schema)?;
    Ok(())
}

fn main() -> Result<()> {
    let args = Cli::parse();

    match args.command {
        Commands::Lint { path } => match validate_schemas(&path) {
            Err(err) => {
                eprintln!("Schema(s) indalid: {:?}", err);
                std::process::exit(1);
            }
            Ok(_) => {}
        },
        Commands::Compat { old, new } => {
            let compatible = compare_schemas(&old, &new);
            if let Err(e) = compatible {
                eprintln!("Schemas incompatible: {} [{:?}]", e, e.source());
                std::process::exit(1);
            }
        }
    }
    Ok(())
}
