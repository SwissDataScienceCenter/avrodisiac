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

fn visit_dirs(dir: &Path, cb: &dyn Fn(&Path) -> Result<usize>) -> Result<usize> {
    let mut errors = 0;
    if dir.is_dir() {
        if dir.file_name() == Some(OsStr::new(".git")) {
            return Ok(0);
        }
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                visit_dirs(&path, cb)?;
            } else {
                errors += cb(&entry.path())?;
            }
        }
    } else {
        errors += cb(dir)?;
    }
    Ok(errors)
}

fn validate_schema(file: &Path) -> Result<usize> {
    if file.is_file() && file.extension() == Some(OsStr::new("avsc")) {
        let schema = Schema::parse_str(String::from_utf8_lossy(&fs::read(file)?).as_ref());
        if let Err(err) = schema {
            eprintln!("{}: {:?}", file.display(), err.to_string());
            return Ok(1);
        }
    }
    Ok(0)
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
        Commands::Lint { path } => {
            let errors = visit_dirs(&path, &validate_schema)?;
            if errors > 0 {
                eprintln!("Found {} errors.", errors);
                std::process::exit(1);
            }
        }
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
