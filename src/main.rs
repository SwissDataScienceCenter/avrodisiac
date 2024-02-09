use std::{
    ffi::OsStr,
    fs::{self, DirEntry},
    io,
    path::{Path, PathBuf},
};

use avro_rs::{schema_compatibility::SchemaCompatibility, Schema};
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

fn visit_dirs(dir: &Path, cb: &dyn Fn(&Path) -> usize) -> io::Result<usize> {
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
                errors += cb(&entry.path());
            }
        }
    } else {
        errors += cb(&dir);
    }
    Ok(errors)
}

fn validate_schema(file: &Path) -> usize {
    if file.is_file() && file.extension() == Some(OsStr::new("avsc")) {
        let schema = Schema::parse_str(
            String::from_utf8_lossy(
                &fs::read(file).expect(&format!("Unable to read file {:?}", file)),
            )
            .as_ref(),
        );
        if let Err(err) = schema {
            println!("{}: {:?}", file.display(), err.to_string());
            return 1;
        }
    }
    0
}

fn compare_schemas(old: &Path, new: &Path) -> io::Result<bool> {
    let old_schema = Schema::parse_str(
        String::from_utf8_lossy(&fs::read(old).expect(&format!("Unable to read file {:?}", old)))
            .as_ref(),
    )
    .expect(format!("Couldn't parse schema: {}", old.display()).as_str());
    let new_schema = Schema::parse_str(
        String::from_utf8_lossy(&fs::read(new).expect(&format!("Unable to read file {:?}", new)))
            .as_ref(),
    )
    .expect(format!("Couldn't parse schema: {}", new.display()).as_str());
    Ok(SchemaCompatibility::mutual_read(&new_schema, &old_schema))
}

fn main() -> Result<(), io::Error> {
    let args = Cli::parse();

    match args.command {
        Commands::Lint { path } => {
            let errors = visit_dirs(&path, &validate_schema)?;
            if errors > 0 {
                println!("Found {} errors.", errors);
                std::process::exit(1);
            }
        }
        Commands::Compat { old, new } => {
            let compatible = compare_schemas(&old, &new);
            if compatible.is_err() || compatible.is_ok_and(|b| !b) {
                println!("Schemas aren't compatible");
                std::process::exit(1);
            }
        }
    }
    Ok(())
}
