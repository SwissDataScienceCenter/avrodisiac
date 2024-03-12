use std::{
    ffi::OsStr,
    fs::{self},
    path::{Path, PathBuf},
};

use anyhow::{bail, Result};
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
        #[arg(short, long)]
        mutual: bool,
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

fn parse_schemas(files: Vec<PathBuf>) -> Result<Vec<Schema>> {
    let schemas: Vec<_> = files
        .iter()
        .filter(|f| f.extension().is_some_and(|e| e == "avsc"))
        .map(|f| String::from_utf8_lossy(&fs::read(f).expect("Unable to read file")).into_owned())
        .collect();
    let schemas: Vec<&str> = schemas.iter().map(String::as_str).collect();
    let parsed = Schema::parse_list(&schemas)?;
    Ok(parsed)
}

fn validate_schemas(path: &Path) -> Result<()> {
    let files = visit_dirs(path)?;
    let _ = parse_schemas(files)?;
    Ok(())
}

fn compare_schemas(old: &Path, new: &Path, mutual: bool) -> Result<()> {
    let old_files = visit_dirs(old)?;
    let old_schemas = parse_schemas(old_files)?;
    let new_files = visit_dirs(new)?;
    let new_schemas = parse_schemas(new_files)?;
    for schema in old_schemas {
        let new_schema = new_schemas
            .iter()
            .filter(|s| {
                s.name().expect("no name on new schema")
                    == schema.name().expect("no name on old schema")
            })
            .next();
        match (new_schema, mutual) {
            (Some(new_schema), true) => SchemaCompatibility::mutual_read(&schema, &new_schema)?,
            (Some(new_schema), false) => SchemaCompatibility::can_read(&schema, &new_schema)?,
            (None, _) => {
                bail!("schema {:?} does not exist anymore", schema.name())
            }
        }
    }
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
        Commands::Compat { old, new, mutual } => {
            let compatible = compare_schemas(&old, &new, mutual);
            if let Err(e) = compatible {
                eprintln!("Schemas incompatible: {} [{:?}]", e, e.source());
                std::process::exit(1);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;

    use super::*;

    use tempfile::tempdir;
    fn create_file(dir: &Path, name: &str, content: &str) {
        let path = dir.join(name);
        let mut file = File::create(path).unwrap();
        writeln!(file, "{}", content).unwrap();
    }

    #[test]
    fn test_schema_validation() -> Result<()> {
        let dir = tempdir()?;
        create_file(
            &dir.path(),
            "test.avsc",
            r#"{
               "name":"test",
               "namespace":"my.namespace",
               "type":"record",
               "fields":[
                   {
                       "name":  "myField",
                       "doc": "just a field",
                       "type":"int"
                   }
               ] 
            }"#,
        );
        validate_schemas(&dir.path())?;
        Ok(())
    }
    #[test]
    fn test_invalid_schema_validation() -> Result<()> {
        let dir = tempdir()?;
        create_file(
            &dir.path(),
            "test.avsc",
            r#"{
               "namespace":"my.namespace",
               "type":"record",
               "fields":[
                   {
                       "type":"int"
                   }
               ] 
            }"#,
        );
        assert!(validate_schemas(&dir.path()).is_err());
        Ok(())
    }

    #[test]
    fn test_schema_compatibility() -> Result<()> {
        let old_dir = tempdir()?;
        create_file(
            &old_dir.path(),
            "test.avsc",
            r#"{
               "name":"test",
               "namespace":"my.namespace",
               "type":"record",
               "fields":[
                   {
                       "name":  "myField",
                       "doc": "just a field",
                       "type":"int"
                   }
               ] 
            }"#,
        );

        let new_dir = tempdir()?;
        create_file(
            &new_dir.path(),
            "test.avsc",
            r#"{
               "name":"test",
               "namespace":"my.namespace",
               "type":"record",
               "fields":[
                   {
                       "name":  "myField",
                       "doc": "just a field",
                       "type":"int"
                   },
                   {
                       "name":  "myOtherField",
                       "doc": "just a field",
                       "type":"int",
                       "default":1
                   }
               ] 
            }"#,
        );
        compare_schemas(old_dir.path(), new_dir.path(), true)?;
        Ok(())
    }

    #[test]
    fn test_schema_incompatibility() -> Result<()> {
        let old_dir = tempdir()?;
        create_file(
            &old_dir.path(),
            "test.avsc",
            r#"{
               "name":"test",
               "namespace":"my.namespace",
               "type":"record",
               "fields":[
                   {
                       "name":  "myField",
                       "doc": "just a field",
                       "type":"int"
                   }
               ] 
            }"#,
        );

        let new_dir = tempdir()?;
        create_file(
            &new_dir.path(),
            "test.avsc",
            r#"{
               "name":"test",
               "namespace":"my.namespace",
               "type":"record",
               "fields":[
                   {
                       "name":  "myField",
                       "doc": "just a field",
                       "type":"string"
                   },
                   {
                       "name":  "myOtherField",
                       "doc": "just a field",
                       "type":"int",
                       "default":1
                   }
               ] 
            }"#,
        );
        assert!(compare_schemas(old_dir.path(), new_dir.path(), true).is_err());
        Ok(())
    }
    #[test]
    fn test_schema_read_compatibility() -> Result<()> {
        let old_dir = tempdir()?;
        create_file(
            &old_dir.path(),
            "test.avsc",
            r#"{
               "name":"test",
               "namespace":"my.namespace",
               "type":"record",
               "fields":[
                   {
                       "name":  "myField",
                       "doc": "just a field",
                       "type":"int"
                   }
               ] 
            }"#,
        );

        let new_dir = tempdir()?;
        create_file(
            &new_dir.path(),
            "test.avsc",
            r#"{
               "name":"test",
               "namespace":"my.namespace",
               "type":"record",
               "fields":[
                   {
                       "name":  "myField",
                       "doc": "just a field",
                       "type":"long"
                   }
               ] 
            }"#,
        );
        compare_schemas(old_dir.path(), new_dir.path(), false)?;
        Ok(())
    }

    #[test]
    fn test_schema_read_incompatibility() -> Result<()> {
        let old_dir = tempdir()?;
        create_file(
            &old_dir.path(),
            "test.avsc",
            r#"{
               "name":"test",
               "namespace":"my.namespace",
               "type":"record",
               "fields":[
                   {
                       "name":  "myField",
                       "doc": "just a field",
                       "type":"long"
                   }
               ] 
            }"#,
        );

        let new_dir = tempdir()?;
        create_file(
            &new_dir.path(),
            "test.avsc",
            r#"{
               "name":"test",
               "namespace":"my.namespace",
               "type":"record",
               "fields":[
                   {
                       "name":  "myField",
                       "doc": "just a field",
                       "type":"int"
                   }
               ] 
            }"#,
        );
        assert!(compare_schemas(old_dir.path(), new_dir.path(), false).is_err());
        Ok(())
    }

    #[test]
    fn test_schema_compatibility_multiple_files() -> Result<()> {
        let old_dir = tempdir()?;
        create_file(
            &old_dir.path(),
            "test.avsc",
            r#"{
               "name":"test",
               "namespace":"my.namespace",
               "type":"record",
               "fields":[
                   {
                       "name": "myField",
                       "doc": "just a field",
                       "type":"int"
                   },
                   {
                       "name": "nest",
                       "doc": "nested field",
                       "type": "my.namespace.nested"
                   }
               ] 
            }"#,
        );

        create_file(
            &old_dir.path(),
            "test2.avsc",
            r#"{
               "name":"nested",
               "namespace":"my.namespace",
               "type":"record",
               "fields":[
                   {
                       "name": "myNestedField",
                       "doc": "just a field",
                       "type":"int"
                   }
               ] 
            }"#,
        );

        let new_dir = tempdir()?;
        create_file(
            &new_dir.path(),
            "test.avsc",
            r#"{
               "name":"test",
               "namespace":"my.namespace",
               "type":"record",
               "fields":[
                   {
                       "name":  "myField",
                       "doc": "just a field",
                       "type":"int"
                   },
                   {
                       "name":  "myOtherField",
                       "doc": "just a field",
                       "type":"int",
                       "default":1
                   },
                   {
                       "name": "nest",
                       "doc": "nested field",
                       "type": "my.namespace.nested"
                   }
               ] 
            }"#,
        );
        create_file(
            &new_dir.path(),
            "test3.avsc",
            r#"{
               "name":"nested",
               "namespace":"my.namespace",
               "type":"record",
               "fields":[
                   {
                       "name": "myNestedField",
                       "doc": "just a field",
                       "type":"int"
                   }
               ] 
            }"#,
        );
        compare_schemas(old_dir.path(), new_dir.path(), true)?;
        Ok(())
    }
    #[test]
    fn test_schema_incompatibility_multiple_files() -> Result<()> {
        let old_dir = tempdir()?;
        create_file(
            &old_dir.path(),
            "test.avsc",
            r#"{
               "name":"test",
               "namespace":"my.namespace",
               "type":"record",
               "fields":[
                   {
                       "name": "myField",
                       "doc": "just a field",
                       "type":"int"
                   },
                   {
                       "name": "nest",
                       "doc": "nested field",
                       "type": "my.namespace.nested"
                   }
               ] 
            }"#,
        );

        create_file(
            &old_dir.path(),
            "test2.avsc",
            r#"{
               "name":"nested",
               "namespace":"my.namespace",
               "type":"record",
               "fields":[
                   {
                       "name": "myNestedField",
                       "doc": "just a field",
                       "type":"int"
                   }
               ] 
            }"#,
        );

        let new_dir = tempdir()?;
        create_file(
            &new_dir.path(),
            "test.avsc",
            r#"{
               "name":"test",
               "namespace":"my.namespace",
               "type":"record",
               "fields":[
                   {
                       "name":  "myField",
                       "doc": "just a field",
                       "type":"int"
                   },
                   {
                       "name":  "myOtherField",
                       "doc": "just a field",
                       "type":"int",
                       "default":1
                   },
                   {
                       "name": "nest",
                       "doc": "nested field",
                       "type": "my.namespace.nested"
                   }
               ] 
            }"#,
        );
        create_file(
            &new_dir.path(),
            "test3.avsc",
            r#"{
               "name":"nested",
               "namespace":"my.namespace",
               "type":"record",
               "fields":[
                   {
                       "name": "myNestedField",
                       "doc": "just a field",
                       "type":"string"
                   }
               ] 
            }"#,
        );
        assert!(compare_schemas(old_dir.path(), new_dir.path(), true).is_err());
        Ok(())
    }
}
