//! One-time imports from the selected output directory into the global journal.
use super::layout::validate_record;
use crate::{
    database::{Database, FILE_NAME},
    error::{Error, Result},
    model::LegacyJournal,
};
use std::path::Path;
use tokio::fs;

pub(super) async fn import_selected(database: &Database, output: &Path) -> Result<()> {
    let source = format!("legacy-output:{}", crate::paths::text(output)?);
    if database.source_imported(source.clone()).await? {
        return Ok(());
    }
    let directory = output.join(".vise");
    let sqlite = directory.join(FILE_NAME);
    if fs::try_exists(&sqlite).await? {
        return import_sqlite(database, source, sqlite).await;
    }
    import_json(database, source, &directory, output).await
}

async fn import_sqlite(
    database: &Database,
    source: String,
    path: std::path::PathBuf,
) -> Result<()> {
    if same_file::is_same_file(database.path(), &path)? {
        return Ok(());
    }
    let (records, rate) = Database::legacy_snapshot(path).await?;
    for record in &records {
        validate_record(record)?;
    }
    database.import_source(source, records, rate).await
}

async fn import_json(
    database: &Database,
    source: String,
    directory: &Path,
    output: &Path,
) -> Result<()> {
    let path = directory.join("state.json");
    let bytes = match fs::read(&path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let mut legacy: LegacyJournal = serde_json::from_slice(&bytes)?;
    if legacy.version != 1 {
        return Err(Error::Invalid("unsupported legacy journal version".into()));
    }
    for record in &mut legacy.downloads {
        crate::paths::resolve_legacy(record, output)?;
        validate_record(record)?;
    }
    database
        .import_source(source, legacy.downloads, None)
        .await?;
    // A durable source marker prevents resurrection even if backup rename fails.
    let backup = directory.join("state.json.migrated");
    if !fs::try_exists(&backup).await? {
        fs::rename(path, backup).await?;
        super::journal::sync_directory(directory.to_path_buf()).await?;
    }
    Ok(())
}
