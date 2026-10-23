//! Filesystem checks run on the blocking pool by engine callers. Stored paths
//! must keep their canonical identity; a substituted symlink is not a new root.
use super::{parent, validate};
use crate::{
    error::{Error, Result},
    model::Record,
};
use std::{
    fs, io,
    path::{Component, Path, PathBuf},
};

pub(crate) fn directory(path: &Path) -> Result<PathBuf> {
    if !path.is_absolute()
        || path
            .components()
            .any(|part| matches!(part, Component::ParentDir | Component::CurDir))
    {
        return Err(Error::Invalid(
            "directory must be an absolute path without traversal".into(),
        ));
    }
    let canonical = fs::canonicalize(path)?;
    if !canonical.starts_with(path) || canonical != path {
        return Err(Error::Invalid(
            "directory no longer matches its canonical path".into(),
        ));
    }
    // Normalize, then check the boundary BEFORE any metadata/open sink. Use the
    // checked value so both runtime callers and taint analysis retain the guard.
    if !canonical.try_exists()? {
        return Err(io::Error::new(io::ErrorKind::NotFound, "directory does not exist").into());
    }
    if !canonical.is_dir() {
        return Err(io::Error::new(io::ErrorKind::NotADirectory, "path is not a directory").into());
    }
    Ok(canonical)
}

/// Construct one immediate child, never accepting an absolute path or traversal.
pub(crate) fn bounded_file(root: &Path, name: &str) -> Result<PathBuf> {
    let mut parts = Path::new(name).components();
    if !root.is_absolute()
        || root
            .components()
            .any(|part| matches!(part, Component::ParentDir | Component::CurDir))
        || !matches!(parts.next(), Some(Component::Normal(_)))
        || parts.next().is_some()
        || name.contains(['/', '\\', ':', '\0'])
    {
        return Err(Error::Invalid(
            "filename must be a single relative path component".into(),
        ));
    }
    let path = root.join(name);
    if !path.starts_with(root) || path.parent() != Some(root) {
        return Err(Error::Invalid(
            "file path escapes its expected directory".into(),
        ));
    }
    Ok(path)
}

/// Rebuild staging names from a typed UUID, checking both the stored layout and
/// existing filesystem entries before checkpoints or destructive operations.
pub(crate) fn staging_files(record: &Record) -> Result<Option<[PathBuf; 2]>> {
    validate(record)?;
    let Some(output) = available_directory(parent(&record.output_path)?)? else {
        return Ok(None);
    };
    let Some(staging) = available_directory(&bounded_file(&output, ".vise")?)? else {
        return Ok(None);
    };
    let files = [
        bounded_file(&staging, &format!("{}.part", record.id))?,
        bounded_file(&staging, &format!("{}.published", record.id))?,
    ];
    for path in &files {
        regular_file(&staging, path)?;
    }
    Ok(Some(files))
}

fn available_directory(path: &Path) -> Result<Option<PathBuf>> {
    match directory(path) {
        Ok(path) => Ok(Some(path)),
        Err(Error::Io(error)) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn regular_file(root: &Path, path: &Path) -> Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(Error::Invalid(
            "staging path must be a regular file, not a symlink".into(),
        ));
    }
    let canonical = match fs::canonicalize(path) {
        Ok(canonical) => canonical,
        // Publication can rename/unlink an entry between metadata and this call.
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if !canonical.starts_with(root) || canonical != path {
        return Err(Error::Invalid(
            "staging file escapes its expected directory".into(),
        ));
    }
    Ok(())
}
