//! Shared filesystem invariants for journals and transfer paths.
use crate::{
    error::{Error, Result},
    model::Record,
};
use std::{
    fs,
    path::{Component, Path},
};

mod checked;
pub(crate) use checked::{bounded_file, directory, staging_files};
#[cfg(test)]
mod tests;

pub(crate) fn private_directory(path: &Path) -> std::io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)
}

pub(crate) fn parent(path: &Path) -> Result<&Path> {
    path.parent()
        .ok_or_else(|| Error::Invalid("task path has no parent directory".into()))
}

pub(crate) fn text(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| Error::Invalid("journal paths must be valid UTF-8".into()))
}

/// Reject lexical escapes before canonicalization can erase their components.
pub(crate) fn validate_absolute(path: &Path) -> Result<()> {
    if !path.is_absolute()
        || path
            .components()
            .any(|part| matches!(part, Component::ParentDir | Component::CurDir))
    {
        return Err(Error::Invalid(
            "paths must be full absolute paths without traversal".into(),
        ));
    }
    text(path)?;
    Ok(())
}

pub(crate) fn validate(record: &Record) -> Result<()> {
    for path in [&record.output_path, &record.part_path] {
        validate_absolute(path)?;
    }
    if record
        .output_path
        .file_name()
        .and_then(|name| name.to_str())
        != Some(record.filename.as_str())
        || record.part_path
            != parent(&record.output_path)?
                .join(".vise")
                .join(format!("{}.part", record.id))
    {
        return Err(Error::Invalid(
            "journal destination or staging path does not match its task".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn unvalidated_path(root: &Path, suffix: &str) -> std::path::PathBuf {
    // PathBuf::join normalizes dot components for Windows verbatim paths. Raw
    // concatenation keeps attack input intact so the validator sees it first.
    let mut raw = root.as_os_str().to_os_string();
    raw.push(std::path::MAIN_SEPARATOR_STR);
    raw.push(suffix);
    raw.into()
}

pub(crate) fn resolve_legacy(record: &mut Record, output: &Path) -> Result<()> {
    let mut components = Path::new(&record.filename).components();
    if !matches!(components.next(), Some(Component::Normal(_)))
        || components.next().is_some()
        || record.filename.contains('\\')
    {
        return Err(Error::Invalid("invalid filename in legacy journal".into()));
    }
    record.output_path = output.join(&record.filename);
    record.part_path = output.join(".vise").join(format!("{}.part", record.id));
    validate(record)
}
