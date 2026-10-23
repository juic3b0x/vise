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

pub(crate) fn validate(record: &Record) -> Result<()> {
    for path in [&record.output_path, &record.part_path] {
        if !path.is_absolute()
            || path
                .components()
                .any(|c| matches!(c, Component::ParentDir | Component::CurDir))
        {
            return Err(Error::Invalid(
                "journal task paths must be full absolute paths without traversal".into(),
            ));
        }
        text(path)?;
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
