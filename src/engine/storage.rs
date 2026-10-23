//! Blocking filesystem preparation and interprocess engine ownership.
use super::Config;
use crate::{
    config,
    error::{Error, Result},
    paths,
};
use fs2::FileExt;
use std::{
    collections::BTreeSet,
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
};

pub(super) use crate::paths::parent;

pub(super) async fn admission_directories(output: PathBuf) -> Result<(PathBuf, PathBuf)> {
    tokio::task::spawn_blocking(move || {
        let output = paths::directory(&output)?;
        let staging = paths::directory(&paths::bounded_file(&output, ".vise")?)?;
        if !staging.starts_with(&output) || staging.parent() != Some(output.as_path()) {
            return Err(Error::Invalid(
                "staging directory escapes the output directory".into(),
            ));
        }
        Ok((output, staging))
    })
    .await
    .map_err(|error| Error::Worker(error.to_string()))?
}

pub(super) async fn prepare(config: &mut Config) -> Result<(PathBuf, File)> {
    let home = match config.home_dir.take() {
        Some(home) => home,
        None => config::home_directory().map_err(|error| Error::Invalid(error.to_string()))?,
    };
    let output = config.output_dir.clone();
    let (home, output, directory, lock) = tokio::task::spawn_blocking(move || {
        if !home.is_absolute() {
            return Err(Error::Invalid("home directory must be absolute".into()));
        }
        paths::private_directory(&home.join(".vise"))?;
        let home = std::fs::canonicalize(home)?;
        let directory = home.join(".vise");
        let lock = lock(
            &directory.join("engine.lock"),
            "global engine; stop the other Vise process or use its API",
        )?;
        std::fs::create_dir_all(&output)?;
        let output = std::fs::canonicalize(output)?;
        paths::text(&output)?;
        Ok::<_, Error>((home, output, directory, lock))
    })
    .await
    .map_err(|error| Error::Worker(error.to_string()))??;
    config.home_dir = Some(home);
    config.output_dir = output;
    Ok((directory, lock))
}

pub(super) async fn lock_outputs(directories: BTreeSet<PathBuf>) -> Result<Vec<File>> {
    tokio::task::spawn_blocking(move || {
        directories
            .into_iter()
            .map(|directory| {
                // Preserve offline/missing destinations; execution reports their I/O error.
                if !directory.is_dir() {
                    return Ok(None);
                }
                let staging = directory.join(".vise");
                paths::private_directory(&staging)?;
                lock(
                    &staging.join("lock"),
                    "output directory; another Vise process may be using its staging files",
                )
                .map(Some)
            })
            .collect::<Result<Vec<_>>>()
            .map(|locks| locks.into_iter().flatten().collect())
    })
    .await
    .map_err(|error| Error::Worker(error.to_string()))?
}

fn lock(path: &Path, purpose: &str) -> Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).truncate(false).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    file.try_lock_exclusive()
        .map_err(|error| Error::Conflict(format!("cannot lock {purpose}: {error}")))?;
    Ok(file)
}
