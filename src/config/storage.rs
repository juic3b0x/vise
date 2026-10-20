//! Blocking, serialized first-run creation; existing files are never rewritten.
use std::{
    fs::{self, OpenOptions},
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use fs2::FileExt;

use super::{FileConfig, LoadedConfig, default_contents};

pub(super) fn load(home: PathBuf) -> Result<LoadedConfig> {
    let directory = home.join(".vise");
    create_directory(&directory)?;
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let lock = options
        .open(directory.join("config.lock"))
        .context("cannot open configuration lock")?;
    FileExt::lock_exclusive(&lock).context("cannot lock configuration")?;
    let path = directory.join("config.toml");
    let source = match fs::read_to_string(&path) {
        Ok(source) => source,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            initialize(&path)?;
            fs::read_to_string(&path).with_context(|| format!("cannot read {}", path.display()))?
        }
        Err(error) => return Err(error).with_context(|| format!("cannot read {}", path.display())),
    };
    // TOML's normal Display can quote values and whole source lines, including secrets.
    let values: FileConfig = toml::from_str(&source).map_err(|error: toml::de::Error| {
        let location = error.span().map(|range| format!(" at bytes {}..{}", range.start, range.end)).unwrap_or_default();
        anyhow::anyhow!("invalid TOML or configuration fields in {}{location}; check section names, field types, and syntax", path.display())
    })?;
    Ok(LoadedConfig { home, path, values })
}

fn create_directory(path: &Path) -> Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(path)
        .with_context(|| format!("cannot create {}", path.display()))
}

fn initialize(path: &Path) -> Result<()> {
    let temporary = path.with_file_name(format!(".config-{}.tmp", uuid::Uuid::new_v4()));
    let result = publish_default(&temporary, path);
    let _ = fs::remove_file(&temporary);
    result?;
    #[cfg(unix)]
    fs::File::open(
        path.parent()
            .context("configuration has no parent directory")?,
    )?
    .sync_all()
    .context("cannot synchronize configuration directory")?;
    Ok(())
}

fn publish_default(temporary: &Path, path: &Path) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(temporary)
        .context("cannot stage default configuration")?;
    file.write_all(default_contents().as_bytes())
        .context("cannot write default configuration")?;
    file.sync_all()
        .context("cannot synchronize default configuration")?;
    drop(file);
    // Publish without overwriting a file created by another process or editor.
    match fs::hard_link(temporary, path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error).with_context(|| format!("cannot publish {}", path.display())),
    }
}
