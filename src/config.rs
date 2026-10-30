//! User preferences, independent of the SQLite transfer journal.
use std::{
    net::IpAddr,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde::Deserialize;

mod storage;
#[cfg(test)]
mod tests;

pub fn default_contents() -> String {
    let token = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    format!(
        r#"[daemon]
host = "127.0.0.1"
port = 8080
VISE_API_TOKEN = "{token}"

[downloads]
default_dir = "~/Downloads/Vise"
max_concurrent = 5
min_split_size = 1048576 # bytes per unfinished portion; 0 disables tail splitting
global_speed_limit_kbps = 0 # restore saved cap; initially unlimited
# cookies_file = "~/cookies.txt" # optional Netscape cookie file
# user_agent = "CustomDownloader/1.0" # optional user agent for new tasks

[proxy]
url = "" # optional default proxy
"#
    )
}

#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FileConfig {
    pub daemon: Daemon,
    pub downloads: Downloads,
    pub proxy: Proxy,
}

// Deliberately omit Debug: these values may contain credentials.
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Daemon {
    pub host: IpAddr,
    pub port: u16,
    #[serde(rename = "VISE_API_TOKEN", alias = "auth_token")]
    pub api_token: String,
}

impl Default for Daemon {
    fn default() -> Self {
        Self {
            host: std::net::Ipv4Addr::LOCALHOST.into(),
            port: 8080,
            api_token: String::new(),
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Downloads {
    pub default_dir: PathBuf,
    pub max_concurrent: u16,
    pub min_split_size: u64,
    /// KiB per second; zero restores the journal cap (initially unlimited).
    pub global_speed_limit_kbps: u64,
    pub cookies_file: Option<PathBuf>,
    pub user_agent: Option<String>,
}

impl Default for Downloads {
    fn default() -> Self {
        Self {
            default_dir: "~/Downloads/Vise".into(),
            max_concurrent: 5,
            min_split_size: 1024 * 1024,
            global_speed_limit_kbps: 0,
            cookies_file: None,
            user_agent: None,
        }
    }
}

#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Proxy {
    pub url: String,
}

pub struct LoadedConfig {
    pub home: PathBuf,
    pub path: PathBuf,
    pub values: FileConfig,
}

/// Resolve the home directory and perform all file I/O on the blocking pool.
pub async fn load() -> Result<LoadedConfig> {
    load_from_home(home_directory()?).await
}

/// Explicit home injection keeps embedding applications and tests isolated.
pub async fn load_from_home(home: PathBuf) -> Result<LoadedConfig> {
    anyhow::ensure!(
        home.is_absolute(),
        "home directory must be an absolute path"
    );
    tokio::task::spawn_blocking(move || storage::load(home))
        .await
        .context("configuration loader task failed")?
}

/// Read client defaults without creating configuration, locks, or journal files.
pub async fn read_existing() -> Result<Option<LoadedConfig>> {
    let home = home_directory()?;
    anyhow::ensure!(
        home.is_absolute(),
        "home directory must be an absolute path"
    );
    tokio::task::spawn_blocking(move || storage::read_existing(home))
        .await
        .context("configuration reader task failed")?
}

pub fn expand_home(path: &Path, home: &Path) -> Result<PathBuf> {
    anyhow::ensure!(
        !path.as_os_str().is_empty(),
        "download directory must not be empty"
    );
    Ok(match path.strip_prefix("~") {
        Ok(suffix) => home.join(suffix),
        Err(_) => path.to_owned(),
    })
}

pub fn home_directory() -> Result<PathBuf> {
    #[cfg(windows)]
    let home = std::env::var_os("USERPROFILE")
        .filter(|v| !v.is_empty())
        .or_else(|| {
            let mut drive = std::env::var_os("HOMEDRIVE")?;
            drive.push(std::env::var_os("HOMEPATH")?);
            Some(drive)
        });
    #[cfg(not(windows))]
    let home = std::env::var_os("HOME").filter(|v| !v.is_empty());
    home.map(PathBuf::from)
        .context("cannot resolve the user's home directory for .vise/config.toml")
}
