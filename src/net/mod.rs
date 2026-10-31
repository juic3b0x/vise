//! Stateless pooled HTTP clients and isolated per-task credentials/proxy routes.
mod cookies;
pub mod credentials;
mod request;
pub(crate) use cookies::load as load_cookies;
pub(crate) use request::Transport;

use crate::{error::Result, proxy::ProxyUrl};
use reqwest::Client;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

pub(crate) async fn client(
    read_timeout: Duration,
    route: Option<&ProxyUrl>,
    cookies_file: Option<&Path>,
) -> Result<(Client, Option<PathBuf>)> {
    let mut builder = Client::builder()
        .user_agent(concat!("Vise/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_secs(15))
        .read_timeout(read_timeout)
        .redirect(reqwest::redirect::Policy::limited(10))
        .pool_max_idle_per_host(64);
    if let Some(route) = route {
        builder = route.apply(builder)?;
    }
    let path = if let Some(path) = cookies_file {
        let loaded = cookies::load(path.to_path_buf()).await?;
        builder = builder.cookie_provider(loaded.jar);
        Some(loaded.path)
    } else {
        None
    };
    Ok((builder.build()?, path))
}
