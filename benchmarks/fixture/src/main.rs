mod content;
mod http;
mod server;
mod stats;

#[tokio::main]
async fn main() {
    if std::env::args_os().nth(1).as_deref() == Some(std::ffi::OsStr::new("--version")) {
        println!("vise-bench-fixture {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    if server::run().await.is_err() {
        // Config and request text must never become diagnostics.
        eprintln!("benchmark fixture failed (invalid configuration or I/O error)");
        std::process::exit(1);
    }
}
