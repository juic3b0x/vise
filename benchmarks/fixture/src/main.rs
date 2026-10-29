mod content;
mod http;
mod server;
mod stats;

#[tokio::main]
async fn main() {
    if server::run().await.is_err() {
        // Config and request text must never become diagnostics.
        eprintln!("benchmark fixture failed (invalid configuration or I/O error)");
        std::process::exit(1);
    }
}
