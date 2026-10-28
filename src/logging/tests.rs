use super::*;
use std::{
    io::{self, Write},
    sync::{Arc, Mutex},
};
use tracing_subscriber::Layer;

#[derive(Clone, Default)]
struct Buffer(Arc<Mutex<Vec<u8>>>);
impl Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn protective_filter_drops_native_and_bridged_targets_without_muting_network_logs() {
    let output = Buffer::default();
    let writer = output.clone();
    let subscriber = tracing_subscriber::registry()
        .with(EnvFilter::new("trace"))
        .with(
            tracing_subscriber::fmt::layer()
                .without_time()
                .with_ansi(false)
                .with_writer(move || writer.clone())
                .with_filter(CredentialGuard),
        );
    tracing::subscriber::with_default(subscriber, || {
        tracing::error!(target:"cookie_store::cookie_store","native-secret");
        tracing::event!(target:"log", tracing::Level::DEBUG, { log.target = "cookie_store::cookie_store", message = "bridged-secret" });
        tracing::debug!(target:"reqwest::connect","visible-reqwest");
        tracing::debug!(target:"hyper::proto","visible-hyper");
        tracing::debug!(target:"h2::proto","visible-h2");
        tracing::debug!(target:"cookie_store::cookie_store_other","visible-boundary");
    });
    let bytes = output.0.lock().unwrap();
    let text = String::from_utf8_lossy(&bytes);
    assert!(!text.contains("native-secret") && !text.contains("bridged-secret"));
    for marker in [
        "visible-reqwest",
        "visible-hyper",
        "visible-h2",
        "visible-boundary",
    ] {
        assert!(text.contains(marker));
    }
}

#[test]
fn actual_cookie_store_log_bridge_uses_log_metadata_and_log_target_field() {
    // Install the existing tracing-log bridge via SubscriberInitExt, without
    // a formatter. The local layer below observes real dependency events.
    let _ = tracing_subscriber::registry().try_init();
    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<(String, String)>>>);
    #[derive(Default)]
    struct Target(String);
    impl Visit for Target {
        fn record_str(&mut self, field: &Field, value: &str) {
            if field.name() == "log.target" {
                self.0 = value.into();
            }
        }
        fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
    }
    impl<S: Subscriber> Layer<S> for Capture {
        fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
            let mut target = Target::default();
            event.record(&mut target);
            self.0
                .lock()
                .unwrap()
                .push((event.metadata().target().into(), target.0));
        }
    }
    let capture = Capture::default();
    let observer = capture.clone();
    tracing::subscriber::with_default(tracing_subscriber::registry().with(observer), || {
        reqwest::cookie::Jar::default().add_cookie_str(
            "session=synthetic; Path=/",
            &url::Url::parse("http://127.0.0.1/").unwrap(),
        );
    });
    assert!(
        capture
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|(raw, target)| raw == "log" && target == "cookie_store::cookie_store")
    );
}
