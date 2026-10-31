use super::*;
use indicatif::{ProgressDrawTarget, TermLike};
use std::{
    io,
    sync::{Arc, Mutex},
};

mod reference;

#[derive(Clone, Debug, Default)]
struct Terminal(Arc<Mutex<Vec<u8>>>);
impl Terminal {
    fn write(&self, text: &str) -> io::Result<()> {
        self.0.lock().unwrap().extend_from_slice(text.as_bytes());
        Ok(())
    }
    fn cursor(&self, n: usize, direction: char) -> io::Result<()> {
        if n > 0 {
            self.write(&format!("\x1b[{n}{direction}"))?;
        }
        Ok(())
    }
}
impl TermLike for Terminal {
    fn width(&self) -> u16 {
        160
    }
    fn height(&self) -> u16 {
        80
    }
    fn move_cursor_up(&self, n: usize) -> io::Result<()> {
        self.cursor(n, 'A')
    }
    fn move_cursor_down(&self, n: usize) -> io::Result<()> {
        self.cursor(n, 'B')
    }
    fn move_cursor_right(&self, n: usize) -> io::Result<()> {
        self.cursor(n, 'C')
    }
    fn move_cursor_left(&self, n: usize) -> io::Result<()> {
        self.cursor(n, 'D')
    }
    fn write_line(&self, s: &str) -> io::Result<()> {
        self.write(s)?;
        self.write("\n")
    }
    fn write_str(&self, s: &str) -> io::Result<()> {
        self.write(s)
    }
    fn clear_line(&self) -> io::Result<()> {
        self.write("\r\x1b[2K")
    }
    fn flush(&self) -> io::Result<()> {
        Ok(())
    }
}

fn snapshot(status: Status) -> Download {
    crate::model::Record {
        id: Uuid::from_u128(1),
        url: "http://127.0.0.1:1/unused".into(),
        filename: "escaped\nfile.bin".into(),
        output_path: "/unused/output.bin".into(),
        part_path: "/unused/.vise/1.part".into(),
        proxy_url: None,
        cookies_file: None,
        request_credentials: None,
        status,
        total_bytes: Some(4096),
        ranged: false,
        validator: None,
        chunks: vec![crate::model::Chunk {
            start: 0,
            end: Some(4096),
            written: if status == Status::Completed {
                4096
            } else {
                2048
            },
            status: crate::model::ChunkStatus::Downloading,
        }],
        error: None,
        checksums: Default::default(),
        speed: 1024.0,
        created_at: 0,
    }
    .snapshot()
}

fn compare(states: &[Status], hidden: bool) {
    let actual = Terminal::default();
    let expected = Terminal::default();
    let mut display = ProgressDisplay::default();
    display
        .progress
        .set_draw_target(ProgressDrawTarget::hidden());
    let legacy = MultiProgress::with_draw_target(ProgressDrawTarget::hidden());
    let mut bars = BTreeMap::new();
    for status in states {
        let item = snapshot(*status);
        display.render(std::slice::from_ref(&item)).unwrap();
        reference::render(&legacy, &mut bars, std::slice::from_ref(&item)).unwrap();
        // Force complete frames, without wall-clock redraw throttling. All data
        // comes from fixed snapshots; timing and terminal refresh counts are not
        // an output-format contract.
        if !hidden {
            display
                .progress
                .set_draw_target(ProgressDrawTarget::term_like(Box::new(actual.clone())));
            legacy.set_draw_target(ProgressDrawTarget::term_like(Box::new(expected.clone())));
        }
        display.progress.println("snapshot").unwrap();
        legacy.println("snapshot").unwrap();
        display
            .progress
            .set_draw_target(ProgressDrawTarget::hidden());
        legacy.set_draw_target(ProgressDrawTarget::hidden());
    }
    display.finish();
    for bar in bars.values() {
        if !bar.is_finished() {
            bar.finish_and_clear();
        }
    }
    assert_eq!(*actual.0.lock().unwrap(), *expected.0.lock().unwrap());
    if hidden {
        assert!(actual.0.lock().unwrap().is_empty());
    } else {
        assert!(!actual.0.lock().unwrap().is_empty());
    }
}

#[test]
fn completion_output_is_byte_identical() {
    compare(
        &[
            Status::Queued,
            Status::Probing,
            Status::Downloading,
            Status::Downloading,
            Status::Verifying,
            Status::Completed,
        ],
        false,
    );
}
#[test]
fn failure_output_is_byte_identical() {
    compare(
        &[Status::Downloading, Status::Failed, Status::Failed],
        false,
    );
}
#[test]
fn pause_resume_output_is_byte_identical() {
    compare(
        &[
            Status::Downloading,
            Status::Paused,
            Status::Paused,
            Status::Queued,
            Status::Probing,
            Status::Downloading,
            Status::Completed,
        ],
        false,
    );
}
#[test]
fn redirected_output_is_byte_identical() {
    compare(
        &[
            Status::Downloading,
            Status::Paused,
            Status::Downloading,
            Status::Failed,
            Status::Queued,
            Status::Completed,
        ],
        true,
    );
}

#[test]
fn changed_names_rates_eta_lengths_and_removed_entries_match_the_old_renderer() {
    let mut display = ProgressDisplay::default();
    display
        .progress
        .set_draw_target(ProgressDrawTarget::hidden());
    let legacy = MultiProgress::with_draw_target(ProgressDrawTarget::hidden());
    let mut bars = BTreeMap::new();
    let mut item = snapshot(Status::Downloading);
    for (name, total, rate, eta) in [
        ("escaped\nfile.bin", Some(4096), 1024.0, Some(2.0)),
        ("renamed.bin", None, 1536.0, Some(0.1)),
        ("renamed.bin", Some(8192), 1024.0 * 1024.0, None),
        ("renamed.bin", Some(8192), f64::NAN, Some(f64::INFINITY)),
    ] {
        item.filename = name.into();
        item.total_bytes = total;
        item.bytes_per_second = rate;
        item.eta_seconds = eta;
        display.render(std::slice::from_ref(&item)).unwrap();
        reference::render(&legacy, &mut bars, std::slice::from_ref(&item)).unwrap();
        let actual = &display.bars.get(&item.id).unwrap().bar;
        let expected = bars.get(&item.id).unwrap();
        assert_eq!(actual.prefix().as_bytes(), expected.prefix().as_bytes());
        assert_eq!(actual.message().as_bytes(), expected.message().as_bytes());
        assert_eq!(actual.position(), expected.position());
        assert_eq!(actual.length(), expected.length());
    }
    assert_eq!(
        display.bars.get(&item.id).unwrap().bar.prefix(),
        "00000000 renamed.bin"
    );
    display.render(&[]).unwrap();
    reference::render(&legacy, &mut bars, &[]).unwrap();
    assert!(display.bars.is_empty() && bars.is_empty());
}
