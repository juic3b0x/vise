//! Snapshot-only progress rendering shared by local and daemon client modes.
use crate::{
    error::{Error, Result},
    model::{Download, Status},
};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

#[derive(Default)]
pub struct ProgressDisplay {
    progress: MultiProgress,
    bars: BTreeMap<Uuid, ProgressBar>,
}
impl ProgressDisplay {
    pub fn messages(&self) -> MultiProgress {
        self.progress.clone()
    }
    pub fn render(&mut self, downloads: &[Download]) -> Result<()> {
        render(&self.progress, &mut self.bars, downloads)
    }
    pub fn finish(&self) {
        for bar in self.bars.values() {
            if !bar.is_finished() {
                bar.finish_and_clear();
            }
        }
    }
}
impl Drop for ProgressDisplay {
    fn drop(&mut self) {
        self.finish();
    }
}

fn render(
    progress: &MultiProgress,
    bars: &mut BTreeMap<Uuid, ProgressBar>,
    downloads: &[Download],
) -> Result<()> {
    let present_ids: BTreeSet<_> = downloads.iter().map(|item| item.id).collect();
    bars.retain(|id, bar| {
        let present = present_ids.contains(id);
        if !present {
            bar.finish_and_clear();
        }
        present
    });
    for item in downloads {
        let bar = bars.entry(item.id).or_insert_with(|| {
            let bar = match item.total_bytes {
                Some(total) => ProgressBar::new(total),
                None => ProgressBar::new_spinner(),
            };
            progress.add(bar)
        });
        let template = if item.total_bytes.is_some() {
            "{prefix:.cyan} {msg} [{bar:28.cyan/blue}] {percent:>3}% {bytes}/{total_bytes}"
        } else {
            "{prefix:.cyan} {msg} {spinner:.cyan} {bytes}"
        };
        bar.set_style(
            ProgressStyle::with_template(template)
                .map_err(|e| Error::Invalid(e.to_string()))?
                .progress_chars("=>-"),
        );
        let id = item.id.to_string();
        bar.set_prefix(format!(
            "{} {}",
            id.get(..8).unwrap_or(&id),
            item.filename.escape_debug()
        ));
        if let Some(total) = item.total_bytes {
            bar.set_length(total);
        }
        bar.set_position(item.downloaded_bytes);
        let eta = item
            .eta_seconds
            .map(|s| format!("{}s", s.ceil() as u64))
            .unwrap_or_else(|| "—".into());
        bar.set_message(format!(
            "{:?} · {}/s · ETA {}",
            item.status,
            bytes(item.bytes_per_second),
            eta
        ));
        bar.tick();
        if matches!(
            item.status,
            Status::Completed | Status::Failed | Status::Paused | Status::Corrupt
        ) {
            if !bar.is_finished() {
                bar.finish();
            }
        } else if bar.is_finished() {
            bar.reset();
            bar.set_position(item.downloaded_bytes);
        }
    }
    Ok(())
}

fn bytes(value: f64) -> String {
    if value >= 1024.0 * 1024.0 {
        format!("{:.1} MiB", value / (1024.0 * 1024.0))
    } else if value >= 1024.0 {
        format!("{:.1} KiB", value / 1024.0)
    } else {
        format!("{value:.0} B")
    }
}
