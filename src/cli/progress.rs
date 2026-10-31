//! Snapshot-only progress rendering shared by local and daemon client modes.
use crate::{
    error::{Error, Result},
    model::{Download, Status},
};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

#[cfg_attr(not(test), derive(Default))]
pub struct ProgressDisplay {
    progress: MultiProgress,
    bars: BTreeMap<Uuid, CachedBar>,
    styles: BTreeMap<bool, ProgressStyle>,
}

#[cfg(test)]
thread_local! {
    static DISPLAY_WORK: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}

#[cfg(test)]
pub(super) fn display_work() -> (usize, usize) {
    DISPLAY_WORK.get()
}

#[cfg(test)]
impl Default for ProgressDisplay {
    fn default() -> Self {
        let (created, rendered) = DISPLAY_WORK.get();
        DISPLAY_WORK.set((created + 1, rendered));
        Self {
            progress: MultiProgress::default(),
            bars: BTreeMap::new(),
            styles: BTreeMap::new(),
        }
    }
}
impl ProgressDisplay {
    pub fn messages(&self) -> MultiProgress {
        self.progress.clone()
    }
    pub fn render(&mut self, downloads: &[Download]) -> Result<()> {
        #[cfg(test)]
        {
            let (created, rendered) = DISPLAY_WORK.get();
            DISPLAY_WORK.set((created, rendered + 1));
        }
        render(&self.progress, &mut self.bars, &mut self.styles, downloads)
    }
    pub fn finish(&self) {
        for cached in self.bars.values() {
            let bar = &cached.bar;
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

#[derive(Default)]
struct CachedText {
    prefix: Option<(String, String)>,
    message: Option<(MessageFields, String)>,
}
#[derive(PartialEq)]
struct MessageFields {
    status: Status,
    speed: u64,
    eta: Option<u64>,
}
struct CachedBar {
    bar: ProgressBar,
    text: CachedText,
}
impl CachedText {
    fn prefix(&mut self, item: &Download) -> String {
        if self
            .prefix
            .as_ref()
            .is_none_or(|(filename, _)| filename != &item.filename)
        {
            let id = item.id.to_string();
            let text = format!(
                "{} {}",
                id.get(..8).unwrap_or(&id),
                item.filename.escape_debug()
            );
            self.prefix = Some((item.filename.clone(), text));
        }
        self.prefix
            .as_ref()
            .map(|(_, text)| text.clone())
            .unwrap_or_default()
    }
    fn message(&mut self, item: &Download) -> String {
        let fields = MessageFields {
            status: item.status,
            speed: item.bytes_per_second.to_bits(),
            eta: item.eta_seconds.map(f64::to_bits),
        };
        if self
            .message
            .as_ref()
            .is_none_or(|(previous, _)| previous != &fields)
        {
            let eta = item
                .eta_seconds
                .map(|s| format!("{}s", s.ceil() as u64))
                .unwrap_or_else(|| "—".into());
            let text = format!(
                "{:?} · {}/s · ETA {}",
                item.status,
                bytes(item.bytes_per_second),
                eta
            );
            self.message = Some((fields, text));
        }
        self.message
            .as_ref()
            .map(|(_, text)| text.clone())
            .unwrap_or_default()
    }
}

fn style(styles: &mut BTreeMap<bool, ProgressStyle>, known: bool) -> Result<ProgressStyle> {
    use std::collections::btree_map::Entry;
    Ok(match styles.entry(known) {
        Entry::Occupied(entry) => entry.get().clone(),
        Entry::Vacant(entry) => {
            let template = if known {
                "{prefix:.cyan} {msg} [{bar:28.cyan/blue}] {percent:>3}% {bytes}/{total_bytes}"
            } else {
                "{prefix:.cyan} {msg} {spinner:.cyan} {bytes}"
            };
            entry
                .insert(
                    ProgressStyle::with_template(template)
                        .map_err(|e| Error::Invalid(e.to_string()))?
                        .progress_chars("=>-"),
                )
                .clone()
        }
    })
}

fn render(
    progress: &MultiProgress,
    bars: &mut BTreeMap<Uuid, CachedBar>,
    styles: &mut BTreeMap<bool, ProgressStyle>,
    downloads: &[Download],
) -> Result<()> {
    let present_ids: BTreeSet<_> = downloads.iter().map(|item| item.id).collect();
    bars.retain(|id, cached| {
        let present = present_ids.contains(id);
        if !present {
            cached.bar.finish_and_clear();
        }
        present
    });
    for item in downloads {
        let cached = bars.entry(item.id).or_insert_with(|| {
            let bar = match item.total_bytes {
                Some(total) => ProgressBar::new(total),
                None => ProgressBar::new_spinner(),
            };
            CachedBar {
                bar: progress.add(bar),
                text: CachedText::default(),
            }
        });
        cached
            .bar
            .set_style(style(styles, item.total_bytes.is_some())?);
        let prefix = cached.text.prefix(item);
        cached.bar.set_prefix(prefix);
        if let Some(total) = item.total_bytes {
            cached.bar.set_length(total);
        }
        cached.bar.set_position(item.downloaded_bytes);
        let message = cached.text.message(item);
        let bar = &cached.bar;
        // Preserve setter/redraw side effects even when the formatted text was
        // reused; only parsing and formatting work is cached.
        bar.set_message(message);
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

#[cfg(test)]
mod tests;
