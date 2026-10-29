use std::{
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

pub const INPUTS: [&str; 5] = [
    "src",
    "Cargo.toml",
    "Cargo.lock",
    "build.rs",
    "build_support.rs",
];

#[derive(Default)]
pub struct Overrides {
    pub commit: Option<String>,
    pub date: Option<String>,
    pub dirty: Option<String>,
}

pub fn format_version(
    version: &str,
    commit: Option<&str>,
    date: Option<&str>,
    dirty: bool,
) -> String {
    let Some(commit) =
        commit.filter(|s| (7..=64).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_hexdigit()))
    else {
        return version.to_owned();
    };
    let Some(short) = commit.get(..7) else {
        return version.to_owned();
    };
    let suffix = if dirty { "-dirty" } else { "" };
    let date = date
        .filter(|date| valid_date(date))
        .map(|date| format!(" {date}"))
        .unwrap_or_default();
    format!("{version} ({}{suffix}{date})", short.to_ascii_lowercase())
}

fn valid_date(date: &str) -> bool {
    if date.len() != 10 {
        return false;
    }
    let fields: Vec<_> = date.split('-').collect();
    let [year, month, day] = fields.as_slice() else {
        return false;
    };
    if year.len() != 4
        || month.len() != 2
        || day.len() != 2
        || !fields.iter().all(|s| s.bytes().all(|b| b.is_ascii_digit()))
    {
        return false;
    }
    let (Ok(year), Ok(month), Ok(day)) =
        (year.parse::<u16>(), month.parse::<u8>(), day.parse::<u8>())
    else {
        return false;
    };
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days = match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        _ => 0,
    };
    year != 0 && day != 0 && day <= days
}

pub fn resolve(
    version: &str,
    root: &Path,
    git: &OsStr,
    overrides: &Overrides,
) -> (String, Vec<PathBuf>) {
    let fallback = || (version.to_owned(), Vec::new());
    if overrides.commit.as_deref() == Some("") {
        return fallback();
    }
    let dirty = match overrides.dirty.as_deref() {
        Some("true" | "1") => Some(true),
        Some("false" | "0") => Some(false),
        None => None,
        _ => return fallback(),
    };
    if overrides
        .date
        .as_deref()
        .is_some_and(|date| !valid_date(date))
    {
        return fallback();
    }
    let metadata = match &overrides.commit {
        Some(commit) => Some((commit.clone(), String::new(), false, vec![])),
        None => discover(root, git),
    };
    let Some((commit, date, detected_dirty, watches)) = metadata else {
        return fallback();
    };
    (
        format_version(
            version,
            Some(&commit),
            overrides.date.as_deref().or(Some(&date)),
            dirty.unwrap_or(detected_dirty),
        ),
        watches,
    )
}

fn run(root: &Path, git: &OsStr, args: &[&str]) -> Option<String> {
    let output = Command::new(git)
        .current_dir(root)
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_TERMINAL_PROMPT", "0")
        .args(["-c", "core.fsmonitor=false"])
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(
        String::from_utf8(output.stdout)
            .ok()?
            .trim_end_matches(['\r', '\n'])
            .to_owned(),
    )
}

fn discover(root: &Path, git: &OsStr) -> Option<(String, String, bool, Vec<PathBuf>)> {
    let top = run(root, git, &["rev-parse", "--show-toplevel"])?;
    if fs::canonicalize(top).ok()? != fs::canonicalize(root).ok()? {
        return None;
    }
    let commit = run(root, git, &["rev-parse", "--verify", "HEAD^{commit}"])?;
    if ![40, 64].contains(&commit.len()) || !commit.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let date = run(
        root,
        git,
        &["show", "-s", "--no-show-signature", "--format=%cs", "HEAD"],
    )?;
    if !valid_date(&date) {
        return None;
    }
    let mut status_args = vec!["status", "--porcelain", "--untracked-files=all", "--"];
    status_args.extend(INPUTS);
    let dirty = !run(root, git, &status_args)?.is_empty();
    Some((commit, date, dirty, git_watches(root, git)?))
}

fn git_watches(root: &Path, git: &OsStr) -> Option<Vec<PathBuf>> {
    let git_path =
        |name: &str| run(root, git, &["rev-parse", "--git-path", name]).map(|path| root.join(path));
    let head = git_path("HEAD")?;
    let mut watches = vec![head.clone(), git_path("index")?, git_path("packed-refs")?];
    let contents = fs::read_to_string(&head).ok()?;
    if let Some(reference) = contents
        .trim_end_matches(['\r', '\n'])
        .strip_prefix("ref: ")
    {
        let active = git_path(reference)?;
        // A packed ref may later become loose. Watch its nearest existing refs
        // directory, never the whole Git directory (index/log writes are noisy).
        let refs = git_path("refs")?;
        let mut parent = active.parent()?;
        while !parent.exists() {
            parent = parent.parent()?;
        }
        if parent.starts_with(refs) {
            watches.push(parent.to_owned());
        }
        watches.push(active);
    }
    Some(watches)
}
