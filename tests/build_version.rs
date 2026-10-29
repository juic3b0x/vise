#[path = "../build_support.rs"]
mod build_support;

use build_support::{Overrides, resolve};
use std::{ffi::OsStr, fs, path::Path, process::Command};

fn git(root: &Path, args: &[&str]) -> bool {
    Command::new("git")
        .current_dir(root)
        .env("HOME", root)
        .env("USERPROFILE", root)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_AUTHOR_DATE", "2020-01-02T03:04:05+00:00")
        .env("GIT_COMMITTER_DATE", "2021-06-07T23:04:05-05:00")
        .args([
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
        ])
        .args(args)
        .output()
        .is_ok_and(|output| output.status.success())
}

fn repository() -> Option<tempfile::TempDir> {
    let root = tempfile::tempdir().unwrap();
    if !git(root.path(), &["--version"]) {
        eprintln!("SKIP: git is unavailable; temporary-repository build-version tests skipped");
        return None;
    }
    assert!(git(root.path(), &["init"]));
    fs::create_dir(root.path().join("src")).unwrap();
    fs::write(root.path().join("src/main.rs"), "fn main() {}\n").unwrap();
    fs::write(root.path().join("Cargo.toml"), "test manifest\n").unwrap();
    fs::write(root.path().join(".gitignore"), "src/ignored\n").unwrap();
    assert!(git(root.path(), &["add", "."]));
    assert!(git(
        root.path(),
        &["-c", "commit.gpgsign=false", "commit", "-m", "initial"]
    ));
    Some(root)
}

fn version(root: &Path) -> String {
    resolve("0.7.0", root, OsStr::new("git"), &Overrides::default()).0
}

#[test]
fn formats_clean_dirty_missing_commit_and_missing_date() {
    use build_support::format_version;
    assert_eq!(
        format_version("0.7.0", Some("ABCDEF012345"), Some("2026-01-02"), false),
        "0.7.0 (abcdef0 2026-01-02)"
    );
    assert_eq!(
        format_version("0.7.0", Some("abcdef012345"), Some("2026-01-02"), true),
        "0.7.0 (abcdef0-dirty 2026-01-02)"
    );
    assert_eq!(format_version("0.7.0", None, None, true), "0.7.0");
    assert_eq!(
        format_version("0.7.0", Some("abcdef0"), None, false),
        "0.7.0 (abcdef0)"
    );
    assert_eq!(
        format_version("0.7.0", Some("secret\npath"), None, false),
        "0.7.0"
    );
}

#[test]
fn clean_dirty_untracked_ignored_and_outside_inputs() {
    let Some(root) = repository() else { return };
    let clean = version(root.path());
    assert!(clean.starts_with("0.7.0 (") && !clean.contains("-dirty"));
    fs::write(root.path().join("README.md"), "outside build inputs").unwrap();
    fs::write(root.path().join("src/ignored"), "ignored").unwrap();
    assert_eq!(version(root.path()), clean);
    fs::write(root.path().join("src/new.rs"), "// new input").unwrap();
    assert!(version(root.path()).contains("-dirty"));
    fs::remove_file(root.path().join("src/new.rs")).unwrap();
    fs::write(root.path().join("src/main.rs"), "// modified input\n").unwrap();
    assert!(version(root.path()).contains("-dirty"));
    assert!(git(root.path(), &["add", "src/main.rs"]));
    assert!(version(root.path()).contains("-dirty"));
    assert!(git(root.path(), &["reset", "--hard", "HEAD"]));
    assert_eq!(version(root.path()), clean);
}

#[test]
fn enclosing_repository_is_not_the_source_checkout() {
    let Some(root) = repository() else { return };
    let copied = root.path().join("copied-source");
    fs::create_dir(&copied).unwrap();
    fs::write(copied.join("Cargo.toml"), "copied manifest").unwrap();
    assert_eq!(version(&copied), "0.7.0");
}

#[test]
fn committer_date_and_individual_overrides_are_respected() {
    let Some(root) = repository() else { return };
    let clean = version(root.path());
    assert!(clean.ends_with(" 2021-06-07)"));
    fs::write(root.path().join("src/main.rs"), "// changed\n").unwrap();
    let mut overrides = Overrides {
        dirty: Some("false".into()),
        ..Overrides::default()
    };
    assert_eq!(
        resolve("0.7.0", root.path(), OsStr::new("git"), &overrides).0,
        clean
    );
    overrides.date = Some("2000-02-29".into());
    assert!(
        resolve("0.7.0", root.path(), OsStr::new("git"), &overrides)
            .0
            .ends_with(" 2000-02-29)")
    );
    overrides.dirty = Some("not-a-boolean".into());
    assert_eq!(
        resolve("0.7.0", root.path(), OsStr::new("git"), &overrides).0,
        "0.7.0"
    );
}

#[test]
fn missing_and_failing_git_and_source_archive_fall_back() {
    let root = tempfile::tempdir().unwrap();
    let missing = root.path().join("nonexistent-git");
    assert_eq!(
        resolve(
            "0.7.0",
            root.path(),
            missing.as_os_str(),
            &Overrides::default()
        )
        .0,
        "0.7.0"
    );
    // A directory cannot be executed on any supported platform.
    assert_eq!(
        resolve(
            "0.7.0",
            root.path(),
            root.path().as_os_str(),
            &Overrides::default()
        )
        .0,
        "0.7.0"
    );
    let archive = tempfile::tempdir().unwrap();
    assert_eq!(version(archive.path()), "0.7.0");
    let Some(root) = repository() else { return };
    assert!(git(root.path(), &["config", "core.bare", "true"]));
    assert_eq!(version(root.path()), "0.7.0");
}

#[test]
fn detached_head_and_packed_refs_use_the_same_commit() {
    let Some(root) = repository() else { return };
    let expected = version(root.path());
    assert!(git(root.path(), &["pack-refs", "--all", "--prune"]));
    assert_eq!(version(root.path()), expected);
    assert!(git(root.path(), &["checkout", "--detach"]));
    assert_eq!(version(root.path()), expected);
    // Exercise CRLF metadata without assuming a particular commit or path separator.
    let head = root.path().join(".git").join("HEAD");
    let contents = fs::read_to_string(&head).unwrap();
    fs::write(&head, contents.replace('\n', "\r\n")).unwrap();
    assert_eq!(version(root.path()), expected);
}

#[test]
fn linked_worktree_resolves_shared_and_private_git_metadata() {
    let Some(root) = repository() else { return };
    let parent = tempfile::tempdir().unwrap();
    let worktree = parent.path().join("linked");
    assert!(git(
        root.path(),
        &["worktree", "add", "--detach", worktree.to_str().unwrap()]
    ));
    assert_eq!(version(&worktree), version(root.path()));
    let (_, watches) = resolve("0.7.0", &worktree, OsStr::new("git"), &Overrides::default());
    assert!(
        watches
            .iter()
            .any(|path| path.ends_with("HEAD") && !path.starts_with(&worktree))
    );
    assert!(
        watches
            .iter()
            .any(|path| path.ends_with("index") && !path.starts_with(&worktree))
    );
}

#[test]
fn shallow_checkout_retains_commit_and_committer_date() {
    let Some(root) = repository() else { return };
    let parent = tempfile::tempdir().unwrap();
    let shallow = parent.path().join("shallow");
    assert!(git(
        parent.path(),
        &[
            "clone",
            "--depth=1",
            "--no-local",
            root.path().to_str().unwrap(),
            shallow.to_str().unwrap()
        ]
    ));
    assert_eq!(version(&shallow), version(root.path()));
}

#[cfg(unix)]
#[test]
fn symlinked_checkout_is_canonicalized_on_both_sides() {
    let Some(root) = repository() else { return };
    let parent = tempfile::tempdir().unwrap();
    let link = parent.path().join("link");
    std::os::unix::fs::symlink(root.path(), &link).unwrap();
    assert_eq!(version(&link), version(root.path()));
}

#[test]
fn authoritative_overrides_do_not_need_git() {
    let root = tempfile::tempdir().unwrap();
    let mut overrides = Overrides {
        commit: Some("deadbee".into()),
        date: Some("2026-01-02".into()),
        dirty: None,
    };
    assert_eq!(
        resolve(
            "0.7.0",
            root.path(),
            OsStr::new("nonexistent-git"),
            &overrides
        ),
        ("0.7.0 (deadbee 2026-01-02)".into(), vec![])
    );
    overrides.dirty = Some("true".into());
    assert!(
        resolve(
            "0.7.0",
            root.path(),
            OsStr::new("nonexistent-git"),
            &overrides
        )
        .0
        .contains("deadbee-dirty")
    );
    overrides.commit = Some(String::new());
    assert_eq!(
        resolve("0.7.0", root.path(), OsStr::new("git"), &overrides).0,
        "0.7.0"
    );
    overrides.commit = Some("deadbee".into());
    overrides.date = Some("2026-02-30".into());
    assert_eq!(
        resolve("0.7.0", root.path(), OsStr::new("git"), &overrides).0,
        "0.7.0"
    );
}

#[test]
fn cli_version_aliases_match_and_create_no_state() {
    let home = tempfile::tempdir().unwrap();
    let output = |flag| {
        Command::new(env!("CARGO_BIN_EXE_vise"))
            .env("HOME", home.path())
            .env("USERPROFILE", home.path())
            .env_remove("VISE_API_TOKEN")
            .arg(flag)
            .output()
            .unwrap()
    };
    let long = output("--version");
    let short = output("-V");
    assert!(long.status.success() && short.status.success());
    assert_eq!(long.stdout, short.stdout);
    assert!(
        String::from_utf8(long.stdout)
            .unwrap()
            .starts_with(concat!("vise ", env!("CARGO_PKG_VERSION")))
    );
    assert!(!home.path().join(".vise").exists());
}
