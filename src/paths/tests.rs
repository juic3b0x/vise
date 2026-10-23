use super::*;
use crate::model::{Chunk, ChunkStatus, Status, timestamp_millis};
use std::io::ErrorKind;
use uuid::Uuid;

fn staged(root: &Path) -> Record {
    fs::create_dir(root.join(".vise")).unwrap();
    let id = Uuid::new_v4();
    Record {
        id,
        url: "https://example.com/file".into(),
        filename: "payload.bin".into(),
        output_path: root.join("payload.bin"),
        part_path: root.join(".vise").join(format!("{id}.part")),
        proxy_url: None,
        cookies_file: None,
        status: Status::Paused,
        total_bytes: Some(3),
        ranged: true,
        validator: None,
        chunks: vec![Chunk {
            start: 0,
            end: Some(3),
            written: 3,
            status: ChunkStatus::Paused,
        }],
        error: None,
        checksums: Default::default(),
        created_at: timestamp_millis(),
        speed: 0.0,
    }
}

#[test]
fn directory_checks_existence_type_absolute_path_and_traversal() {
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    assert_eq!(directory(&root).unwrap(), root);
    assert!(
        matches!(directory(&root.join("missing")), Err(Error::Io(error)) if error.kind() == ErrorKind::NotFound)
    );
    fs::write(root.join("file"), b"keep").unwrap();
    assert!(
        matches!(directory(&root.join("file")), Err(Error::Io(error)) if error.kind() == ErrorKind::NotADirectory)
    );
    assert!(directory(Path::new("relative")).is_err());
    assert!(directory(&root.join("..")).is_err());
    let dotted = root.join("valid..name 雪");
    fs::create_dir(&dotted).unwrap();
    assert_eq!(directory(&dotted).unwrap(), dotted);
}

#[test]
fn child_paths_cannot_replace_the_root_or_add_components() {
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    for name in [
        "",
        ".",
        "..",
        "../escape",
        "a/../../escape",
        "a/b",
        "a\\b",
        "/tmp/escape",
        "C:\\escape",
        "\\\\host\\share",
        "nul\0file",
    ] {
        assert!(bounded_file(&root, name).is_err(), "accepted {name:?}");
    }
    for name in ["payload.bin", "valid..name", "雪 file.bin"] {
        let path = bounded_file(&root, name).unwrap();
        assert!(path.starts_with(&root));
        assert_eq!(path.parent(), Some(root.as_path()));
    }
    assert!(bounded_file(Path::new("relative"), "file").is_err());
    assert!(bounded_file(&root.join(".."), "file").is_err());
}

#[test]
fn staging_layout_requires_uuid_names_and_preserves_missing_volumes() {
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    let mut record = staged(&root);
    fs::write(&record.part_path, b"abc").unwrap();
    let files = staging_files(&record).unwrap().unwrap();
    assert_eq!(
        files,
        [
            record.part_path.clone(),
            record.part_path.with_extension("published")
        ]
    );
    let original = record.part_path.clone();
    record.part_path = root.join("outside.part");
    assert!(staging_files(&record).is_err());
    record.part_path = original;
    fs::remove_file(&record.part_path).unwrap();
    fs::create_dir(&record.part_path).unwrap();
    assert!(staging_files(&record).is_err());
    fs::remove_dir(&record.part_path).unwrap();
    fs::remove_dir(root.join(".vise")).unwrap();
    assert!(staging_files(&record).unwrap().is_none());
    assert_eq!(record.downloaded(), 3);
}

#[cfg(unix)]
#[test]
fn canonical_checks_reject_symlinked_roots_and_files() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    let record = staged(&root);
    let outside = root.join("outside");
    fs::create_dir(&outside).unwrap();
    fs::write(outside.join("keep"), b"keep").unwrap();
    symlink(&outside, root.join("alias")).unwrap();
    assert!(directory(&root.join("alias")).is_err());
    symlink(outside.join("keep"), &record.part_path).unwrap();
    assert!(staging_files(&record).is_err());
    fs::remove_file(&record.part_path).unwrap();
    symlink(outside.join("missing"), &record.part_path).unwrap();
    assert!(staging_files(&record).is_err());
    fs::remove_file(&record.part_path).unwrap();
    fs::remove_dir(root.join(".vise")).unwrap();
    symlink(&outside, root.join(".vise")).unwrap();
    assert!(staging_files(&record).is_err());
    assert_eq!(fs::read(outside.join("keep")).unwrap(), b"keep");
}
