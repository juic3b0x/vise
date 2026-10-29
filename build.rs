mod build_support;

fn main() {
    let root = std::env::var_os("CARGO_MANIFEST_DIR").map(std::path::PathBuf::from);
    for name in ["VISE_GIT_COMMIT", "VISE_GIT_DATE", "VISE_GIT_DIRTY", "PATH"] {
        println!("cargo:rerun-if-env-changed={name}");
    }
    let variable =
        |name| std::env::var_os(name).map(|value| value.into_string().unwrap_or_default());
    let overrides = build_support::Overrides {
        commit: variable("VISE_GIT_COMMIT"),
        date: variable("VISE_GIT_DATE"),
        dirty: variable("VISE_GIT_DIRTY"),
    };
    let package = std::env::var("CARGO_PKG_VERSION").unwrap_or_default();
    let (version, mut watches) = root.as_ref().map_or_else(
        || (package.clone(), Vec::new()),
        |root| build_support::resolve(&package, root, std::ffi::OsStr::new("git"), &overrides),
    );
    if let Some(root) = root {
        watches.extend(build_support::INPUTS.iter().map(|path| root.join(path)));
        if root.join(".git").is_file() {
            watches.push(root.join(".git"));
        }
    }
    // Missing files cause perpetual Cargo reruns; directories here never include target/.
    for path in watches.into_iter().filter(|path| path.exists()) {
        if let Some(path) = path.to_str().filter(|path| !path.contains(['\r', '\n'])) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    println!("cargo:rustc-env=VISE_BUILD_VERSION={version}");
}
