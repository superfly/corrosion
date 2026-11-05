fn main() {
    build_info_build::build_script();

    let pkg_version = env!("CARGO_PKG_VERSION");
    let sha = git(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".to_string());
    let short_sha = git(&["rev-parse", "--short=8", "HEAD"]);
    let dirty = std::process::Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .map(|o| o.status.success() && !o.stdout.is_empty())
        .unwrap_or(false);

    println!("cargo:rustc-env=CORRO_GIT_SHA={sha}");
    let version = match short_sha {
        Some(sha) => format!("{pkg_version}+{sha}{}", if dirty { ".dirty" } else { "" }),
        None => pkg_version.to_string(),
    };
    println!("cargo:rustc-env=CORRO_VERSION={version}");
}

fn git(args: &[&str]) -> Option<String> {
    std::process::Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
}
