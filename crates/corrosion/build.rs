fn main() {
    build_info_build::build_script();

    println!("cargo:rerun-if-env-changed=CORRO_GIT_SHA");
    println!("cargo:rerun-if-env-changed=CORRO_GIT_SHORT_SHA");
    println!("cargo:rerun-if-env-changed=CORRO_GIT_DIRTY");

    let pkg_version = env!("CARGO_PKG_VERSION");
    let sha = env_var("CORRO_GIT_SHA")
        .or_else(|| git(&["rev-parse", "HEAD"]))
        .unwrap_or_else(|| "unknown".to_string());
    let short_sha = env_var("CORRO_GIT_SHORT_SHA")
        .or_else(|| git(&["rev-parse", "--short=8", "HEAD"]))
        .or_else(|| sha.get(..8).map(str::to_string));
    let dirty = env_var("CORRO_GIT_DIRTY")
        .and_then(|value| match value.as_str() {
            "1" | "true" | "yes" => Some(true),
            "0" | "false" | "no" => Some(false),
            _ => None,
        })
        .unwrap_or_else(|| {
            std::process::Command::new("git")
                .args(["status", "--porcelain"])
                .output()
                .map(|o| o.status.success() && !o.stdout.is_empty())
                .unwrap_or(false)
        });

    println!("cargo:rustc-env=CORRO_GIT_SHA={sha}");
    let version = match short_sha {
        Some(sha) => format!("{pkg_version}+{sha}{}", if dirty { ".dirty" } else { "" }),
        None => pkg_version.to_string(),
    };
    println!("cargo:rustc-env=CORRO_VERSION={version}");
}

fn env_var(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
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
