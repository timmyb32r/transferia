fn main() -> Result<(), Box<dyn core::error::Error>> {
    use std::fs;
    use std::path::PathBuf;
    use std::process::Command;

    let workspace = PathBuf::from("../..");
    let web = workspace.join("web");
    println!("cargo:rerun-if-changed=../../web/src");
    println!("cargo:rerun-if-changed=../../web/scripts/build.mjs");
    println!("cargo:rerun-if-changed=../../web/scripts/generate-api.mjs");
    println!(
        "cargo:rerun-if-changed=../transferia-server-contracts/contracts/server-api.schema.json"
    );
    println!("cargo:rerun-if-changed=../../web/package.json");
    println!("cargo:rerun-if-changed=../../web/package-lock.json");
    println!("cargo:rerun-if-env-changed=TRANSFERIA_SKIP_SERVER_UI");
    if std::env::var_os("TRANSFERIA_SKIP_SERVER_UI").is_some() {
        let output = PathBuf::from(std::env::var_os("OUT_DIR").ok_or("OUT_DIR is missing")?)
            .join("server-ui");
        fs::create_dir_all(&output)?;
        fs::write(output.join("index.html"), "")?;
        fs::write(output.join("app.js"), "")?;
        fs::write(output.join("style.css"), "")?;
        return Ok(());
    }

    let package_lock = fs::read(web.join("package-lock.json"))?;
    let dependency_stamp = web.join("node_modules/.transferia-package-lock.json");
    if fs::read(&dependency_stamp).ok().as_deref() != Some(package_lock.as_slice()) {
        let status = Command::new("npm")
            .args(["ci", "--prefer-offline", "--no-audit", "--no-fund"])
            .current_dir(&web)
            .status()?;
        if !status.success() {
            return Err(format!("server UI dependency installation failed: {status}").into());
        }
        fs::write(dependency_stamp, package_lock)?;
    }
    let output =
        PathBuf::from(std::env::var_os("OUT_DIR").ok_or("OUT_DIR is missing")?).join("server-ui");
    let status = Command::new("npm")
        .args(["run", "build"])
        .current_dir(&web)
        .env("SERVER_UI_OUT_DIR", &output)
        .env("PROFILE", std::env::var("PROFILE").unwrap_or_default())
        .status()?;
    if !status.success() {
        return Err(format!("server UI build failed with status {status}").into());
    }
    Ok(())
}
