fn main() -> Result<(), Box<dyn core::error::Error>> {
    build_server_ui()?;

    tonic_prost_build::configure()
        // Runtime uses raw tonic paths so generated client/server wrappers only
        // increase compile time and expose an API surface the binary never uses.
        .build_client(false)
        .build_server(false)
        .compile_protos(
            &[
                "proto/ydb/ydb_persqueue_v1.proto",
                "proto/ydb/ydb_discovery.proto",
            ],
            &["proto/"],
        )?;
    Ok(())
}

fn build_server_ui() -> Result<(), Box<dyn core::error::Error>> {
    use std::path::PathBuf;
    use std::process::Command;

    println!("cargo:rerun-if-changed=web/src");
    println!("cargo:rerun-if-changed=web/scripts/build.mjs");
    println!("cargo:rerun-if-changed=web/scripts/generate-api.mjs");
    println!("cargo:rerun-if-changed=contracts/server-api.schema.json");
    println!("cargo:rerun-if-changed=web/package.json");
    println!("cargo:rerun-if-changed=web/package-lock.json");
    println!("cargo:rerun-if-env-changed=TRANSFERIA_SKIP_SERVER_UI");

    // Updating the Rust-owned API schema must not depend on a frontend build
    // that still consumes the previously committed schema artifact.
    if std::env::var_os("TRANSFERIA_SKIP_SERVER_UI").is_some() {
        return Ok(());
    }

    if !PathBuf::from("web/node_modules").is_dir() {
        return Err("web dependencies are missing; run `npm ci --prefix web`".into());
    }
    let output =
        PathBuf::from(std::env::var_os("OUT_DIR").ok_or("OUT_DIR is missing")?).join("server-ui");
    let status = Command::new("npm")
        .args(["run", "build"])
        .current_dir("web")
        .env("SERVER_UI_OUT_DIR", &output)
        .env("PROFILE", std::env::var("PROFILE").unwrap_or_default())
        .status()?;
    if !status.success() {
        return Err(format!("server UI build failed with status {status}").into());
    }
    Ok(())
}
