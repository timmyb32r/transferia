//! Embedded assets for the local control-plane UI.
//!
//! Keeping asset generation in this leaf crate prevents ordinary control-plane
//! edits from invalidating the TypeScript build.

pub const INDEX_HTML: &str = include_str!(concat!(env!("OUT_DIR"), "/server-ui/index.html"));
pub const APP_JS: &str = include_str!(concat!(env!("OUT_DIR"), "/server-ui/app.js"));
pub const STYLE_CSS: &str = include_str!(concat!(env!("OUT_DIR"), "/server-ui/style.css"));
pub const APP_JS_GZIP: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/server-ui/app.js.gz"));
pub const STYLE_CSS_GZIP: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/server-ui/style.css.gz"));
pub const APP_JS_VERSION: &str =
    include_str!(concat!(env!("OUT_DIR"), "/server-ui/app.js.version"));
pub const STYLE_CSS_VERSION: &str =
    include_str!(concat!(env!("OUT_DIR"), "/server-ui/style.css.version"));
