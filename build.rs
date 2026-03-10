fn main() {
    if cfg!(target_os = "windows") {
        // Bundled DuckDB uses Windows Restart Manager APIs and fails to link
        // on MSVC unless we explicitly link the system Rstrtmgr library.
        println!("cargo:rustc-link-lib=dylib=Rstrtmgr");
    }
}
