{
  description = "Calhacks 2025";

  inputs = {
    nixpkgs.url      = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url  = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };
      in
      {
        devShells.default = with pkgs; mkShell.override {
          stdenv = stdenvAdapters.useMoldLinker clangStdenv;
        } {
          buildInputs = [
            (rust-bin.selectLatestNightlyWith (toolchain: toolchain.default.override {
              extensions = [ "rust-src" ];
            }))
            mold
            clang
            python312
            uv
            gcc
            gcc.cc.lib
            zlib
            # DuckDB for database operations
            duckdb
            # Performance optimization packages
            jemalloc
            perf-tools
            valgrind
            # Profiling tools
            cargo-flamegraph
            # Additional development tools
            pkg-config
            openssl
            openssl.dev
          ];
          
          shellHook = ''
            # Export library paths for dynamically linked executables
            export LD_LIBRARY_PATH=${gcc.cc.lib}/lib:${zlib}/lib:$LD_LIBRARY_PATH
          '';
        };
      }
    );
}
