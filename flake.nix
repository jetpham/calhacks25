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
            # Performance optimization packages
            jemalloc
            perf-tools
            valgrind
            # Additional development tools
            pkg-config
            openssl
            openssl.dev
          ];
          shellHook = ''
            export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:${gcc.cc.lib}/lib"
            # Performance optimization flags (removed problematic flags for proc-macros)
            export RUSTFLAGS="-C target-cpu=native"
            # Use jemalloc
            export JEMALLOC_SYS_WITH_MALLOC_CONF="background_thread:true,dirty_decay_ms:0,muzzy_decay_ms:0"
            # Enable jemalloc profiling if needed
            export MALLOC_CONF="background_thread:true,dirty_decay_ms:0,muzzy_decay_ms:0"
          '';
        };
      }
    );
}
