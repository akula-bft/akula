{
  description = "Akula development shell";

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
      with pkgs;
      {
        packages.default = mkShell {
          buildInputs = [
            (rust-bin.fromRustupToolchainFile ./rust-toolchain)
            pkgconfig
            clang
            libclang.lib
            e2fsprogs
            protobuf
          ];

          shellHook = ''
            export LIBCLANG_PATH="${pkgs.libclang.lib}/lib";
          '';
        };
      }
    );
}
