{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    rust.url = "github:oxalica/rust-overlay";
    foundry.url = "github:shazow/foundry.nix/stable";
  };
  outputs = { self, nixpkgs, rust, foundry, ... }:
    let
      overlays = [
        (import rust)
        (self: super: {
          rustToolchain = super.rust-bin.stable.latest.default;
        })
        foundry.overlay
      ];

      allSystems = [
        "x86_64-linux" # 64-bit Intel/AMD Linux
        "aarch64-linux" # 64-bit ARM Linux
        "x86_64-darwin" # 64-bit Intel macOS
        "aarch64-darwin" # 64-bit ARM macOS
      ];

      # Helper to provide system-specific attributes
      forAllSystems = f: nixpkgs.lib.genAttrs allSystems (system: f {
        pkgs = import nixpkgs { inherit overlays system; };
      });
    in
    {
      devShells = forAllSystems ({ pkgs }: {
        default = pkgs.mkShell {
          packages = (with pkgs; [
            # Includes cargo, clippy, cargo-fmt, rustdoc, rustfmt, and other tools.
            rustToolchain
            foundry-bin
            solc
            bun
            protobuf
            uv
            cmake
            nodejs-slim
            postgresql
          ]) ++ pkgs.lib.optionals pkgs.stdenv.isDarwin (with pkgs; [
            libiconv
          ]);
        };
      });
    };
}
