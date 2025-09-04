{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    foundry.url = "github:shazow/foundry.nix/stable";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };
  outputs =
    { nixpkgs, fenix, foundry, ... }:
    let
      overlays = [
        fenix.overlays.default
        foundry.overlay
      ];

      forAllSystems = f: nixpkgs.lib.genAttrs nixpkgs.lib.systems.flakeExposed (system: f {
        # NOTE: Fenix doesn't support rustup's +nightly syntax (e.g., `cargo +nightly fmt`)
        # This is a rustup-specific feature that requires rustup's toolchain management.
        # See: https://github.com/nix-community/fenix/issues/110
        toolchain = with fenix.packages.${system}; combine [
          (fromToolchainFile {
            file = ./rust-toolchain.toml;
            sha256 = "sha256-+9FmLhAOezBZCOziO0Qct1NOrfpjNsXxc/8I0c7BdKE=";
          })
          stable.rust-src # This is needed for rust-analyzer to find stdlib symbols. Should use the same channel as the toolchain.
        ];

        pkgs = import nixpkgs { inherit overlays system; };
      });
    in
    {
      formatter = forAllSystems ({ pkgs }: pkgs.alejandra);
      devShells = forAllSystems ({ pkgs, toolchain }: {
        default = pkgs.mkShell {
          shellHook = ''
            # This is not ideal, but it's needed to support our `just` commands.
            rustup install nightly --profile minimal --component rustfmt
          '';

          packages = with pkgs; [
            rustup
            toolchain
            foundry-bin
            solc
            bun
            protobuf
            uv
            cmake
            corepack
            nodejs
            postgresql
            just
            cargo-nextest
          ];
        };
      });
    };
}
