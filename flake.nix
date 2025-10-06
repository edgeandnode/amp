{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    foundry = {
      url = "github:shazow/foundry.nix/stable";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane = {
      url = "github:ipetkov/crane";
    };
  };
  outputs = {
    nixpkgs,
    fenix,
    foundry,
    crane,
    ...
  }: let
    overlays = [
      fenix.overlays.default
      foundry.overlay
    ];

    forAllSystems = f:
      nixpkgs.lib.genAttrs nixpkgs.lib.systems.flakeExposed (
        system: let
          pkgs = import nixpkgs {inherit overlays system;};

          # NOTE: Fenix doesn't support rustup's +nightly syntax (e.g., `cargo +nightly fmt`)
          # This is a rustup-specific feature that requires rustup's toolchain management.
          # See: https://github.com/nix-community/fenix/issues/110
          toolchain = with fenix.packages.${system};
            combine [
              (fromToolchainFile {
                file = ./rust-toolchain.toml;
                sha256 = "sha256-SJwZ8g0zF2WrKDVmHrVG3pD2RGoQeo24MEXnNx5FyuI=";
              })
              stable.rust-src # This is needed for rust-analyzer to find stdlib symbols. Should use the same channel as the toolchain.
            ];

          craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;

          # Extract version from workspace Cargo.toml
          workspaceCargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);
          nozzleVersion = workspaceCargoToml.workspace.package.version;

          # Build dependencies
          nativeBuildInputs = with pkgs; [
            cmake
            protobuf
          ];

          buildInputs = with pkgs; [
            postgresql
          ];

          # Pre-fetch the rusty_v8 library (required by js-runtime crate)
          rustyV8Lib = pkgs.fetchurl {
            url = "https://github.com/denoland/rusty_v8/releases/download/v135.1.1/librusty_v8_release_${
              if pkgs.stdenv.isDarwin
              then "aarch64-apple-darwin"
              else if pkgs.stdenv.isAarch64
              then "aarch64-unknown-linux-gnu"
              else "x86_64-unknown-linux-gnu"
            }.a.gz";
            hash =
              if pkgs.stdenv.isDarwin
              then "sha256-L/JaeFjuEe0lCs7jONx6mp2MRbvPou2QsgDUwfk8HgQ="
              else if pkgs.stdenv.isAarch64
              then "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
              else "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
          };

          # Build the nozzle package
          nozzle = craneLib.buildPackage {
            pname = "nozzle";
            version = nozzleVersion;
            src = craneLib.cleanCargoSource ./.;
            strictDeps = true;

            inherit nativeBuildInputs buildInputs;

            # Point v8 build to pre-fetched library
            RUSTY_V8_ARCHIVE = rustyV8Lib;

            # Build only the nozzle binary
            cargoExtraArgs = "-p nozzle";
          };
        in
          f {inherit pkgs toolchain nozzle;}
      );
  in {
    formatter = forAllSystems ({pkgs, ...}: pkgs.alejandra);

    packages = forAllSystems ({nozzle, ...}: {
      inherit nozzle;
      default = nozzle;
    });

    apps = forAllSystems ({nozzle, ...}: {
      nozzle = {
        type = "app";
        program = "${nozzle}/bin/nozzle";
      };
      default = {
        type = "app";
        program = "${nozzle}/bin/nozzle";
      };
    });

    devShells = forAllSystems ({
      pkgs,
      toolchain,
      ...
    }: {
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
