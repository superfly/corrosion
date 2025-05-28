{
  description = "corrosion2-dev";

  ## Specify the flake environment inputs
  inputs = {
    ## The fenix flake gives us access to nightly rust toolchains
    fenix = {
      url = "github:nix-community/fenix?rev=1c050d9008ff9e934f8bb5298c902259ea2cb3f7";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    nixpkgs.url = "nixpkgs/nixos-25.05";
    flake-utils.url = "github:numtide/flake-utils?rev=11707dc2f618dd54ca8739b309ec4fc024de578b";
  };

  ## Specify flake environment outputs
  ##
  ## The flake-utils harness is used to make supporting different
  ## architectures (x86_64-linux, aarch64-darwin, etc) easier.
  outputs =
    {
      flake-utils,
      nixpkgs,
      fenix,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
        # TODO: Sort out duplicate dependencies (rusqlite => hashbrown => ahash).
        # ahash 0.7.6 requires stdsimd feature which has been removed in latest nightly
        # We specify this specific nightly according to this [forum post](https://users.rust-lang.org/t/error-e0635-unknown-feature-stdsimd/106445/4)
        toolchain = fenix.packages."${system}".fromToolchainFile {
          file = ./rust-toolchain.toml;
          sha256 = "sha256-Lfw8OhV/94shHfMyaPYce3l62VJw47pW8Aaiv1bkgxs=";
        };

        rust = pkgs.makeRustPlatform {
          cargo = toolchain;
          rustc = toolchain;
        };

      in
      {
        packages.mdbook-shell = pkgs.mkShell {
          buildInputs = with pkgs; [
            mdbook
            mdbook-linkcheck
            mdbook-admonish
          ];

          shellHook = ''
            mdbook serve
          '';
        };

        # Run using `nix develop`. This devShell sets up the same dependencies and toolchain used in package.default
        devShells.default = pkgs.mkShell {
          name = "test-shell";
          nativeBuildInputs = with pkgs; [
            clang
            mold
            toolchain
            pkg-config
          ];

          RUST_BACKTRACE = 1;
        };
        ## Here we declare the only flake output to be a nix build
        ## of the corrosion crate tree
        packages.default = rust.buildRustPackage {
          name = "corrosion2";
          src = ./.;

          cargoLock = {
            lockFile = ./Cargo.lock;

            # Needed for vendored dependencies
            allowBuiltinFetchGit = true;
          };

          # Include a shell hook to run when we use `nix develop`
          #
          # NOTE: this depends on /etc/security/limits.conf
          # setting an appropriate soft or hard-limit.  Without it
          # users can't override their personal limits.
          shellHook = ''
            ulimit -n 65536
          '';

          # Useful when doing development builds
          # TODO: Create packages.development which unsets this. packages.default should default into doing tests.
          doCheck = false;

          ## Build environment dependencies
          nativeBuildInputs = [
            pkgs.pkg-config
            pkgs.mold
            pkgs.clang
          ];

          ## Dependencies for the linked binary
          buildInputs = with pkgs; [
            openssl
            sqlite
            libgit2
          ];
        };
      }
    );
}
