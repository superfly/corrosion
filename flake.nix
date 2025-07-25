{
  description = "corrosion2-dev";

  ## Specify the flake environment inputs
  inputs = {
    ## The fenix flake gives us access to nightly rust toolchains
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    nixpkgs.url = "nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  ## Specify flake environment outputs
  ##
  ## The flake-utils harness is used to make supporting different
  ## architectures (x86_64-linux, aarch64-darwin, etc) easier.
  outputs = { flake-utils, nixpkgs, fenix, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        rust-toolchain = fenix.packages."${system}".fromToolchainFile {
          file = ./rust-toolchain.toml;
          sha256 = "sha256-3aoA7PuH09g8F+60uTUQhnHrb/ARDLueSOD08ZVsWe0=";
        };
      in
        {
          packages.mdbook-shell = pkgs.mkShell {
            buildInputs = with pkgs; [ mdbook mdbook-linkcheck mdbook-admonish ];

            shellHook = ''
              mdbook serve
            '';
          };
          
          ## Here we declare the only flake output to be a nix build
          ## of the corrosion crate tree
          packages.default =
            pkgs.rustPlatform.buildRustPackage {
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
              doCheck = false;
              buildType = "debug";
              
              ## Build environment dependencies
              nativeBuildInputs = [
                pkgs.pkg-config
                pkgs.mold
                pkgs.clang
                rust-toolchain
              ];

              ## Dependencies for the linked binary
              buildInputs = with pkgs; [
                openssl
                sqlite
                libgit2
              ];
            };
        });
}
