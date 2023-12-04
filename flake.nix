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
        rust-latest = fenix.packages."${system}".latest;
      in
        {
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

              ## Build environment dependencies
              nativeBuildInputs = [
                pkgs.pkg-config
                pkgs.mold
                rust-latest.toolchain
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
