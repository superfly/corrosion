# Setup development environment

This section contains some information that might be relevant for you when setting up a `corrosion` development environment.  Depending on your computer setup your experience will vary.

We try to document any quirks in development here.  If you find yourself having to adjust something in order to be able to work on `corrosion`, please let us know/ open a PR!  Thank you!


## Dependencies with Nix

The corrosion repository comes with a `flake.nix` file, which can build the main `corrosion` binary, load development dependencies, and serve a local mdbook server for the book (that you're reading right now!).

If you're not familiar with Nix you can learn more about it here: [https://nixos.org](https://nixos.org/explore).

You will have to enable the "experimental-features" flag for Nix.  You can do so by creating a configuration at `~/.config/nix/nix.conf`:

```conf
extra-experimental-features = nix-command flakes
```

Afterwards you can: 

- run `nix build` to build the `corrosion` command and all dependent crates.  The final binary can then be found under `./result/`
- run `nix develop` to get a development shell for `corrosion`.  Note: this will use a shell hook to update the limit of open file descriptors via `ulimit`.  Make sure that your system is configured to allow this without requiring privileges!
- run `nix develop .#mdbook-shell` to start the book development server
