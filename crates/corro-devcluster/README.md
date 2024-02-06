# corro-devcluster

A tool to quickly start a corrosion cluster for development.

- Build corrosion with nix (keep in mind that changes to the
  `Cargo.lock` file need to be checked into git to be visible to a nix
  build)
- Provide a topology config (`.txt` is fine, see
  [example_topologies](example_topologies/))
- Provide a state directory.  Node state and log output will be kept
  in separated directories.
