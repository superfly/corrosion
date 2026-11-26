#!/usr/bin/env python3
import json
import subprocess

metadata = json.loads(
    subprocess.check_output(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        text=True,
    )
)

cmd = ["cargo", "clean", "--release"]
for package in metadata["packages"]:
    cmd.extend(["--package", package["name"]])

subprocess.run(cmd, check=True)
