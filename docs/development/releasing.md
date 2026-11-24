# Releasing

Releases are built **on a maintainer's machine** and published as files of a GitHub release. GitHub doesn't build anything; it only hosts the files.

## What a release contains

| File | For |
| --- | --- |
| `syntasks-<v>-py3-none-any.whl` | `pip install` |
| `syntasks-<v>.tar.gz` | the source distribution |
| `SHA256SUMS` | checksums of both |

## Cut a release

1. Make sure `main` is clean and green (lint and tests; see [Testing](testing.md)).
2. Set `__version__` in `src/syntasks/__init__.py`.
3. Add a `## [X.Y.Z]` section to `CHANGELOG.md`, and commit both.
4. Run the script:

   ```bash
   scripts/release.sh 0.2.0 --dry-run    # optional: checks, tests and build only; look at dist/
   scripts/release.sh 0.2.0
   ```

The script checks the tree, the tag, the version and the changelog. It then runs ruff, mypy and pytest, builds `dist/` with `uv build`, validates it with `twine check`, and installs the wheel alone into an empty environment to check that it pulls in no other package, imports as the right version and runs `syntasks`. Finally it tags `v0.2.0`, pushes `main` and the tag, and creates the GitHub release, with the changelog section plus install instructions as notes.

- A version with a suffix, such as `0.3.0-rc.1`, becomes a **prerelease**.
- `--draft` publishes a draft to check before announcing; `--skip-tests` skips lint, types and tests (the smoke test below still runs).

You need `uv`, and `gh` signed in with permission to create releases.

## Versions

SynTasks follows [semantic versioning](https://semver.org). Until 1.0, minor versions may break things, and the changelog says how to upgrade.
