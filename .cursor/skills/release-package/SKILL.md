---
name: release-package
description: Automates the process of releasing a new version of the pyspark-udtf package. Use when the user wants to release a new version, bump the version, or publish to PyPI.
---

# Release Package

This skill guides the agent through the process of releasing a new version of the `pyspark-udtf` package.

## Prerequisites

- `uv` installed (version >= 0.7.0 recommended).
- Git repository initialized and clean.
- PyPI credentials configured (or `UV_PUBLISH_TOKEN` set).

## Workflow

Follow these steps sequentially to release a new version:

1.  **Verify Environment & Clean State**
    - Check if `uv` is installed.
    - Ensure there are no uncommitted changes.
    ```bash
    uv --version
    if [ -n "$(git status --porcelain)" ]; then
      echo "Working directory is not clean. Please commit or stash changes first."
      exit 1
    fi
    ```

2.  **Rebase on Master**
    Ensure the current branch is up to date with `origin/master`.
    ```bash
    git fetch origin
    git rebase origin/master
    ```
    - *Action*: If conflicts occur, abort the rebase (`git rebase --abort`) and notify the user. Do NOT resolve conflicts automatically.

3.  **Bump Version**
    Update the version in `pyproject.toml`.
    - Check the current version first.
    - Increment the version (patch, minor, or major as requested).
    - **Preferred**: Use `uv version --bump <type>` (requires uv >= 0.7.0).
    - **Fallback**: Edit `pyproject.toml` manually if using older uv versions.

4.  **Build Package**
    Build the distribution artifacts using `uv`.
    ```bash
    uv build
    ```
    - *Verification*: Check that `dist/` contains the new version artifacts (`.tar.gz` and `.whl`).

5.  **Commit Changes**
    Commit the version bump.
    ```bash
    git add pyproject.toml
    git commit -m "Bump version to X.Y.Z"
    ```

6.  **Publish to PyPI**
    Publish the package using `uv`.
    ```bash
    uv publish
    ```
    - *Note*: Ensure PyPI credentials are configured or available in the environment.

7.  **Push Changes**
    Push the commit to the remote repository.
    ```bash
    git push --force-with-lease  # If rebased
    # OR
    git push
    ```

## Verification

- Check the `dist/` folder to ensure only the intended versions are present.
- Verify the new version is available on PyPI after publishing.
