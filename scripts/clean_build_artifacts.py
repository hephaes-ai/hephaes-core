#!/usr/bin/env python3
"""Remove generated Python build and cache artifacts from the repository."""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent

DIRECTORY_NAMES = {
    "__pycache__",
    ".eggs",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "build",
    "dist",
    "htmlcov",
}

FILE_SUFFIXES = {".pyc", ".pyo"}
FILE_NAMES = {".coverage"}
SKIP_DIRECTORIES = {".git", "venv", ".venv"}


def iter_artifacts(root: Path) -> list[Path]:
    artifacts: set[Path] = set()

    for current_root, dir_names, file_names in os.walk(root, topdown=True):
        current_path = Path(current_root)
        kept_dir_names: list[str] = []
        for directory in sorted(dir_names):
            if directory in SKIP_DIRECTORIES:
                continue

            directory_path = current_path / directory
            if directory in DIRECTORY_NAMES or directory.endswith(".egg-info"):
                artifacts.add(directory_path)
                continue

            kept_dir_names.append(directory)

        dir_names[:] = kept_dir_names

        for file_name in file_names:
            file_path = current_path / file_name
            if file_name in FILE_NAMES or file_path.suffix in FILE_SUFFIXES:
                artifacts.add(file_path)

    demo_output = root / "demo" / "output"
    if demo_output.exists():
        artifacts.add(demo_output)

    return sorted(artifacts, key=lambda path: (len(path.parts), str(path)), reverse=True)


def remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be removed without deleting anything.",
    )
    args = parser.parse_args()

    artifacts = iter_artifacts(REPO_ROOT)

    if not artifacts:
        print("No build artifacts found.")
        return 0

    action = "Would remove" if args.dry_run else "Removing"
    for artifact in artifacts:
        print(f"{action}: {artifact.relative_to(REPO_ROOT)}")
        if not args.dry_run:
            remove_path(artifact)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
