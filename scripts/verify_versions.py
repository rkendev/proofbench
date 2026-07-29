#!/usr/bin/env python3
"""Version-consistency check across all dependency-declaring files (INV-5).

A tool pin declared in one file and disagreeing in another fails the build. The
two files that declare tool versions are requirements-dev.txt (the source of
truth) and .pre-commit-config.yaml (the `rev:` of each hook repo). This script
extracts the ruff and mypy versions from both and exits non-zero, naming the
tool, if they disagree.

Self-contained: parses the pre-commit YAML with a small regex scan so it needs
no external dependency and can run under a bare interpreter.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# Tools whose versions appear in more than one declaring file.
TOOLS = ("ruff", "mypy")

# Map a substring of a pre-commit repo URL to the tool it pins.
_REPO_TO_TOOL = {
    "ruff-pre-commit": "ruff",
    "mirrors-mypy": "mypy",
}

_REQ_RE = re.compile(r"^([A-Za-z0-9_.-]+)==([^\s#]+)")
_REPO_RE = re.compile(r"repo:\s*(\S+)")
_REV_RE = re.compile(r"""rev:\s*["']?v?([0-9][^\s"']*)""")


def parse_requirements(path: Path) -> dict[str, str]:
    """Return {tool: version} for the pinned tools found in a requirements file."""
    pins: dict[str, str] = {}
    if not path.exists():
        return pins
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = _REQ_RE.match(stripped)
        if match and match.group(1).lower() in TOOLS:
            pins[match.group(1).lower()] = match.group(2)
    return pins


def parse_precommit(path: Path) -> dict[str, str]:
    """Return {tool: version} extracted from a .pre-commit-config.yaml file."""
    pins: dict[str, str] = {}
    if not path.exists():
        return pins
    current_tool: str | None = None
    for line in path.read_text(encoding="utf-8").splitlines():
        repo_match = _REPO_RE.search(line)
        if repo_match:
            current_tool = None
            for needle, tool in _REPO_TO_TOOL.items():
                if needle in repo_match.group(1):
                    current_tool = tool
                    break
            continue
        rev_match = _REV_RE.search(line)
        if rev_match and current_tool is not None:
            pins[current_tool] = rev_match.group(1)
            current_tool = None
    return pins


def find_conflicts(root: Path) -> list[str]:
    """Return a human-readable list of tools whose pins disagree across files."""
    req_file = root / "requirements-dev.txt"
    pc_file = root / ".pre-commit-config.yaml"
    req = parse_requirements(req_file)
    pre = parse_precommit(pc_file)

    conflicts: list[str] = []
    for tool in TOOLS:
        declared = {
            req_file.name: req.get(tool),
            pc_file.name: pre.get(tool),
        }
        present = {name: ver for name, ver in declared.items() if ver is not None}
        if len(present) > 1 and len(set(present.values())) > 1:
            detail = ", ".join(f"{name}={ver}" for name, ver in sorted(present.items()))
            conflicts.append(f"{tool}: {detail}")
    return conflicts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parent.parent,
        help="Repository root containing the declaring files.",
    )
    args = parser.parse_args(argv)

    conflicts = find_conflicts(args.root)
    if conflicts:
        print("Dependency version mismatch (INV-5):", file=sys.stderr)
        for conflict in conflicts:
            print(f"  {conflict}", file=sys.stderr)
        return 1
    print("Dependency versions consistent across declaring files (INV-5).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
