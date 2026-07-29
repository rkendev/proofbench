"""INV-5: the version-consistency check passes on matched pins and fails on a
seeded mismatch.

Runs scripts/verify_versions.py as a subprocess (exercising its real exit code)
against the actual repo root (expect zero) and against a temporary root seeded with
a divergent ruff pin between requirements-dev.txt and the pre-commit config (expect
non-zero).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "verify_versions.py"

_PRECOMMIT_TEMPLATE = """\
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v{ruff}
    hooks:
      - id: ruff
  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v{mypy}
    hooks:
      - id: mypy
"""


def _run(root: Path) -> int:
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--root", str(root)],
        capture_output=True,
        text=True,
        check=False,
    ).returncode


def _seed(root: Path, req_ruff: str, pc_ruff: str, mypy: str) -> None:
    (root / "requirements-dev.txt").write_text(
        f"ruff=={req_ruff}\nmypy=={mypy}\npytest==8.3.4\npre-commit==4.0.1\n",
        encoding="utf-8",
    )
    (root / ".pre-commit-config.yaml").write_text(
        _PRECOMMIT_TEMPLATE.format(ruff=pc_ruff, mypy=mypy), encoding="utf-8"
    )


def test_matched_pins_pass() -> None:
    assert _run(REPO_ROOT) == 0, "real repo pins must be consistent (INV-5)"


def test_matched_seeded_pins_pass(tmp_path: Path) -> None:
    _seed(tmp_path, req_ruff="0.15.22", pc_ruff="0.15.22", mypy="1.15.0")
    assert _run(tmp_path) == 0


def test_seeded_mismatch_fails(tmp_path: Path) -> None:
    _seed(tmp_path, req_ruff="0.15.22", pc_ruff="0.14.0", mypy="1.15.0")
    assert _run(tmp_path) != 0, "a divergent pin across files must fail the build (INV-5)"
