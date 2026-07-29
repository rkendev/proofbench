"""The build-hygiene gate fails red on a broken pre-commit config.

Hermetic and network-free: it exercises `pre-commit validate-config`, which parses
the config without installing any hook environment. The seeded-broken fixture (a
plain YAML scalar containing colon-space inside a value) must make validate-config
exit non-zero; the real config must validate. The heavier
`pre-commit run --all-files` is exercised by scripts/verify-precommit.sh in
bootstrap, not here.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
REAL_CONFIG = REPO_ROOT / ".pre-commit-config.yaml"
BROKEN_CONFIG = Path(__file__).resolve().parent / "fixtures" / "broken_precommit_config.yaml"


def _precommit() -> str:
    candidate = Path(sys.executable).parent / "pre-commit"
    if candidate.exists():
        return str(candidate)
    found = shutil.which("pre-commit")
    if found:
        return found
    pytest.skip("pre-commit executable not found")


def _validate(config: Path) -> int:
    return subprocess.run(
        [_precommit(), "validate-config", str(config)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    ).returncode


def test_seeded_broken_config_fails_red() -> None:
    assert BROKEN_CONFIG.exists(), "broken fixture missing"
    assert _validate(BROKEN_CONFIG) != 0, "broken config must fail validate-config"


def test_real_config_validates() -> None:
    assert REAL_CONFIG.exists(), "real pre-commit config missing"
    assert _validate(REAL_CONFIG) == 0, "real config must pass validate-config"
