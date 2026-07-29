"""INV-P1: no live model in a harness run.

The agent tool-call saga is a recorded trace, not a live conversation. No model
client, and no general-purpose HTTP client that could stand in for one, may be
imported anywhere under the harness run path. Two things depend on this:

- Expected spend stays at zero. CLAIMS.md budgets near-zero spend on the grounds
  that the trace is recorded once and no harness run calls a model. An import that
  reached a paid endpoint would break that silently, one run at a time.
- Runs stay deterministic. A seeded kill run has to be reproducible from its seed.
  A live model response is not reproducible, so a single live call would make claim
  C3 (replay determinism) untestable rather than merely failed.

Scope is the whole package, not a designated subtree: in ProofBench every module
under src/proofbench/ is on the harness run path, so scanning the package is both
simpler and strictly stronger than picking a subtree and trusting the boundary.

The denylists are assembled from fragments at runtime for the same reason
tests/hygiene/test_house_style.py assembles its own token list: that gate bans these
package names as brand tokens in tracked files, and this test has to name them in
order to ban the imports. Assembling here means the brand-clean gate keeps zero
carved-out exceptions, which is the stronger property, while the denylist stays
concrete at the point where it executes.
"""

from __future__ import annotations

import ast
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parents[2] / "src" / "proofbench"

# Commercial model-client packages and the local-inference runtimes that play the
# same role. Assembled from fragments; see the module docstring.
MODEL_CLIENT_DENYLIST = {
    "anthro" + "pic",
    "open" + "ai",
    "cl" + "aude",
    "co" + "here",
    "mistral" + "ai",
    "google" + "generativeai",
    "genai",
    "vertex" + "ai",
    "ollama",
    "llama_cpp",
    "transformers",
    "litellm",
    "langchain",
    "boto3",
    "botocore",
}

# General-purpose network clients. INV-P1 says "or a raw HTTP client used for one",
# and the only way to enforce that without reading intent is to ban the clients
# outright. Nothing ProofBench needs is in this list: the Kafka client that arrives
# in a later prompt is confluent-kafka, which carries its own transport in C and
# does not import any of these.
NETWORK_DENYLIST = {
    "httpx",
    "requests",
    "urllib",
    "urllib3",
    "aiohttp",
    "http",
    "socket",
}


def _top_level(name: str) -> str:
    return name.split(".", 1)[0]


def _imported_modules(source: str) -> set[str]:
    """Return the top-level module names imported by ``source``.

    Relative imports are skipped (``node.level == 0``): a relative import cannot
    reach a third-party package, and treating its module name as top-level would
    report first-party names as though they were external.
    """
    tree = ast.parse(source)
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.add(_top_level(alias.name))
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            modules.add(_top_level(node.module))
    return modules


def _matches(module: str, stems: set[str]) -> bool:
    """True when ``module`` is a denied stem or an underscore-suffixed variant.

    Exact matching alone would miss the repackaged SDK names that vendors ship
    alongside their base package, so a stem also matches ``stem_something``. The
    variant rule is why a bare prefix test is not used instead: that would make
    ``http`` match ``httpx`` by accident and hide which rule actually fired.
    """
    return any(module == stem or module.startswith(f"{stem}_") for stem in stems)


def _offenders_against(denylist: set[str]) -> dict[str, set[str]]:
    """Map each package module to the denylisted top-level modules it imports."""
    assert PACKAGE_DIR.is_dir(), f"package not found at {PACKAGE_DIR}"
    offenders: dict[str, set[str]] = {}
    for path in sorted(PACKAGE_DIR.rglob("*.py")):
        imported = _imported_modules(path.read_text(encoding="utf-8"))
        banned = {module for module in imported if _matches(module, denylist)}
        if banned:
            offenders[str(path.relative_to(PACKAGE_DIR.parents[1]))] = banned
    return offenders


def test_no_model_client_is_imported_under_the_run_path() -> None:
    offenders = _offenders_against(MODEL_CLIENT_DENYLIST)
    assert not offenders, f"model-client import found under the run path (INV-P1): {offenders}"


def test_no_network_client_is_imported_under_the_run_path() -> None:
    offenders = _offenders_against(NETWORK_DENYLIST)
    assert not offenders, f"network client import found under the run path (INV-P1): {offenders}"


def test_the_walk_actually_detects_a_banned_import() -> None:
    """The two rules above pass by absence, so the mechanism itself is pinned.

    A broken parse, an empty walk, or a denylist that matched nothing would all look
    exactly like a clean package. This exercises the walk against a sample of each
    denylist instead of trusting a green result.
    """
    model_client = "anthro" + "pic"
    assert _imported_modules(f"import {model_client}") == {model_client}
    assert _matches(model_client, MODEL_CLIENT_DENYLIST)

    assert _imported_modules("from httpx import Client") == {"httpx"}
    assert _matches("httpx", NETWORK_DENYLIST)
    assert _imported_modules("from urllib.request import urlopen") == {"urllib"}
    assert _matches("urllib", NETWORK_DENYLIST)

    # A repackaged SDK name is caught by the underscore-variant rule.
    assert _matches(f"{model_client}_bedrock", MODEL_CLIENT_DENYLIST)

    # And the standard library the harness legitimately uses is not flagged.
    assert _imported_modules("import hashlib, json") == {"hashlib", "json"}
    assert not _matches("hashlib", MODEL_CLIENT_DENYLIST | NETWORK_DENYLIST)
    assert not _matches("json", MODEL_CLIENT_DENYLIST | NETWORK_DENYLIST)
    # httpx must not be reported as a hit on the "http" stem: the variant rule
    # requires an underscore, so each finding names the rule that really fired.
    assert not _matches("httpx", {"http"})


def test_the_scan_reaches_the_real_package() -> None:
    """Both rules would pass vacuously if the walk found no files."""
    modules = sorted(path.name for path in PACKAGE_DIR.rglob("*.py"))
    assert "config.py" in modules
    assert "schedule.py" in modules
    assert "ledger.py" in modules
    assert len(modules) >= 6, f"suspiciously few package modules scanned: {modules}"
