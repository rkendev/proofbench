"""INV-1: no secret or endpoint literal appears in source.

Greps every Python module under src/ for endpoint and credential patterns. All
connection details come from the config object populated by the environment;
example values live only in .env.example (which is not source and is excluded). The
patterns target literal values (URLs, host:port, access keys, embedded credentials),
not identifiers, so config field names do not false-positive.

There is deliberately no allowance list. The sibling repository this gate is ported
from carries a narrow exemption for a public repository link its dashboard renders;
ProofBench has no such source, so importing the exemption would be dead code that
only weakens the gate.
"""

from __future__ import annotations

import re
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[2] / "src"

FORBIDDEN = [
    ("url with scheme", re.compile(r"\b(?:https?|kafka|redis|postgres(?:ql)?)://\S+", re.I)),
    ("credentials in url", re.compile(r"://[^/\s]+:[^/@\s]+@")),
    ("aws access key id", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("ipv4 host:port", re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}:\d{2,5}\b")),
    ("localhost endpoint", re.compile(r"\blocalhost:\d{2,5}\b", re.I)),
    (
        "secret assignment",
        re.compile(
            r"(?i)\b(?:password|passwd|secret|token|api[_-]?key|"
            r"aws_secret_access_key)\b\s*[:=]\s*['\"][^'\"]+['\"]"
        ),
    ),
]


def test_source_has_no_secret_or_endpoint_literals() -> None:
    assert SRC_DIR.is_dir(), f"src not found at {SRC_DIR}"
    findings: list[str] = []
    for path in sorted(SRC_DIR.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        for label, pattern in FORBIDDEN:
            for match in pattern.finditer(text):
                findings.append(f"{path}: {label}: {match.group(0)!r}")
                break
    assert not findings, "secret/endpoint literal(s) in source (INV-1): " + "; ".join(findings)


def test_the_patterns_actually_match_what_they_claim_to() -> None:
    """The rule above passes by absence, so the patterns themselves are pinned.

    A typo that made one of them match nothing would be indistinguishable from a
    clean tree.
    """
    patterns = dict(FORBIDDEN)
    assert patterns["url with scheme"].search("bootstrap = 'kafka://host:9092'")
    assert patterns["credentials in url"].search("https://user:secretvalue@example.invalid/x")
    assert patterns["aws access key id"].search("AKIAIOSFODNN7EXAMPLE")
    assert patterns["ipv4 host:port"].search("connect('10.0.0.1:9092')")
    assert patterns["localhost endpoint"].search("servers = 'localhost:9092'")
    assert patterns["secret assignment"].search("api_key = 'abc123'")
    # A config field name is an identifier, not a literal, and must not match.
    assert not patterns["secret assignment"].search("broker_bootstrap_servers: str | None = None")
    assert not patterns["localhost endpoint"].search("consumer_max_batch_records: int = 100")
