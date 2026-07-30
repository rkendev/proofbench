"""Evidence that survives a kill and that a stranger can recompute.

Both properties are forced by PB-T3 rather than chosen. The writer gets SIGKILLed,
so a half-written file is a real outcome and not a theoretical one; and CLAIMS.md
makes the matrix the product, whose selling point is that someone who does not trust
the author can recompute the counts, which means committing the ledgers rather than
only their digests.

The gzip determinism test is the one that would otherwise be found the hard way: with
gzip's default header mtime, committing the evidence twice produces different bytes
for identical records, and a repository whose gates are byte-equality cannot afford a
file that changes for no reason.
"""

from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

import pytest

from proofbench.core.evidence import (
    canonical_json,
    canonical_ledger_payload,
    digest_of,
    gzip_bytes,
    ledger_checksum,
    ledger_jsonable,
    read_json_gz,
    write_atomically,
    write_json,
    write_json_gz,
)
from proofbench.interfaces.ledger import SideEffectRecord


def record(key: str, sequence: int, checksum: str = "a" * 64) -> SideEffectRecord:
    return SideEffectRecord(
        idempotency_key=key,
        saga_id=key.split(":", 1)[0],
        step_name=key.split(":", 1)[1],
        sequence=sequence,
        payload_checksum=checksum,
    )


LEDGER = (
    record("s0000:create_ticket", 0),
    record("s0000:charge_card", 1),
    record("s0000:send_confirmation", 2),
)


# --------------------------------------------------------------------------
# Atomicity: a kill mid-write cannot leave a truncated document
# --------------------------------------------------------------------------


def test_a_write_lands_whole(tmp_path: Path) -> None:
    target = tmp_path / "run_summary.json"
    write_json(target, {"status": "clean", "duplicated": 0})
    assert json.loads(target.read_text(encoding="utf-8"))["status"] == "clean"


def test_an_overwrite_leaves_no_intermediate_state(tmp_path: Path) -> None:
    """os.replace is atomic, so a reader sees the old file or the new one.

    The marker depends on this entirely. It has to be durable before os.kill, or the
    restarted phase reads "not yet fired" from a truncated file and re-fires the
    fault, which loops the run forever.
    """
    target = tmp_path / "fault_state.json"
    write_json(target, {"fired": False})
    write_json(target, {"fired": True})
    assert json.loads(target.read_text(encoding="utf-8")) == {"fired": True}


def test_no_temporary_file_is_left_behind(tmp_path: Path) -> None:
    write_json(tmp_path / "a.json", {"x": 1})
    write_json_gz(tmp_path / "b.json.gz", {"x": 1})
    leftovers = [p.name for p in tmp_path.iterdir() if p.name.startswith(".")]
    assert leftovers == [], f"temporary files survived: {leftovers}"


def test_a_failed_write_removes_its_temporary_file(tmp_path: Path) -> None:
    """Otherwise a killed run would litter the evidence tree with partial files."""
    with pytest.raises(TypeError):
        write_json(tmp_path / "bad.json", {"unserializable": object()})
    assert list(tmp_path.iterdir()) == []


def test_the_temporary_file_is_created_beside_its_target(tmp_path: Path) -> None:
    """os.replace is only atomic within one filesystem.

    A temporary file in the system temporary directory would make the rename a copy
    across filesystems on many hosts, which is not atomic and would reintroduce
    exactly the torn-write window this module exists to close.
    """
    nested = tmp_path / "run_07" / "good"
    write_json(nested / "summary.json", {"ok": True})
    assert (nested / "summary.json").is_file()
    # The parent was created, which is what lets a phase write into a fresh tree.
    assert nested.is_dir()


def test_bytes_are_written_verbatim(tmp_path: Path) -> None:
    target = tmp_path / "raw.bin"
    write_atomically(target, b"\x00\x01\x02not-text")
    assert target.read_bytes() == b"\x00\x01\x02not-text"


# --------------------------------------------------------------------------
# Reproducible compression
# --------------------------------------------------------------------------


def test_gzip_of_identical_bytes_is_identical(tmp_path: Path) -> None:
    """The mtime=0 rule, which is the whole reason gzip_bytes exists.

    With gzip's default header the same ledger compresses to different bytes on every
    run, so the committed evidence would show a diff whenever it was regenerated even
    with no record changed. A repository whose gates are byte-equality cannot carry a
    file that changes for no reason.
    """
    payload = {"records": ledger_jsonable(LEDGER)}
    first = gzip_bytes(canonical_json(payload).encode("utf-8"))
    second = gzip_bytes(canonical_json(payload).encode("utf-8"))
    assert first == second

    a, b = tmp_path / "one.json.gz", tmp_path / "two.json.gz"
    write_json_gz(a, payload)
    write_json_gz(b, payload)
    assert a.read_bytes() == b.read_bytes()


def test_the_default_gzip_header_really_does_vary() -> None:
    """The rule above would pass vacuously if gzip were deterministic anyway.

    This pins the hazard rather than the fix: the header carries an mtime, so two
    compressions of identical bytes differ in the four bytes at offset 4. Without
    this the mtime=0 argument would be an assertion about gzip that nobody checked.
    """
    data = b"the same bytes both times"
    with_mtime = gzip.compress(data, mtime=1)
    with_other = gzip.compress(data, mtime=2)
    assert with_mtime != with_other
    assert with_mtime[4:8] != with_other[4:8]
    # And the fix removes exactly that difference.
    assert gzip_bytes(data)[4:8] == b"\x00\x00\x00\x00"


def test_a_gzipped_ledger_round_trips(tmp_path: Path) -> None:
    payload = {"records": ledger_jsonable(LEDGER)}
    target = tmp_path / "observed_sink_a.json.gz"
    write_json_gz(target, payload)
    assert read_json_gz(target) == payload


def test_the_compression_actually_pays_for_itself(tmp_path: Path) -> None:
    """The evidence policy rests on roughly tenfold, so it is measured not assumed.

    600 records per ledger, three ledgers per execution, 42 executions. If the ratio
    were nearer twofold the policy would be committing tens of megabytes rather than
    a couple, and that is worth knowing before 42 directories are written.
    """
    ledger = tuple(record(f"s{index // 3:04d}:step{index % 3}", index) for index in range(600))
    payload = {"records": ledger_jsonable(ledger)}
    plain = canonical_json(payload).encode("utf-8")
    compressed = gzip_bytes(plain)
    ratio = len(plain) / len(compressed)
    assert ratio > 5.0, f"gzip only achieved {ratio:.1f}x, so the evidence budget is wrong"


# --------------------------------------------------------------------------
# Digests over the plain bytes, computed rather than typed
# --------------------------------------------------------------------------


def test_the_digest_covers_the_uncompressed_document(tmp_path: Path) -> None:
    """So a reader with any gzip tool can recompute it.

    A digest over the compressed bytes would be a statement about the compressor:
    correct, checkable only by someone using the same compressor at the same level,
    and useless to the stranger the evidence policy is written for.
    """
    payload = {"records": ledger_jsonable(LEDGER)}
    target = tmp_path / "expected_ledger.json.gz"
    returned = write_json_gz(target, payload)

    plain = gzip.decompress(target.read_bytes())
    assert returned == hashlib.sha256(plain).hexdigest()
    assert returned == digest_of(payload)


def test_a_truncated_ledger_fails_its_digest(tmp_path: Path) -> None:
    """The seeded violation for the evidence format.

    A digest nobody can fail is decoration. Truncating the committed file has to make
    the recorded checksum disagree, or the digests in ledger_checksums.json say
    nothing about the ledgers beside them.
    """
    payload = {"records": ledger_jsonable(LEDGER)}
    target = tmp_path / "observed_sink_b.json.gz"
    recorded = write_json_gz(target, payload)

    intact = target.read_bytes()
    target.write_bytes(intact[: len(intact) // 2])

    with pytest.raises((gzip.BadGzipFile, EOFError, OSError)):
        read_json_gz(target)

    # And a ledger that decompresses but has lost a record also disagrees.
    short = {"records": ledger_jsonable(LEDGER[:-1])}
    write_json_gz(target, short)
    assert digest_of(short) != recorded


# --------------------------------------------------------------------------
# The C3 canonical form
# --------------------------------------------------------------------------


def test_the_canonical_form_is_independent_of_arrival_order() -> None:
    """A replay is not obliged to reproduce arrival order, only content.

    Offsets, timestamps and producer epochs all differ on a replay, so sorting is
    what lets "byte-identical" mean something about the effect log rather than
    something impossible about the partition.
    """
    forwards = ledger_checksum(LEDGER)
    backwards = ledger_checksum(tuple(reversed(LEDGER)))
    assert forwards == backwards


def test_the_canonical_form_keeps_every_duplicate() -> None:
    """Collapsing duplicates would make C3 blind to the thing C1 measures.

    If the original run duplicated a side effect, a faithful replay duplicates it
    too. A comparison that deduplicated would call a sink holding one charge and a
    sink holding two identical, which is precisely the difference this project
    exists to count.
    """
    once = ledger_checksum(LEDGER)
    twice = ledger_checksum((*LEDGER, LEDGER[1]))
    assert once != twice
    assert len(canonical_ledger_payload((*LEDGER, LEDGER[1]))) == 4


def test_the_canonical_form_notices_a_changed_payload() -> None:
    """Keys alone would call a replay identical that rebuilt different contents.

    That is the failure claim C3 exists to detect, so it must not be invisible to
    C3's own comparison.
    """
    tampered = (LEDGER[0], record("s0000:charge_card", 1, checksum="b" * 64), LEDGER[2])
    assert ledger_checksum(LEDGER) != ledger_checksum(tampered)


def test_the_arrival_order_is_still_recorded_in_the_evidence() -> None:
    """Sorting happens at comparison time, not by discarding the observation.

    A sink ledger's arrival order is evidence about what the fault did, so the
    written ledger keeps it and only the C3 comparison normalises.
    """
    reversed_ledger = tuple(reversed(LEDGER))
    written = ledger_jsonable(reversed_ledger)
    assert [row["sequence"] for row in written] == [2, 1, 0]
    assert [row["sequence"] for row in canonical_ledger_payload(reversed_ledger)] == [0, 1, 2]
