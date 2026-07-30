"""Writing evidence that survives a SIGKILL and that a stranger can recompute.

Two properties, both forced by PB-T3 rather than chosen for tidiness.

**Atomicity, because the writer gets killed.** ``Path.write_text`` truncates the
target and then writes, so a SIGKILL landing between the two leaves a zero-length or
half-written file where a JSON document should be. For most evidence that is
annoying; for the fault marker it is fatal, because the marker is what stops the
restarted phase re-firing the fault, and a truncated marker reads as "not yet
fired". So every write here goes to a temporary file in the same directory, is
fsynced, is moved into place with ``os.replace`` (atomic on a single filesystem),
and the containing directory is fsynced too. Without the directory fsync the rename
itself can be lost on power failure, which is a smaller risk than a SIGKILL but the
same class of problem and one extra call to close.

**Reproducible compression, because the digests are the product.** CLAIMS.md says
the harness and its matrix are the product, and the selling point is that a stranger
can recompute the counts. That means committing the ledgers, not only their digests:
digest-only was acceptable for an apparatus check, where the question was whether the
apparatus read zero, and it is not acceptable for a published claim, where the
question is whether the numbers follow from the records. JSON compresses roughly
tenfold, so 42 executions of three ledgers each is a couple of megabytes.

gzip embeds an mtime in its header by default, so compressing identical bytes twice
produces different files and every re-run would show a spurious diff. ``mtime=0``
removes it. The digests are taken over the **uncompressed** bytes, so they are a
statement about the ledger rather than about the compressor, and a reader who
decompresses with any tool gets a digest that matches.

The digests are computed here rather than typed. PB-T2's committed
``ledger_checksums.json`` was hand-written, which made it a claim about the ledgers
rather than a measurement of them, and the ledgers it described were not committed at
all.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import tempfile
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

from proofbench.interfaces.ledger import SideEffectRecord

# The one JSON form the whole evidence tree uses. sort_keys so a re-run is diffable,
# indent so a reader can open a file without a formatter, trailing newline so text
# tools treat it as a well-formed file. Identical to what PB-T2 used, kept identical
# on purpose: the control-run evidence has to stay comparable across the change.
_JSON_INDENT = 2

# gzip level 9 rather than the default 6. These files are written once, committed
# once, and read by anyone who wants to check the counts, so the asymmetry between
# compression cost and repository size runs entirely one way.
_GZIP_LEVEL = 9

# Fixed header mtime, so identical ledger bytes always produce identical gz bytes.
_GZIP_MTIME = 0


def canonical_json(payload: Any) -> str:
    """Return the one text form evidence is written in."""
    return json.dumps(payload, indent=_JSON_INDENT, sort_keys=True) + "\n"


def _fsync_dir(directory: Path) -> None:
    """Flush the directory entry, so a completed rename cannot be lost.

    Opened O_RDONLY because a directory cannot be opened for writing; fsync on the
    descriptor is what durably records the rename.
    """
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_atomically(path: Path, data: bytes) -> None:
    """Write ``data`` to ``path`` so that a kill mid-write cannot corrupt it.

    Either the old contents or the new ones are visible afterwards, never a
    truncated mixture. The temporary file is created in the destination directory
    rather than in the system temporary directory, because ``os.replace`` is only
    atomic within one filesystem and those two are frequently not the same one.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    # mkstemp rather than NamedTemporaryFile: the descriptor has to be fsynced and
    # closed before the rename, and the path has to outlive the handle, which
    # NamedTemporaryFile's delete semantics make awkward to express.
    descriptor, name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_dir(path.parent)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def write_json(path: Path, payload: Any) -> None:
    """Write one JSON evidence file atomically."""
    write_atomically(path, canonical_json(payload).encode("utf-8"))


def gzip_bytes(data: bytes) -> bytes:
    """Compress ``data`` reproducibly: the same input always gives the same output.

    ``mtime=0`` is the whole reason this is a function rather than a call to
    ``gzip.compress``. With gzip's default the header carries the current time, so
    the committed evidence would show a diff on every regeneration even when not one
    ledger record had changed, and a repository whose gate is byte-equality cannot
    afford a file that changes for no reason.
    """
    from io import BytesIO

    buffer = BytesIO()
    with gzip.GzipFile(
        fileobj=buffer, mode="wb", compresslevel=_GZIP_LEVEL, mtime=_GZIP_MTIME
    ) as handle:
        handle.write(data)
    return buffer.getvalue()


def write_json_gz(path: Path, payload: Any) -> str:
    """Write one gzipped JSON ledger and return the digest of its plain bytes.

    The digest covers the uncompressed document, so it is a statement about the
    ledger and not about the compressor. A reader who decompresses with any tool at
    all can recompute it, which is the property that makes the evidence checkable by
    someone who does not trust the author.
    """
    plain = canonical_json(payload).encode("utf-8")
    write_atomically(path, gzip_bytes(plain))
    return hashlib.sha256(plain).hexdigest()


def read_json_gz(path: Path) -> Any:
    """Read back a gzipped JSON ledger. Used by the C3 replay and by the tests."""
    return json.loads(gzip.decompress(path.read_bytes()).decode("utf-8"))


def digest_of(payload: Any) -> str:
    """The SHA-256 a gzipped ledger's plain bytes would have, without writing it."""
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------
# Ledgers
# --------------------------------------------------------------------------


def ledger_jsonable(records: Iterable[SideEffectRecord]) -> list[dict[str, Any]]:
    """One ledger as plain data, in the order it was given.

    Order is preserved rather than normalised, because a sink ledger's arrival order
    is itself evidence about what the fault did. C3's comparison sorts, and it does
    that at the point of comparison rather than by discarding the observation here.
    """
    return [
        {
            "idempotency_key": record.idempotency_key,
            "saga_id": record.saga_id,
            "step_name": record.step_name,
            "sequence": record.sequence,
            "payload_checksum": record.payload_checksum,
        }
        for record in records
    ]


def canonical_ledger_payload(records: Sequence[SideEffectRecord]) -> list[dict[str, Any]]:
    """The order-independent form C3 compares, with duplicates preserved.

    Claim C3 asks whether a replay "rebuilds the sink byte-identical to the original
    run, verified by checksum". Raw topic bytes cannot match: a replay gets different
    offsets, different timestamps and a different producer epoch. So the comparison
    is over the effect log, which is the faithful reading of "the sink" for a harness
    whose whole subject is side effects.

    Sorted by ``(sequence, idempotency_key)`` so that arrival order, which a replay
    is not obliged to reproduce, cannot make two identical effect logs disagree.
    Every occurrence is kept rather than deduplicated: if the original run duplicated
    a side effect, a faithful replay duplicates it too, and a comparison that
    silently collapsed duplicates would call those two sinks identical when one holds
    a double charge and the other does not.
    """
    return sorted(
        ledger_jsonable(records),
        key=lambda record: (int(record["sequence"]), str(record["idempotency_key"])),
    )


def ledger_checksum(records: Sequence[SideEffectRecord]) -> str:
    """The C3 checksum for one sink ledger: SHA-256 over its canonical form."""
    return digest_of(canonical_ledger_payload(records))
