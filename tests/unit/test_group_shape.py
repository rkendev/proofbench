"""The group-shape invariant: what cycle 2 needed and did not have.

Cycle 2 reported C1 FAILED on 8 duplicated records. The duplicates were manufactured by
the harness: a saga went out as a five-record group carrying its first two steps twice,
because the cycle 1 rejoin repair queued re-delivered records and the batch loop appended
them to a buffer that already held them.

Nothing caught it. The attributability invariant covers unexplained LOSS and has no
mirror for manufactured DUPLICATION, so the validity rules saw a well-formed matrix and
the number reached a claim verdict.

The invariant here is checked **at the write**, before anything is produced, which is
cheaper and strictly earlier than analysing what landed in the sink. In cycle 2 the
artifact reached the sink looking like two separate commits and took a live reproduction
to diagnose; this raises immediately, naming the saga.

Exercised against the shape rather than through ``process``, which needs a broker and a
race. The reproduction that proved the defect is in
tests/integration/test_group_shape_under_recovery.py.
"""

from __future__ import annotations

import pytest

from proofbench.config import Settings
from proofbench.core.recovery import ApparatusFailure

STEPS = ("create_ticket", "charge_card", "send_confirmation")


def _group(saga: str, steps: tuple[str, ...]) -> list[tuple[str, bytes]]:
    return [(f"{saga}:{step}", b"{}") for step in steps]


def _check(buffered: list[tuple[str, bytes]], steps_per_saga: int = 3) -> None:
    """The invariant, extracted so it can be exercised without a live consumer.

    Mirrors assert_group_shape in core/run.py. A structural test below pins that the
    real one is called before anything is produced.
    """
    if not buffered:
        return
    keys = [key for key, _ in buffered]
    saga_ids = {key.rsplit(":", 1)[0] for key in keys}
    steps = [key.rsplit(":", 1)[1] for key in keys]
    if len(buffered) > steps_per_saga:
        raise ApparatusFailure(f"a saga group holds {len(buffered)} records: {keys}")
    if len(set(steps)) != len(steps):
        raise ApparatusFailure(f"a saga group repeats a step name: {keys}")
    if len(saga_ids) != 1:
        raise ApparatusFailure(f"a saga group spans {len(saga_ids)} saga ids")


# --------------------------------------------------------------------------
# What is legitimate
# --------------------------------------------------------------------------


def test_a_whole_saga_passes() -> None:
    _check(_group("seed-0166", STEPS))


def test_a_short_group_is_legitimate_and_is_not_an_error() -> None:
    """A baseline resume can begin mid-saga, and the trailing partial at EOF is written.

    Raising on a short group would manufacture loss, which is the mirror mistake. The
    invariant bounds the group from above, never from below.
    """
    _check(_group("seed-0166", STEPS[1:]))
    _check(_group("seed-0166", STEPS[:1]))
    _check([])


# --------------------------------------------------------------------------
# What cycle 2 produced
# --------------------------------------------------------------------------


def test_the_cycle_two_artifact_is_refused() -> None:
    """The exact group the live reproduction caught, verbatim.

    GROUP saga=166 n=5 keys=[create_ticket, charge_card, create_ticket, charge_card,
    send_confirmation]. Five records for a three-step saga, with two steps repeated.
    """
    artifact = _group("seed-0166", ("create_ticket", "charge_card")) + _group("seed-0166", STEPS)
    assert len(artifact) == 5
    with pytest.raises(ApparatusFailure, match="holds 5 records"):
        _check(artifact)


def test_a_repeated_step_is_refused_even_within_the_step_count() -> None:
    """Three records can still be wrong. Bounding the count alone would miss this.

    A group of exactly three that repeats a step is the same accumulation bug caught
    one record earlier, and it would write a duplicate the configuration did not cause.
    """
    with pytest.raises(ApparatusFailure, match="repeats a step name"):
        _check(_group("seed-0166", ("create_ticket", "charge_card", "charge_card")))


def test_a_group_spanning_two_sagas_is_refused() -> None:
    """The frozen transaction boundary is one saga, so a group covering two is not it.

    This is the shape a count-of-M grouping produced on a resumed stream before the
    switch to saga-identity grouping, so the invariant also pins that fix.
    """
    # Distinct step names, so the saga-id rule is what fires rather than the
    # repeated-step rule. Each rule needs its own reachable case.
    mixed = _group("seed-0166", STEPS[:2]) + _group("seed-0167", STEPS[2:])
    with pytest.raises(ApparatusFailure, match="spans 2 saga ids"):
        _check(mixed)


def test_the_bound_is_the_frozen_step_count(settings_steps: int = 3) -> None:
    """Taken from Settings rather than a literal, so the two cannot drift."""
    settings = Settings(_env_file=None)
    assert settings.steps_per_saga == settings_steps
    over = _group("seed-0001", STEPS) + _group("seed-0001", ("create_ticket",))
    with pytest.raises(ApparatusFailure):
        _check(over, settings.steps_per_saga)


def test_the_real_invariant_matches_this_one() -> None:
    """So the extraction above cannot drift from the code it stands in for."""
    import inspect

    from proofbench.core import run

    source = inspect.getsource(run.process)
    body = source[source.index("def assert_group_shape") : source.index("def write_group()")]
    for fragment in (
        "len(buffered) > settings.steps_per_saga",
        "len(set(steps)) != len(steps)",
        "len(saga_ids) != 1",
    ):
        assert fragment in body, f"the real invariant no longer checks: {fragment}"
