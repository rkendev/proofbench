"""Every invariant is actually called, checked by AST and never by text search.

Three times in this run a guard passed vacuously. The third was a call-site check
written as a substring search: it matched the invariant's own ``def`` line and stayed
green with the call deleted. That is not three mistakes but one missing rule.

    A call-site check written as a text search is defective by construction, because
    the search always matches the definition.

So every check that an invariant is genuinely invoked walks the AST of the calling
module and asserts a Call node, and every one is proven red by deleting the call. This
file is the single place that rule is enforced, so a new invariant acquires the check by
being added to one table rather than by someone remembering.
"""

from __future__ import annotations

import ast
import inspect
from typing import Any

import pytest

# invariant -> (module attribute holding the caller, human reason it must be called)
CALL_SITES: tuple[tuple[str, str, str], ...] = (
    (
        "assert_group_shape",
        "process",
        "a buffer that accumulated the same records twice would be written unchecked, "
        "which is the cycle 2 artifact",
    ),
    (
        "assert_boundary_discriminates",
        "resolve_delivery_error",
        "a fault-window boundary stuck open would absorb every apparatus break as "
        "recovery and inflate C2 on evidence that looks ordinary",
    ),
    (
        "rejoin_consumer",
        "process",
        "a saga replay commits offsets and that needs group membership, which is the cycle 1 void",
    ),
    (
        "assert_losses_are_attributable",
        "run_matrix",
        "an unexplained loss would be reported as a claim result rather than as an "
        "apparatus defect",
    ),
    (
        "assert_denominators",
        "evaluate",
        "an all() over an empty or mis-filtered collection returns a pass, and a false "
        "C1 pass is the worst thing this project could publish",
    ),
)


def _callers(source: str) -> set[str]:
    """Every name called as a function anywhere in ``source``, from the AST."""
    tree = ast.parse(source)
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                names.add(node.func.id)
            elif isinstance(node.func, ast.Attribute):
                names.add(node.func.attr)
    return names


def _source_for(caller: str) -> str:
    if caller == "run_matrix":
        from proofbench.config import repo_root

        return (repo_root() / "scripts" / "run_matrix.py").read_text(encoding="utf-8")
    for module_name in ("proofbench.core.run", "proofbench.core.claims"):
        import importlib

        module = importlib.import_module(module_name)
        target: Any = getattr(module, caller, None)
        if target is not None:
            return inspect.getsource(target)
    raise AssertionError(f"no source found for caller {caller!r}")


@pytest.mark.parametrize(("invariant", "caller", "reason"), CALL_SITES, ids=lambda v: str(v)[:40])
def test_the_invariant_is_actually_called(invariant: str, caller: str, reason: str) -> None:
    """Proven red by deleting the call, which a text search could not detect."""
    assert invariant in _callers(_source_for(caller)), (
        f"{caller} does not call {invariant}. Without it, {reason}."
    )


def test_a_text_search_would_have_passed_where_the_ast_check_fails() -> None:
    """The rule itself, demonstrated rather than asserted.

    This is why the rule exists: with the call deleted the definition remains, so the
    substring is still present and a text-based gate stays green while the invariant is
    never invoked.
    """
    with_call_deleted = (
        "def assert_group_shape() -> None:\n    return None\n\ndef caller():\n    pass\n"
    )

    # A text search finds the name and concludes, wrongly, that it is called.
    assert "assert_group_shape" in with_call_deleted

    # The AST check sees that nothing calls it.
    assert "assert_group_shape" not in _callers(with_call_deleted)


def test_every_listed_invariant_exists() -> None:
    """So a renamed invariant fails loudly rather than silently dropping its check."""
    import importlib

    for invariant, caller, _ in CALL_SITES:
        modules = ("proofbench.core.run", "proofbench.core.claims", "proofbench.core.window")
        found = invariant in _callers(_source_for(caller)) or any(
            hasattr(importlib.import_module(m), invariant) for m in modules
        )
        assert found, f"{invariant} no longer exists, so its call-site check is dead"


def test_the_table_is_not_empty() -> None:
    """The parametrised rule above would pass vacuously with an empty table."""
    assert len(CALL_SITES) >= 5
