"""Pure logic of the incremental vector store sync (no DB, no model: unit-testable). ADR 0007, ADR 0013.

A vector store is a projection of its source (for the LC 214: the chunks of the current law version). Each chunk has
a deterministic id that changes when its text changes (lc03: md5 of chunker version + text), so the sync is a set
difference between "what the source says should exist" and "what the store has":
    new   = desired - existing  -> embed + insert
    stale = existing - desired  -> delete (chunk removed, or its text changed and got a new id)
Re-running it converges to the same state (idempotent): a run that dies half-way is fixed by the next one, with no
flags or checkpoints.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SyncPlan:
    mode: str                 # "full" | "incremental"
    reason: str
    new_ids: frozenset
    stale_ids: frozenset
    kept: int


def plan_sync(desired_ids: set, existing_ids: set, *, model: str, stored_models: set,
              force_full: bool = False) -> SyncPlan:
    """Decide what to embed/delete. A model change forces a full rebuild: vectors from different
    models are not comparable, so they can never share a table (and may not even have the same size)."""
    reason = None
    if force_full:
        reason = "full refresh requested"
    elif stored_models - {model}:
        reason = f"embedding model changed ({', '.join(sorted(stored_models))} -> {model})"
    elif not existing_ids:
        reason = "empty vector store"

    if reason:
        return SyncPlan("full", reason, frozenset(desired_ids), frozenset(existing_ids), 0)
    new = frozenset(desired_ids - existing_ids)
    stale = frozenset(existing_ids - desired_ids)
    return SyncPlan("incremental", "delta against the current store", new, stale,
                    len(existing_ids) - len(stale))
