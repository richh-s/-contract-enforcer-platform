"""
contracts/attributor.py
Phase 1 — Attribution helpers for data contract enforcement.

Provides utilities for tracing which pipeline run, model, or agent
produced a given data record, linking validation failures back to
their source.

Usage:
    from contracts.attributor import attribute_violations

    attributed = attribute_violations(violations, lineage_info)
"""

from __future__ import annotations

from typing import Any


def attribute_violations(
    violations: list[dict],
    lineage_info: dict | None,
) -> list[dict]:
    """
    Enrich a list of validation violation dicts with attribution metadata
    sourced from the contract's lineage block.

    Each violation is augmented with:
        source_pipeline  — upstream pipeline that produced the data
        git_commit       — commit hash at time of production
        git_author       — committer identity
        snapshot_id      — lineage snapshot the data belongs to

    Parameters
    ----------
    violations : list[dict]
        CheckResult dicts from contracts/runner.py (status != PASS).
    lineage_info : dict | None
        The lineage block from the data contract, or None.

    Returns
    -------
    list[dict]
        Same violations, each extended with an ``attribution`` sub-dict.
    """
    attribution: dict[str, Any] = {}

    if lineage_info:
        attribution = {
            "snapshot_id":     lineage_info.get("snapshot_id"),
            "git_commit":      lineage_info.get("git_commit"),
            "git_author":      lineage_info.get("git_author"),
            "git_committed_at": lineage_info.get("git_committed_at"),
            "source_pipeline": _infer_pipeline(lineage_info),
        }

    attributed = []
    for v in violations:
        enriched = dict(v)
        enriched["attribution"] = attribution
        attributed.append(enriched)

    return attributed


def _infer_pipeline(lineage_info: dict) -> str | None:
    """Derive a human-readable pipeline name from lineage upstream list."""
    upstream = lineage_info.get("upstream", [])
    if upstream:
        first = upstream[0]
        if isinstance(first, dict):
            return first.get("id") or first.get("description")
        return str(first)
    return lineage_info.get("source_file")
