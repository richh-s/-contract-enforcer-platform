"""
contracts/registry.py
Contract Registry — PRIMARY blast radius source.

The registry is the authoritative catalog of all deployed contracts.
It is queried FIRST for blast radius computation; contract-level lineage
adds depth (breaking_if_changed, fields_consumed) as enrichment.

Design principle (from spec):
  "Enforcement always runs at the consumer boundary.
   The registry is the primary source for identifying consumers;
   lineage is enrichment."

Loading order (primary → secondary):
  1. contract_registry/subscriptions.yaml  ← PRIMARY authoritative subscriptions
  2. generated_contracts/*.yaml lineage     ← adds git context, snapshot IDs

Registry index structure:
  {
    contract_id: {
      "fields":     [list of field paths],
      "consumers":  [
        {
          "consumer_id": "week4-cartographer",
          "fields_consumed": [...],
          "breaking_if_changed": [...],
          "enforcement_mode": "ENFORCE | WARN | AUDIT"
        }
      ]
    }
  }

Usage:
    from contracts.registry import ContractRegistry

    reg = ContractRegistry()
    blast = reg.query_blast_radius("week3-extractions", {"confidence", "doc_id"})
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

_HERE = Path(__file__).resolve().parent.parent


class ContractRegistry:
    """
    Loads all deployed contracts and provides cross-contract consumer lookups.

    The registry is the PRIMARY source for blast radius computation.
    It knows about every contract in generated_contracts/ and every
    consumer declared in their lineage.downstream sections.

    Lineage in individual contracts enriches the registry answer with
    field-level detail (breaking_if_changed, specific fields_consumed).
    """

    def __init__(
        self,
        contracts_dir: Path | None = None,
        registry_cache: Path | None = None,
    ) -> None:
        self._contracts_dir = contracts_dir or (_HERE / "generated_contracts")
        self._cache_path    = registry_cache or (_HERE / "schema_snapshots" / "registry_index.json")
        self._index: dict[str, dict] = {}
        self._load()

    # ── Index build ───────────────────────────────────────────────────────────

    def _load(self) -> None:
        """
        Build registry index.

        Step 1 — subscriptions.yaml (PRIMARY):
          contract_registry/subscriptions.yaml is the authoritative list of
          every consumer subscription. It is loaded first so that blast radius
          queries always reflect the full declared subscription set, even for
          contracts whose generated YAML has an empty lineage.downstream.

        Step 2 — generated_contracts/*.yaml (ENRICHMENT):
          Scans generated contracts to pull in field lists and any lineage
          consumers not already declared in subscriptions.yaml.
          Duplicate consumer entries (same consumer_id for same contract) are
          skipped — subscriptions.yaml always wins.
        """
        # ── Step 1: Load subscriptions.yaml as PRIMARY consumer source ────────
        subs_path = _HERE / "contract_registry" / "subscriptions.yaml"
        if subs_path.exists():
            try:
                with open(subs_path) as fh:
                    subs = yaml.safe_load(fh) or {}
                for sub in subs.get("subscriptions") or []:
                    cid = sub.get("contract_id", "")
                    if not cid:
                        continue
                    if cid not in self._index:
                        self._index[cid] = {
                            "fields":      [],
                            "consumers":   [],
                            "source_file": str(subs_path),
                        }
                    seen = {c["consumer_id"] for c in self._index[cid]["consumers"]}
                    for consumer in sub.get("consumers") or []:
                        c_id = consumer.get("consumer_id", "unknown")
                        if c_id not in seen:
                            self._index[cid]["consumers"].append(consumer)
                            seen.add(c_id)
            except Exception:
                pass  # malformed subscriptions.yaml — fall through to contracts

        # ── Step 2: Enrich with field lists from generated_contracts/*.yaml ───
        for path in sorted(self._contracts_dir.glob("*.yaml")):
            if path.name.endswith("_dbt.yml"):
                continue
            try:
                with open(path) as fh:
                    contract = yaml.safe_load(fh)
                self._index_contract(contract, path)
            except Exception:
                pass  # malformed contract — skip, don't crash

        # Persist index so it can be inspected
        self._cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._cache_path, "w") as fh:
            json.dump(self._index, fh, indent=2)

    def _index_contract(self, contract: dict, path: Path) -> None:
        """Extract fields and consumers from one contract into the index."""
        contract_id = (
            contract.get("id", "")
            .replace("urn:contract:", "")
            .replace(":v1", "")
            .replace(":v2", "")
        ) or path.stem

        models    = contract.get("models", {}) or {}
        lineage   = contract.get("lineage", {}) or {}
        downstream = lineage.get("downstream", []) or []

        # Collect all field paths from this contract's model
        fields: list[str] = []
        for model_name, model_def in models.items():
            for fname in (model_def.get("fields") or {}).keys():
                fields.append(fname)

        # Consumers declared in lineage.downstream — only add if not already
        # present from subscriptions.yaml (subscriptions.yaml is authoritative)
        existing = self._index.get(contract_id, {})
        seen_ids = {c["consumer_id"] for c in existing.get("consumers", [])}
        new_consumers: list[dict] = list(existing.get("consumers", []))
        for node in downstream:
            c_id = node.get("id", "unknown")
            if c_id not in seen_ids:
                new_consumers.append({
                    "consumer_id":       c_id,
                    "description":       node.get("description", ""),
                    "fields_consumed":   node.get("fields_consumed") or [],
                    "breaking_if_changed": node.get("breaking_if_changed") or [],
                    "source_contract":   contract_id,
                })
                seen_ids.add(c_id)

        self._index[contract_id] = {
            "fields":    fields,
            "consumers": new_consumers,
            "source_file": str(path),
        }

    # ── Primary query: blast radius ───────────────────────────────────────────

    def query_blast_radius(
        self,
        contract_id: str,
        breaking_fields: set[str],
        lineage_downstream: list[dict] | None = None,
    ) -> dict:
        """
        Compute blast radius using the registry as PRIMARY source.

        Step 1 — Registry query (primary):
          Find all consumers in the registry who declare any breaking_field
          in their fields_consumed or breaking_if_changed.
          This covers ALL contracts, not just the one being diffed.

        Step 2 — Lineage enrichment (secondary):
          If lineage_downstream is provided, merge any additional consumers
          not already found in the registry. Lineage adds depth for the
          specific contract but does not replace the registry result.

        Returns a blast radius dict with registry_consumers (primary)
        and lineage_enriched_consumers (secondary) clearly separated.
        """
        # ── Step 1: Registry primary query ────────────────────────────────────
        registry_consumers: list[dict] = []
        seen_ids: set[str] = set()

        # Check the direct contract first, then cross-contract
        search_order = [contract_id] + [
            cid for cid in self._index if cid != contract_id
        ]

        for cid in search_order:
            entry = self._index.get(cid)
            if not entry:
                continue
            for consumer in entry["consumers"]:
                c_id = consumer["consumer_id"]
                if c_id in seen_ids:
                    continue
                consumed    = set(consumer.get("fields_consumed") or [])
                breaking    = set(consumer.get("breaking_if_changed") or [])
                if breaking_fields & (consumed | breaking):
                    registry_consumers.append({
                        **consumer,
                        "registry_source": cid,
                        "matched_fields": sorted(
                            breaking_fields & (consumed | breaking)
                        ),
                    })
                    seen_ids.add(c_id)

        # ── Step 2: Lineage enrichment ─────────────────────────────────────────
        lineage_enriched: list[dict] = []
        if lineage_downstream:
            for node in lineage_downstream:
                c_id = node.get("id", "unknown")
                if c_id in seen_ids:
                    continue  # already found via registry
                consumed = set(node.get("fields_consumed") or [])
                breaking = set(node.get("breaking_if_changed") or [])
                if breaking_fields & (consumed | breaking):
                    lineage_enriched.append({
                        "consumer_id":       c_id,
                        "fields_consumed":   node.get("fields_consumed") or [],
                        "breaking_if_changed": node.get("breaking_if_changed") or [],
                        "registry_source":   "lineage_only",
                        "matched_fields":    sorted(
                            breaking_fields & (consumed | breaking)
                        ),
                    })
                    seen_ids.add(c_id)

        all_affected = registry_consumers + lineage_enriched

        return {
            "affected_consumers_count":    len(all_affected),
            "registry_consumers":          registry_consumers,
            "lineage_enriched_consumers":  lineage_enriched,
            "affected_nodes":              [c["consumer_id"] for c in all_affected],
            "affected_pipelines":          [c["consumer_id"] for c in all_affected],
            "blast_radius_method":         "registry_primary+lineage_enrichment",
        }

    # ── Helpers ───────────────────────────────────────────────────────────────

    def get_contract(self, contract_id: str) -> dict | None:
        return self._index.get(contract_id)

    def list_contracts(self) -> list[str]:
        return sorted(self._index.keys())

    def consumer_boundary_check(
        self,
        contract_id: str,
        consumer_id: str,
        field: str,
    ) -> bool:
        """
        Returns True if consumer_id has declared field as a contract dependency.
        This is the consumer-boundary enforcement check:
        consumers declare what they depend on; the registry enforces it.
        """
        entry = self._index.get(contract_id, {})
        for consumer in entry.get("consumers", []):
            if consumer["consumer_id"] == consumer_id:
                combined = set(consumer.get("fields_consumed", [])) | \
                           set(consumer.get("breaking_if_changed", []))
                return field in combined
        return False
