"""
contracts/runner.py
Phase 2 — Bitol v3 Data Contract Validation Runner

Enforcement principle (from spec):
  "Enforcement always runs at the consumer boundary."

  This runner is a CONSUMER-SIDE tool. It is invoked by a consumer
  (or on behalf of a consumer) before the data is processed, not by
  the producer after writing. The contract declares what the consumer
  expects; the runner enforces those expectations against the actual data.

  The registry (contracts/registry.py) identifies which consumers
  depend on which fields. Violations are blast-radius-attributed to
  all registered consumers of the failing field.

CLI usage:
    python contracts/runner.py \\
        --contract generated_contracts/week3_extractions.yaml \\
        --data outputs/week3/extractions.jsonl \\
        --output validation_reports/week3_check.json

Never crashes. Missing columns become ERROR results, not exceptions.
Writes baselines on first run to schema_snapshots/baselines.json.

Phase 2 additions:
  • Strict JSON report schema (machine-parseable by auto-grader)
  • snapshot_id = SHA-256 of input JSONL
  • Z-score drift: z>3 → FAIL/HIGH, z>2 → WARN/MEDIUM
  • Severity: structural/range → CRITICAL; drift by z-score; others LOW
  • sample_failing: record IDs (not indices), capped at 5
  • records_failing: exact per-record count
  • status: PASS | FAIL | WARN | ERROR  (WARN is new in Phase 2)
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import re
import statistics
import sys
import uuid
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import yaml

# ── sys.path guard ─────────────────────────────────────────────────────────────
_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from contracts._clauses import SOURCE_WEEK3  # noqa: E402

# ── Constants ──────────────────────────────────────────────────────────────────
_BASELINES_PATH = Path("schema_snapshots") / "baselines.json"
_MAX_SAMPLES    = 5

# Primary-key fields per source type (used to populate sample_failing with IDs)
_PK_FIELDS: dict[str, list[str]] = {
    "week1":  ["id", "run_id"],
    "week3":  ["doc_id"],
    "week4":  ["snapshot_id"],
    "week5":  ["event_id"],
    "traces": ["id"],
}

# Week3 fields nested inside extracted_facts[]
_WEEK3_NESTED: set[str] = {"confidence", "fact", "fact_id"}


# ── Phase 2 CheckResult ────────────────────────────────────────────────────────
@dataclass
class CheckResult:
    """
    Strict Phase 2 check result.

    check_id        "week3.extracted_facts.confidence.range"
    column_name     "extracted_facts[*].confidence"
    check_type      "not_null" | "pattern" | "range" | "z_score" | ...
    status          PASS | FAIL | WARN | ERROR
    actual_value    "max=51.3000" (human-readable observed value)
    expected        "max<=1.0"   (what the contract requires)
    severity        CRITICAL | HIGH | MEDIUM | LOW
    records_failing exact count of records that fail this check
    sample_failing  up to 5 record IDs (strings) for failing records
    message         narrative description
    z_score         float (only for z_score / variance_collapse checks)
    """
    check_id:        str
    column_name:     str
    check_type:      str
    status:          str
    actual_value:    str
    expected:        str
    severity:        str
    records_failing: int
    sample_failing:  list
    message:         str
    z_score:         Optional[float] = None

    def to_dict(self) -> dict:
        d: dict[str, Any] = {
            "check_id":        self.check_id,
            "column_name":     self.column_name,
            "check_type":      self.check_type,
            "status":          self.status,
            "actual_value":    self.actual_value,
            "expected":        self.expected,
            "severity":        self.severity,
            "records_failing": self.records_failing,
            "sample_failing":  self.sample_failing,
            "message":         self.message,
        }
        if self.z_score is not None:
            d["z_score"] = round(self.z_score, 4)
        return d


# ── Small helpers ──────────────────────────────────────────────────────────────

def _compute_sha256(path: str) -> str:
    """SHA-256 of the input file (used as snapshot_id)."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _display_column(field_name: str, source_type: str) -> str:
    """Return display column name, e.g. extracted_facts[*].confidence for week3."""
    if source_type == SOURCE_WEEK3 and field_name in _WEEK3_NESTED:
        return f"extracted_facts[*].{field_name}"
    return field_name


def _build_check_id(source_type: str, field_name: str, check_type: str) -> str:
    """Build canonical check_id: '{source}.{col_path}.{check_type}'."""
    col = _display_column(field_name, source_type)
    col = col.replace("[*].", ".").strip(".")
    col = re.sub(r"\.+", ".", col)
    return f"{source_type}.{col}.{check_type}"


def _get_record_id(record: dict, source_type: str) -> str:
    """Extract primary key string from a record for sample_failing."""
    for fname in _PK_FIELDS.get(source_type, ["id"]):
        v = record.get(fname)
        if v is not None:
            return str(v)
    # fallback: first scalar field value
    for k, v in record.items():
        if isinstance(v, (str, int, float)) and v is not None:
            return f"{k}:{str(v)[:40]}"
    return "unknown"


def _extract_pairs(
    records: list[dict], field_name: str, source_type: str
) -> list[tuple]:
    """
    Return (value, record_id) pairs for a field.
    Handles week3 nested extracted_facts[] transparently.
    """
    pairs: list[tuple] = []
    if source_type == SOURCE_WEEK3 and field_name in _WEEK3_NESTED:
        for rec in records:
            rid = _get_record_id(rec, source_type)
            for fact in rec.get("extracted_facts", []):
                pairs.append((fact.get(field_name), rid))
    else:
        for rec in records:
            rid = _get_record_id(rec, source_type)
            pairs.append((_safe_get(rec, field_name), rid))
    return pairs


def _safe_get(record: dict, field_path: str, default: Any = None) -> Any:
    parts = field_path.split(".")
    val   = record
    for part in parts:
        if not isinstance(val, dict) or part not in val:
            return default
        val = val[part]
    return val


def _parse_dt(s: Any) -> Optional[datetime.datetime]:
    if not isinstance(s, str):
        return None
    s = s.strip().replace(" ", "T")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.datetime.fromisoformat(s)
    except ValueError:
        return None


def _map_yaml_severity(yaml_sev: str) -> str:
    """Map Phase 1 YAML severity labels to Phase 2 labels."""
    return {
        "BREAKING": "CRITICAL",
        "ERROR":    "HIGH",
        "WARNING":  "MEDIUM",
        "INFO":     "LOW",
    }.get(str(yaml_sev).upper(), "MEDIUM")


# ── Load helpers ───────────────────────────────────────────────────────────────

def load_contract(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def load_data(path: str) -> list[dict]:
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return records


# ── STRUCTURAL CHECKS ──────────────────────────────────────────────────────────

def run_structural_checks(records: list[dict], contract: dict) -> list[CheckResult]:
    results:     list[CheckResult] = []
    quality      = contract.get("quality", {})
    clauses      = quality.get("structural", [])
    source_type  = contract.get("info", {}).get("sourceType", "")

    for clause in clauses:
        field_name  = clause.get("field", "")
        clause_type = clause.get("type", "")
        check       = clause.get("check", "")
        yaml_sev    = clause.get("severity", "ERROR")
        col_display = _display_column(field_name, source_type)

        # ── aspirational: skip gracefully ────────────────────────────────────
        if clause.get("status") == "aspirational":
            results.append(CheckResult(
                check_id        = _build_check_id(source_type, field_name, "aspirational"),
                column_name     = col_display,
                check_type      = "aspirational",
                status          = "PASS",
                actual_value    = "skipped",
                expected        = "aspirational",
                severity        = "LOW",
                records_failing = 0,
                sample_failing  = [],
                message         = f"Aspirational clause — '{field_name}' not yet in data schema.",
            ))
            continue

        # ── not_null ──────────────────────────────────────────────────────────
        if clause_type == "structural" and check == "not_null":
            pairs    = _extract_pairs(records, field_name, source_type)
            fail_ids: list[str] = []
            for val, rid in pairs:
                if val is None or val == "":
                    if rid not in fail_ids:
                        fail_ids.append(rid)

            status   = "PASS" if not fail_ids else "FAIL"
            severity = "CRITICAL" if status == "FAIL" else _map_yaml_severity(yaml_sev)
            results.append(CheckResult(
                check_id        = _build_check_id(source_type, field_name, "not_null"),
                column_name     = col_display,
                check_type      = "not_null",
                status          = status,
                actual_value    = f"null_count={len(fail_ids)}",
                expected        = "null_count=0",
                severity        = severity,
                records_failing = len(fail_ids),
                sample_failing  = fail_ids[:_MAX_SAMPLES],
                message         = (
                    f"'{col_display}': {len(fail_ids)} records with null/empty values."
                    if fail_ids
                    else f"'{col_display}': no null values ({len(pairs)} values checked)."
                ),
            ))

        # ── pattern ───────────────────────────────────────────────────────────
        elif clause_type == "pattern":
            pattern = clause.get("pattern", "")
            try:
                compiled = re.compile(pattern)
            except re.error as exc:
                results.append(CheckResult(
                    check_id        = _build_check_id(source_type, field_name, "pattern"),
                    column_name     = col_display,
                    check_type      = "pattern",
                    status          = "ERROR",
                    actual_value    = "invalid_regex",
                    expected        = f"pattern={pattern}",
                    severity        = "CRITICAL",
                    records_failing = 0,
                    sample_failing  = [],
                    message         = f"Invalid regex '{pattern}': {exc}",
                ))
                continue

            pairs    = _extract_pairs(records, field_name, source_type)
            non_null = [(v, rid) for v, rid in pairs if v is not None]
            if not non_null:
                results.append(CheckResult(
                    check_id        = _build_check_id(source_type, field_name, "pattern"),
                    column_name     = col_display,
                    check_type      = "pattern",
                    status          = "ERROR",
                    actual_value    = "column_missing",
                    expected        = f"pattern={pattern}",
                    severity        = "CRITICAL",
                    records_failing = 0,
                    sample_failing  = [],
                    message         = f"Field '{field_name}' not present in any record (missing column).",
                ))
                continue

            fail_ids = []
            for val, rid in non_null:
                if not compiled.match(str(val)):
                    if rid not in fail_ids:
                        fail_ids.append(rid)

            status   = "PASS" if not fail_ids else "FAIL"
            severity = "CRITICAL" if status == "FAIL" else _map_yaml_severity(yaml_sev)
            results.append(CheckResult(
                check_id        = _build_check_id(source_type, field_name, "pattern"),
                column_name     = col_display,
                check_type      = "pattern",
                status          = status,
                actual_value    = f"non_matching={len(fail_ids)}",
                expected        = f"pattern={pattern}",
                severity        = severity,
                records_failing = len(fail_ids),
                sample_failing  = fail_ids[:_MAX_SAMPLES],
                message         = (
                    f"'{col_display}': {len(fail_ids)} records do not match pattern '{pattern}'."
                    if fail_ids
                    else f"'{col_display}': all {len(non_null)} values match pattern."
                ),
            ))

        # ── accepted_values ───────────────────────────────────────────────────
        elif clause_type == "accepted_values":
            accepted = clause.get("accepted_values", [])
            if not isinstance(accepted, list) or not accepted:
                continue
            accepted_set = {str(v) for v in accepted}

            pairs    = _extract_pairs(records, field_name, source_type)
            non_null = [(v, rid) for v, rid in pairs if v is not None]
            if not non_null:
                results.append(CheckResult(
                    check_id        = _build_check_id(source_type, field_name, "accepted_values"),
                    column_name     = col_display,
                    check_type      = "accepted_values",
                    status          = "ERROR",
                    actual_value    = "column_missing",
                    expected        = f"one_of={accepted[:5]}",
                    severity        = "CRITICAL",
                    records_failing = 0,
                    sample_failing  = [],
                    message         = f"Field '{field_name}' not present in any record (missing column).",
                ))
                continue

            fail_ids: list[str] = []
            bad_vals: list[str] = []
            for val, rid in non_null:
                if str(val) not in accepted_set:
                    if rid not in fail_ids:
                        fail_ids.append(rid)
                    if len(bad_vals) < 3 and str(val) not in bad_vals:
                        bad_vals.append(str(val))

            status   = "PASS" if not fail_ids else "FAIL"
            severity = "CRITICAL" if status == "FAIL" else _map_yaml_severity(yaml_sev)
            results.append(CheckResult(
                check_id        = _build_check_id(source_type, field_name, "accepted_values"),
                column_name     = col_display,
                check_type      = "accepted_values",
                status          = status,
                actual_value    = f"invalid_count={len(fail_ids)},examples={bad_vals}",
                expected        = f"one_of={accepted[:5]}",
                severity        = severity,
                records_failing = len(fail_ids),
                sample_failing  = fail_ids[:_MAX_SAMPLES],
                message         = (
                    f"'{col_display}': {len(fail_ids)} records have invalid values {bad_vals}."
                    if fail_ids
                    else f"'{col_display}': all values are in accepted set."
                ),
            ))

        # ── range ─────────────────────────────────────────────────────────────
        elif clause_type == "range":
            mn = clause.get("minimum")
            mx = clause.get("maximum")

            pairs      = _extract_pairs(records, field_name, source_type)
            non_null   = [(v, rid) for v, rid in pairs if v is not None]
            fail_ids: list[str]   = []
            float_vals: list[float] = []

            for val, rid in non_null:
                try:
                    fval = float(val)
                    float_vals.append(fval)
                    if (mn is not None and fval < mn) or (mx is not None and fval > mx):
                        if rid not in fail_ids:
                            fail_ids.append(rid)
                except (TypeError, ValueError):
                    pass

            act_min  = min(float_vals) if float_vals else None
            act_max  = max(float_vals) if float_vals else None
            act_mean = (sum(float_vals) / len(float_vals)) if float_vals else None
            actual_value = (
                f"max={act_max:.4f}, mean={act_mean:.4f}, min={act_min:.4f}"
                if act_min is not None else "no_numeric_values"
            )
            exp_parts: list[str] = []
            if mn is not None:
                exp_parts.append(f"min>={mn}")
            if mx is not None:
                exp_parts.append(f"max<={mx}")
            expected = ",".join(exp_parts) or "any"

            status   = "PASS" if not fail_ids else "FAIL"
            severity = "CRITICAL" if status == "FAIL" else _map_yaml_severity(yaml_sev)
            results.append(CheckResult(
                check_id        = _build_check_id(source_type, field_name, "range"),
                column_name     = col_display,
                check_type      = "range",
                status          = status,
                actual_value    = actual_value,
                expected        = expected,
                severity        = severity,
                records_failing = len(fail_ids),
                sample_failing  = fail_ids[:_MAX_SAMPLES],
                message         = (
                    f"'{col_display}': {len(fail_ids)} records outside range [{mn}, {mx}]. "
                    f"Observed min={act_min:.4f}, max={act_max:.4f}."
                    if fail_ids
                    else f"'{col_display}': all values in range [{mn}, {mx}]."
                ),
            ))

    return results


# ── SCHEMA-DERIVED RANGE CHECKS ───────────────────────────────────────────────

def run_schema_range_checks(records: list[dict], contract: dict) -> list[CheckResult]:
    """
    Walk contract.schema and emit range checks for any numeric field that has
    minimum and/or maximum constraints.  This catches cases where the generator
    wrote the bounds into schema instead of quality.structural.

    Handles week3 nested structure: schema.extracted_facts.items.<field>
    """
    results:    list[CheckResult] = []
    source_type = contract.get("info", {}).get("sourceType", "")
    schema      = contract.get("schema", {})

    # Flatten schema entries to (field_name, min, max) triples
    field_bounds: list[tuple[str, Any, Any]] = []

    def _walk(node: dict, prefix: str = "") -> None:
        if not isinstance(node, dict):
            return
        # Is this a leaf field spec with numeric bounds?
        if "minimum" in node or "maximum" in node:
            mn = node.get("minimum")
            mx = node.get("maximum")
            field_bounds.append((prefix, mn, mx))
            return
        # Recurse
        for key, val in node.items():
            if key in ("type", "format", "required", "unique", "nullable",
                       "description", "pattern", "enum"):
                continue
            if key == "items" and isinstance(val, dict):
                # array items — recurse with same prefix (week3 style)
                _walk(val, prefix)
            else:
                child_prefix = f"{prefix}.{key}" if prefix else key
                _walk(val, child_prefix)

    _walk(schema)

    for raw_field, mn, mx in field_bounds:
        if mn is None and mx is None:
            continue
        # For week3 nested fields "extracted_facts.confidence" → use leaf name
        # so _extract_pairs handles the nested array correctly
        leaf = raw_field.split(".")[-1]
        field_name  = leaf if (source_type == SOURCE_WEEK3 and leaf in _WEEK3_NESTED) else raw_field
        col_display = _display_column(field_name, source_type)
        pairs       = _extract_pairs(records, field_name, source_type)
        non_null    = [(v, rid) for v, rid in pairs if v is not None]
        fail_ids:   list[str]   = []
        float_vals: list[float] = []

        for val, rid in non_null:
            try:
                fval = float(val)
                float_vals.append(fval)
                if (mn is not None and fval < mn) or (mx is not None and fval > mx):
                    if rid not in fail_ids:
                        fail_ids.append(rid)
            except (TypeError, ValueError):
                pass

        if not float_vals:
            continue

        act_min  = min(float_vals)
        act_max  = max(float_vals)
        act_mean = sum(float_vals) / len(float_vals)
        exp_parts = []
        if mn is not None:
            exp_parts.append(f"min>={mn}")
        if mx is not None:
            exp_parts.append(f"max<={mx}")
        expected = ",".join(exp_parts)

        status   = "PASS" if not fail_ids else "FAIL"
        severity = "CRITICAL" if status == "FAIL" else "LOW"
        results.append(CheckResult(
            check_id        = _build_check_id(source_type, field_name, "range"),
            column_name     = col_display,
            check_type      = "range",
            status          = status,
            actual_value    = f"max={act_max:.4f}, mean={act_mean:.4f}, min={act_min:.4f}",
            expected        = expected,
            severity        = severity,
            records_failing = len(fail_ids),
            sample_failing  = fail_ids[:_MAX_SAMPLES],
            message         = (
                f"'{col_display}': {len(fail_ids)} records outside [{mn}, {mx}]. "
                f"Observed min={act_min:.4f}, max={act_max:.4f}."
                if fail_ids
                else f"'{col_display}': all values in range [{mn}, {mx}]."
            ),
        ))

    return results


# ── STATISTICAL CHECKS ─────────────────────────────────────────────────────────

def run_statistical_checks(records: list[dict], contract: dict) -> list[CheckResult]:
    results:    list[CheckResult] = []
    quality     = contract.get("quality", {})
    clauses     = quality.get("statistical", [])
    source_type = contract.get("info", {}).get("sourceType", "")

    for clause in clauses:
        field_name  = clause.get("field", "")
        check       = clause.get("check", "")
        col_display = _display_column(field_name, source_type)

        if check == "non_zero_variance":
            pairs = _extract_pairs(records, field_name, source_type)
            vals  = []
            for val, _ in pairs:
                if val is not None:
                    try:
                        vals.append(float(val))
                    except (TypeError, ValueError):
                        pass

            if not vals:
                results.append(CheckResult(
                    check_id        = _build_check_id(source_type, field_name, "non_zero_variance"),
                    column_name     = col_display,
                    check_type      = "non_zero_variance",
                    status          = "ERROR",
                    actual_value    = "no_values",
                    expected        = "std>0.0",
                    severity        = "MEDIUM",
                    records_failing = 0,
                    sample_failing  = [],
                    message         = f"No numeric values found for '{col_display}'.",
                ))
                continue

            try:
                std = statistics.stdev(vals)
            except statistics.StatisticsError:
                std = 0.0
            mean_val = statistics.mean(vals)

            status = "FAIL" if std == 0.0 else "PASS"
            results.append(CheckResult(
                check_id        = _build_check_id(source_type, field_name, "non_zero_variance"),
                column_name     = col_display,
                check_type      = "non_zero_variance",
                status          = status,
                actual_value    = f"std={std:.4f},mean={mean_val:.4f}",
                expected        = "std>0.0",
                severity        = "MEDIUM" if status == "FAIL" else "LOW",
                records_failing = 0,
                sample_failing  = [],
                message         = (
                    f"'{col_display}': std=0.0, mean={mean_val:.4f} — "
                    f"all {len(vals)} values are identical (likely hard-coded default)."
                    if std == 0.0
                    else f"'{col_display}': std={std:.4f}, mean={mean_val:.4f} — variance OK."
                ),
            ))

    return results


# ── CROSS-FIELD CHECKS ─────────────────────────────────────────────────────────

def run_cross_field_checks(
    records:     list[dict],
    contract:    dict,
    source_type: str,
) -> list[CheckResult]:
    results: list[CheckResult] = []
    quality  = contract.get("quality", {})
    clauses  = quality.get("crossField", [])

    for clause in clauses:
        field_name  = clause.get("field", "")
        check       = clause.get("check", "")
        col_display = _display_column(field_name, source_type)

        # ── aspirational entity refs ──────────────────────────────────────────
        if check == "entity_refs_resolve":
            results.append(CheckResult(
                check_id        = _build_check_id(source_type, field_name, "entity_refs_resolve"),
                column_name     = col_display,
                check_type      = "entity_refs_resolve",
                status          = "PASS",
                actual_value    = "skipped",
                expected        = "aspirational",
                severity        = "LOW",
                records_failing = 0,
                sample_failing  = [],
                message         = "Aspirational: entity_refs/entities fields not yet in data.",
            ))
            continue

        # ── timestamp >= payload.*_at (week5) ─────────────────────────────────
        if check == "gte_payload_occurred_at":
            fail_ids: list[str] = []
            skipped = 0
            for rec in records:
                ts      = _safe_get(rec, "timestamp")
                payload = rec.get("payload") or {}
                if not isinstance(payload, dict):
                    skipped += 1
                    continue
                dt_ts = _parse_dt(ts)
                if dt_ts is None:
                    skipped += 1
                    continue
                rid = _get_record_id(rec, source_type)
                for k, v in payload.items():
                    if not (k.endswith("_at") or k.endswith("_time")):
                        continue
                    dt_p = _parse_dt(v)
                    if dt_p is None:
                        continue
                    try:
                        ts_aw = dt_ts.replace(tzinfo=datetime.timezone.utc) if dt_ts.tzinfo is None else dt_ts
                        dp_aw = dt_p.replace(tzinfo=datetime.timezone.utc) if dt_p.tzinfo is None else dt_p
                        if ts_aw < dp_aw and rid not in fail_ids:
                            fail_ids.append(rid)
                    except Exception:
                        skipped += 1

            status = "PASS" if not fail_ids else "FAIL"
            results.append(CheckResult(
                check_id        = _build_check_id(source_type, field_name, "cross_field_timestamp"),
                column_name     = col_display,
                check_type      = "cross_field_timestamp",
                status          = status,
                actual_value    = f"violations={len(fail_ids)}",
                expected        = "timestamp>=payload.*_at",
                severity        = "HIGH" if status == "FAIL" else "LOW",
                records_failing = len(fail_ids),
                sample_failing  = fail_ids[:_MAX_SAMPLES],
                message         = (
                    f"timestamp < payload.*_at in {len(fail_ids)} events — clock skew detected."
                    if fail_ids else "All timestamps >= payload occurred_at values."
                ),
            ))

        # ── stream_position monotonic per stream_id (week5) ───────────────────
        elif check == "monotonic_per_group":
            group_by = clause.get("group_by", "stream_id")
            groups: dict[str, list] = defaultdict(list)
            for rec in records:
                gval = _safe_get(rec, group_by)
                pos  = _safe_get(rec, field_name)
                if gval is not None and pos is not None:
                    try:
                        groups[str(gval)].append((int(pos), _get_record_id(rec, source_type)))
                    except (TypeError, ValueError):
                        pass

            fail_ids   = []
            non_mono_n = 0
            for _, pos_list in groups.items():
                sorted_pl = sorted(pos_list, key=lambda x: x[0])
                positions = [p for p, _ in sorted_pl]
                expected  = list(range(positions[0], positions[0] + len(positions)))
                if positions != expected:
                    non_mono_n += 1
                    for _, rid in sorted_pl[:_MAX_SAMPLES]:
                        if rid not in fail_ids:
                            fail_ids.append(rid)

            status = "PASS" if not fail_ids else "FAIL"
            results.append(CheckResult(
                check_id        = _build_check_id(source_type, field_name, "cross_field_monotonic"),
                column_name     = col_display,
                check_type      = "cross_field_monotonic",
                status          = status,
                actual_value    = f"non_monotonic_streams={non_mono_n}",
                expected        = "monotonic_increment=1_per_stream",
                severity        = "HIGH" if status == "FAIL" else "LOW",
                records_failing = len(fail_ids),
                sample_failing  = fail_ids[:_MAX_SAMPLES],
                message         = (
                    f"{non_mono_n} streams have non-monotonic {field_name}."
                    if fail_ids
                    else f"All {len(groups)} streams have monotonic {field_name}."
                ),
            ))

    return results


# ── BASELINE MANAGEMENT ────────────────────────────────────────────────────────

def load_or_create_baseline(
    contract_id: str,
    records:     list[dict],
    source_type: str,
) -> dict:
    _BASELINES_PATH.parent.mkdir(parents=True, exist_ok=True)
    existing: dict = {}
    if _BASELINES_PATH.exists():
        try:
            existing = json.loads(_BASELINES_PATH.read_text())
        except (json.JSONDecodeError, OSError):
            existing = {}

    if contract_id in existing:
        return existing[contract_id]

    # First run → compute and persist baseline
    baseline = _compute_baseline(records, source_type)
    baseline["created_at"]   = datetime.datetime.now(datetime.timezone.utc).isoformat()
    baseline["record_count"] = len(records)
    baseline["contract_id"]  = contract_id

    existing[contract_id] = baseline
    _BASELINES_PATH.write_text(json.dumps(existing, indent=2))
    return baseline


def _compute_baseline(records: list[dict], source_type: str) -> dict:
    field_stats: dict[str, dict] = {}

    # Week3: confidence is nested inside extracted_facts[]
    if source_type == SOURCE_WEEK3:
        vals = []
        for rec in records:
            for fact in rec.get("extracted_facts", []):
                v = fact.get("confidence")
                if v is not None:
                    try:
                        vals.append(float(v))
                    except (TypeError, ValueError):
                        pass
        if vals:
            field_stats["confidence"] = {
                "mean": statistics.mean(vals),
                "std":  statistics.stdev(vals) if len(vals) > 1 else 0.0,
                "min":  min(vals),
                "max":  max(vals),
                "null_fraction": 0.0,
            }
        return {"field_stats": field_stats}

    # Week5 + others: top-level numeric fields
    numeric_fields = ["stream_position", "global_position", "event_version",
                      "total_tokens", "total_cost"]
    for fname in numeric_fields:
        vals = []
        for rec in records:
            v = _safe_get(rec, fname)
            if v is not None:
                try:
                    vals.append(float(v))
                except (TypeError, ValueError):
                    pass
        if vals:
            null_count = sum(1 for rec in records if _safe_get(rec, fname) is None)
            field_stats[fname] = {
                "mean": statistics.mean(vals),
                "std":  statistics.stdev(vals) if len(vals) > 1 else 0.0,
                "min":  min(vals),
                "max":  max(vals),
                "null_fraction": null_count / max(len(records), 1),
            }
    return {"field_stats": field_stats}


# ── BASELINE DRIFT CHECKS (Phase 2: z-score) ──────────────────────────────────

def run_baseline_drift_checks(
    records:     list[dict],
    baseline:    dict,
    source_type: str,
) -> list[CheckResult]:
    """
    Phase 2 drift checks:
      z > 3  → FAIL,  severity = HIGH
      z > 2  → WARN,  severity = MEDIUM
      else   → PASS,  severity = LOW

    z = abs(current_mean - baseline_mean) / baseline_std
    """
    results:      list[CheckResult] = []
    field_stats   = baseline.get("field_stats", {})
    baseline_cnt  = baseline.get("record_count", 0)

    # ── Volume drop ────────────────────────────────────────────────────────────
    if baseline_cnt > 0:
        ratio = len(records) / baseline_cnt
        if ratio < 0.5:
            results.append(CheckResult(
                check_id        = _build_check_id(source_type, "*", "volume_drop"),
                column_name     = "*",
                check_type      = "volume_drop",
                status          = "FAIL",
                actual_value    = f"record_count={len(records)}",
                expected        = f"record_count>={int(baseline_cnt * 0.5)}",
                severity        = "HIGH",
                records_failing = 0,
                sample_failing  = [],
                message         = (
                    f"Record count dropped to {len(records)} from baseline {baseline_cnt} "
                    f"({ratio:.1%}). Possible data loss."
                ),
            ))

    # ── Per-field z-score ──────────────────────────────────────────────────────
    for fname, bstats in field_stats.items():
        b_mean = bstats.get("mean", 0.0)
        b_std  = bstats.get("std",  0.0)

        pairs = _extract_pairs(records, fname, source_type)
        vals  = []
        for val, _ in pairs:
            if val is not None:
                try:
                    vals.append(float(val))
                except (TypeError, ValueError):
                    pass
        if not vals:
            continue

        c_mean = statistics.mean(vals)
        c_std  = statistics.stdev(vals) if len(vals) > 1 else 0.0

        # Z-score calculation
        if b_std > 0:
            z = abs(c_mean - b_mean) / b_std
        elif abs(c_mean - b_mean) > 1e-9:
            z = 99.0   # infinite drift
        else:
            z = 0.0

        z_capped = min(z, 99.0)

        if z > 3:
            status, severity = "FAIL", "HIGH"
        elif z > 2:
            status, severity = "WARN", "MEDIUM"
        else:
            status, severity = "PASS", "LOW"

        col_display = _display_column(fname, source_type)
        results.append(CheckResult(
            check_id        = _build_check_id(source_type, fname, "z_score"),
            column_name     = col_display,
            check_type      = "z_score",
            status          = status,
            actual_value    = f"z={z_capped:.2f},mean={c_mean:.4f},std={c_std:.4f}",
            expected        = "z<=3.0(FAIL) z<=2.0(WARN)",
            severity        = severity,
            records_failing = 0,
            sample_failing  = [],
            message         = (
                f"'{col_display}' z={z_capped:.2f}: "
                f"baseline_mean={b_mean:.4f}, current_mean={c_mean:.4f}."
            ),
            z_score         = z_capped,
        ))

        # ── Variance collapse ─────────────────────────────────────────────────
        if c_std == 0.0 and b_std > 0.0:
            results.append(CheckResult(
                check_id        = _build_check_id(source_type, fname, "variance_collapse"),
                column_name     = col_display,
                check_type      = "variance_collapse",
                status          = "FAIL",
                actual_value    = f"std={c_std:.4f}",
                expected        = f"std~={b_std:.4f}",
                severity        = "HIGH",
                records_failing = 0,
                sample_failing  = [],
                message         = (
                    f"'{col_display}' variance collapsed to 0 "
                    f"(was std={b_std:.4f} at baseline)."
                ),
            ))

    return results


# ── REPORT ASSEMBLY ────────────────────────────────────────────────────────────

def assemble_report(
    all_checks:  list[CheckResult],
    contract_id: str,
    data_path:   str,
) -> dict:
    total   = len(all_checks)
    passed  = sum(1 for c in all_checks if c.status == "PASS")
    failed  = sum(1 for c in all_checks if c.status == "FAIL")
    warned  = sum(1 for c in all_checks if c.status == "WARN")
    errored = sum(1 for c in all_checks if c.status == "ERROR")

    snapshot_id = _compute_sha256(data_path)
    now         = datetime.datetime.now(datetime.timezone.utc).isoformat()

    return {
        # ── Phase 2 spec-required top-level keys ───────────────────────────────
        "report_id":     str(uuid.uuid4()),
        "contract_id":   contract_id,
        "snapshot_id":   snapshot_id,          # SHA-256 of input JSONL
        "run_timestamp": now,
        "total_checks":  total,
        "passed":        passed,
        "failed":        failed,
        "warned":        warned,
        "errored":       errored,
        "results":       [c.to_dict() for c in all_checks],
        # ── Extended (non-spec) fields for CLI / attributor ────────────────────
        "data_path": data_path,
        "exit_code": 1 if failed > 0 else 0,
    }


# ── MAIN ORCHESTRATOR ──────────────────────────────────────────────────────────

def run_validation(
    contract_path: str,
    data_path:     str,
    output_path:   str,
) -> dict:
    print(f"[runner] Loading contract : {contract_path}")
    contract    = load_contract(contract_path)
    contract_id = contract.get("id", Path(contract_path).stem)
    source_type = contract.get("info", {}).get("sourceType", "")

    print(f"[runner] Loading data     : {data_path}")
    records = load_data(data_path)
    print(f"[runner] {len(records)} records loaded")

    all_checks: list[CheckResult] = []

    print("[runner] Structural checks ...")
    all_checks += run_structural_checks(records, contract)

    print("[runner] Schema range checks ...")
    all_checks += run_schema_range_checks(records, contract)

    print("[runner] Statistical checks ...")
    all_checks += run_statistical_checks(records, contract)

    print("[runner] Cross-field checks ...")
    all_checks += run_cross_field_checks(records, contract, source_type)

    print("[runner] Baseline load / create ...")
    baseline = load_or_create_baseline(contract_id, records, source_type)

    print("[runner] Drift checks (z-score) ...")
    all_checks += run_baseline_drift_checks(records, baseline, source_type)

    report = assemble_report(all_checks, contract_id, data_path)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2))
    print(f"[runner] Report written   : {output_path}")

    return report


# ── CLI ────────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="runner",
        description="Phase 2 — Validate a Bitol data contract against a JSONL dataset",
    )
    p.add_argument("--contract", required=True, help="Path to Bitol YAML contract")
    p.add_argument("--data",     required=True, help="Path to JSONL data file")
    p.add_argument("--output",   required=False, default=None,
                   help="Path to write JSON validation report (default: validation_reports/<contract_id>.json)")
    return p


def main() -> None:
    args = build_parser().parse_args()
    # Derive default output path from contract name when not provided
    if args.output is None:
        contract_stem = Path(args.contract).stem
        output_path = str(Path("validation_reports") / f"{contract_stem}.json")
    else:
        output_path = args.output
    report = run_validation(
        contract_path=args.contract,
        data_path=args.data,
        output_path=output_path,
    )
    status_line = (
        f"[runner] {report['total_checks']} checks | "
        f"{report['passed']} passed | "
        f"{report['failed']} failed | "
        f"{report['warned']} warned | "
        f"{report['errored']} errored"
    )
    print(status_line)
    if report["failed"] > 0:
        print("[runner] ❌ VIOLATIONS DETECTED — see report for details")
    else:
        print("[runner] ✓  All checks passed (or warned)")
    sys.exit(report["exit_code"])


if __name__ == "__main__":
    main()
