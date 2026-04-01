"""
contracts/runner.py
Phase 1 — Bitol v3 Data Contract Validation Runner

CLI usage:
    python contracts/runner.py \\
        --contract generated_contracts/week3-extractions.yaml \\
        --data outputs/week3/extractions.jsonl \\
        --output validation_reports/week3-extractions.json

Never crashes. Missing columns become ERROR results, not exceptions.
Writes baselines on first run to schema_snapshots/baselines.json.
"""

from __future__ import annotations

import argparse
import datetime
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml

# ── sys.path guard (same as generator.py) ─────────────────────────────────────
_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from contracts._clauses import SOURCE_WEEK3, SOURCE_WEEK5  # noqa: E402

# ── Patterns ──────────────────────────────────────────────────────────────────
_UUID_RE   = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)
_HEX64_RE  = re.compile(r"^[0-9a-f]{64}$")
_PASCAL_RE = re.compile(r"^[A-Z][a-zA-Z0-9]+$")

_BASELINES_PATH = Path("schema_snapshots") / "baselines.json"
_MAX_SAMPLES = 5


# ── CheckResult ────────────────────────────────────────────────────────────────
@dataclass
class CheckResult:
    check_id:          str
    field:             str
    rule:              str
    status:            str        # PASS | FAIL | ERROR | SKIP
    severity:          str        # BREAKING | ERROR | WARNING | INFO
    message:           str
    failed_count:      int = 0
    total_count:       int = 0
    sample_violations: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "check_id":          self.check_id,
            "field":             self.field,
            "rule":              self.rule,
            "status":            self.status,
            "severity":          self.severity,
            "message":           self.message,
            "failed_count":      self.failed_count,
            "total_count":       self.total_count,
            "sample_violations": self.sample_violations,
        }


# ── Helpers ────────────────────────────────────────────────────────────────────
def _safe_get(record: dict, field_path: str, default: Any = None) -> Any:
    """Dot-path accessor. Returns default if any segment is missing."""
    parts = field_path.split(".")
    val = record
    for part in parts:
        if not isinstance(val, dict) or part not in val:
            return default
        val = val[part]
    return val


def _parse_dt(s: Any) -> Optional[datetime.datetime]:
    """Parse ISO-8601 string to aware datetime. stdlib only, no dateutil."""
    if not isinstance(s, str):
        return None
    s = s.strip().replace(" ", "T")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.datetime.fromisoformat(s)
    except ValueError:
        return None


def _flatten_facts_confidence(records: list[dict]) -> list[float]:
    """Extract all confidence values from week3 extracted_facts[]."""
    vals = []
    for rec in records:
        for fact in rec.get("extracted_facts", []):
            v = fact.get("confidence")
            if v is not None:
                try:
                    vals.append(float(v))
                except (TypeError, ValueError):
                    pass
    return vals


def _get_values_for_field(records: list[dict], field_name: str, source_type: str) -> list:
    """
    Extract all values for a field, handling week3 nested extracted_facts[].
    For week3 fields 'fact' and 'confidence', iterates into extracted_facts[].
    """
    if source_type == SOURCE_WEEK3 and field_name in ("fact", "confidence"):
        vals = []
        for rec in records:
            for fact in rec.get("extracted_facts", []):
                vals.append(fact.get(field_name))
        return vals
    return [_safe_get(rec, field_name) for rec in records]


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


# ── Structural checks ──────────────────────────────────────────────────────────
def run_structural_checks(records: list[dict], contract: dict) -> list[CheckResult]:
    results = []
    quality = contract.get("quality", {})
    clauses = quality.get("structural", [])
    source_type = contract.get("info", {}).get("sourceType", "")

    for clause in clauses:
        field_name  = clause.get("field", "")
        rule        = clause.get("rule", "")
        clause_type = clause.get("type", "")
        check       = clause.get("check", "")
        severity    = clause.get("severity", "ERROR")
        check_id    = f"structural.{rule}"

        if clause.get("status") == "aspirational":
            results.append(CheckResult(
                check_id=check_id, field=field_name, rule=rule,
                status="SKIP", severity=severity,
                message=f"Aspirational clause — field '{field_name}' not yet in data schema.",
                total_count=len(records),
            ))
            continue

        # ── not_null ──
        if clause_type == "structural" and check == "not_null":
            values = _get_values_for_field(records, field_name, source_type)
            missing = 0
            violations = []
            for i, val in enumerate(values):
                if val is None or val == "":
                    missing += 1
                    if len(violations) < _MAX_SAMPLES:
                        violations.append({"record_index": i, "value": val})
            status = "PASS" if missing == 0 else "FAIL"
            results.append(CheckResult(
                check_id=check_id, field=field_name, rule=rule,
                status=status, severity=severity,
                message=(f"'{field_name}': {missing}/{len(values)} null/missing values."
                         if missing else f"'{field_name}': no null values ({len(values)} checked)."),
                failed_count=missing, total_count=len(values),
                sample_violations=violations,
            ))

        # ── pattern ──
        elif clause_type == "pattern":
            pattern = clause.get("pattern", "")
            try:
                compiled = re.compile(pattern)
            except re.error as e:
                results.append(CheckResult(
                    check_id=check_id, field=field_name, rule=rule,
                    status="ERROR", severity=severity,
                    message=f"Invalid regex pattern '{pattern}': {e}",
                    total_count=len(records),
                ))
                continue

            failed, violations = 0, []
            field_missing = 0
            for i, rec in enumerate(records):
                val = _safe_get(rec, field_name)
                if val is None:
                    field_missing += 1
                    continue
                if not compiled.match(str(val)):
                    failed += 1
                    if len(violations) < _MAX_SAMPLES:
                        violations.append({"record_index": i, "value": str(val)[:80]})

            if field_missing == len(records):
                results.append(CheckResult(
                    check_id=check_id, field=field_name, rule=rule,
                    status="ERROR", severity=severity,
                    message=f"Field '{field_name}' not present in any record (missing column).",
                    total_count=len(records),
                ))
            else:
                status = "PASS" if failed == 0 else "FAIL"
                results.append(CheckResult(
                    check_id=check_id, field=field_name, rule=rule,
                    status=status, severity=severity,
                    message=(f"'{field_name}': {failed} values do not match pattern '{pattern}'."
                             if failed else f"'{field_name}': all values match pattern."),
                    failed_count=failed, total_count=len(records) - field_missing,
                    sample_violations=violations,
                ))

        # ── accepted_values ──
        elif clause_type == "accepted_values":
            accepted = clause.get("accepted_values", [])
            if not isinstance(accepted, list) or not accepted:
                continue
            accepted_set = set(str(v) for v in accepted)
            failed, violations = 0, []
            field_missing = 0
            for i, rec in enumerate(records):
                val = _safe_get(rec, field_name)
                if val is None:
                    field_missing += 1
                    continue
                if str(val) not in accepted_set:
                    failed += 1
                    if len(violations) < _MAX_SAMPLES:
                        violations.append({"record_index": i, "value": str(val)})
            if field_missing == len(records):
                results.append(CheckResult(
                    check_id=check_id, field=field_name, rule=rule,
                    status="ERROR", severity=severity,
                    message=f"Field '{field_name}' not present in any record (missing column).",
                    total_count=len(records),
                ))
            else:
                status = "PASS" if failed == 0 else "FAIL"
                results.append(CheckResult(
                    check_id=check_id, field=field_name, rule=rule,
                    status=status, severity=severity,
                    message=(f"'{field_name}': {failed} values not in accepted set."
                             if failed else f"'{field_name}': all values accepted."),
                    failed_count=failed, total_count=len(records) - field_missing,
                    sample_violations=violations,
                ))

        # ── range ──
        elif clause_type == "range":
            mn = clause.get("minimum")
            mx = clause.get("maximum")
            failed, violations = 0, []
            for i, rec in enumerate(records):
                val = _safe_get(rec, field_name)
                if val is None:
                    continue
                try:
                    fval = float(val)
                except (TypeError, ValueError):
                    continue
                if (mn is not None and fval < mn) or (mx is not None and fval > mx):
                    failed += 1
                    if len(violations) < _MAX_SAMPLES:
                        violations.append({"record_index": i, "value": fval})
            status = "PASS" if failed == 0 else "FAIL"
            results.append(CheckResult(
                check_id=check_id, field=field_name, rule=rule,
                status=status, severity=severity,
                message=(f"'{field_name}': {failed} values outside [{mn}, {mx}]."
                         if failed else f"'{field_name}': all values in range [{mn}, {mx}]."),
                failed_count=failed, total_count=len(records),
                sample_violations=violations,
            ))

    return results


# ── Statistical checks ─────────────────────────────────────────────────────────
def run_statistical_checks(records: list[dict], contract: dict) -> list[CheckResult]:
    results = []
    quality = contract.get("quality", {})
    clauses = quality.get("statistical", [])
    source_type = contract.get("info", {}).get("sourceType", "")

    for clause in clauses:
        field_name = clause.get("field", "")
        rule       = clause.get("rule", "")
        check      = clause.get("check", "")
        severity   = clause.get("severity", "WARNING")
        check_id   = f"statistical.{rule}"

        if check == "non_zero_variance":
            # Special handling for week3: confidence is nested
            if source_type == SOURCE_WEEK3 and field_name == "confidence":
                vals = _flatten_facts_confidence(records)
            else:
                vals = []
                for rec in records:
                    v = _safe_get(rec, field_name)
                    if v is not None:
                        try:
                            vals.append(float(v))
                        except (TypeError, ValueError):
                            pass

            if not vals:
                results.append(CheckResult(
                    check_id=check_id, field=field_name, rule=rule,
                    status="SKIP", severity=severity,
                    message=f"No numeric values found for '{field_name}'.",
                ))
                continue

            import statistics
            try:
                std = statistics.stdev(vals)
            except statistics.StatisticsError:
                std = 0.0

            mean_val = statistics.mean(vals)
            status = "FAIL" if std == 0.0 else "PASS"
            results.append(CheckResult(
                check_id=check_id, field=field_name, rule=rule,
                status=status, severity=severity,
                message=(
                    f"'{field_name}': std=0.0, mean={mean_val:.4f} — "
                    f"all {len(vals)} values are identical. "
                    f"Likely hard-coded default value."
                    if std == 0.0
                    else f"'{field_name}': std={std:.4f}, mean={mean_val:.4f} — variance OK."
                ),
                total_count=len(vals),
            ))

    return results


# ── Cross-field checks ─────────────────────────────────────────────────────────
def run_cross_field_checks(
    records: list[dict],
    contract: dict,
    source_type: str,
) -> list[CheckResult]:
    results = []
    quality = contract.get("quality", {})
    clauses = quality.get("crossField", [])

    for clause in clauses:
        field_name = clause.get("field", "")
        rule       = clause.get("rule", "")
        check      = clause.get("check", "")
        severity   = clause.get("severity", "ERROR")
        check_id   = f"cross_field.{rule}"

        # ── Week3: entity_refs resolve (aspirational — skip gracefully) ──
        if check == "entity_refs_resolve":
            results.append(CheckResult(
                check_id=check_id, field=field_name, rule=rule,
                status="SKIP", severity=severity,
                message="Aspirational spec: 'entity_refs' and 'entities' fields not yet in data.",
                total_count=len(records),
            ))
            continue

        # ── Week5: timestamp >= payload.*_at ──
        if check == "gte_payload_occurred_at":
            failed, violations = 0, []
            skipped = 0
            for i, rec in enumerate(records):
                ts = _safe_get(rec, "timestamp")
                payload = rec.get("payload") or {}
                if not isinstance(payload, dict):
                    skipped += 1
                    continue
                dt_ts = _parse_dt(ts)
                if dt_ts is None:
                    skipped += 1
                    continue
                # Find all payload fields ending in _at or _time
                for k, v in payload.items():
                    if not (k.endswith("_at") or k.endswith("_time")):
                        continue
                    dt_payload = _parse_dt(v)
                    if dt_payload is None:
                        continue
                    # Make both offset-aware for comparison
                    try:
                        if dt_ts.tzinfo is None:
                            dt_ts = dt_ts.replace(tzinfo=datetime.timezone.utc)
                        if dt_payload.tzinfo is None:
                            dt_payload = dt_payload.replace(tzinfo=datetime.timezone.utc)
                        if dt_ts < dt_payload:
                            failed += 1
                            if len(violations) < _MAX_SAMPLES:
                                violations.append({
                                    "record_index": i,
                                    "timestamp": ts,
                                    "payload_field": k,
                                    "payload_value": v,
                                })
                    except Exception:
                        skipped += 1

            status = "PASS" if failed == 0 else "FAIL"
            results.append(CheckResult(
                check_id=check_id, field=field_name, rule=rule,
                status=status, severity=severity,
                message=(
                    f"timestamp < payload.*_at in {failed} events — clock skew detected."
                    if failed else "All timestamps >= payload occurred_at values."
                ),
                failed_count=failed, total_count=len(records) - skipped,
                sample_violations=violations,
            ))

        # ── Week5: stream_position monotonic per stream_id ──
        elif check == "monotonic_per_group":
            group_by = clause.get("group_by", "stream_id")
            groups: dict[str, list[tuple[int, int]]] = defaultdict(list)
            for i, rec in enumerate(records):
                gval = _safe_get(rec, group_by)
                pos  = _safe_get(rec, field_name)
                if gval is not None and pos is not None:
                    try:
                        groups[str(gval)].append((int(pos), i))
                    except (TypeError, ValueError):
                        pass

            failed_streams, violations = 0, []
            for stream_id, pos_list in groups.items():
                sorted_pos = sorted(pos_list, key=lambda x: x[0])
                positions  = [p for p, _ in sorted_pos]
                expected   = list(range(positions[0], positions[0] + len(positions)))
                if positions != expected:
                    failed_streams += 1
                    if len(violations) < _MAX_SAMPLES:
                        violations.append({
                            "stream_id": stream_id,
                            "positions": positions,
                            "expected":  expected,
                        })

            total_streams = len(groups)
            status = "PASS" if failed_streams == 0 else "FAIL"
            results.append(CheckResult(
                check_id=check_id, field=field_name, rule=rule,
                status=status, severity=severity,
                message=(
                    f"{failed_streams}/{total_streams} streams have non-monotonic {field_name}."
                    if failed_streams
                    else f"All {total_streams} streams have monotonic {field_name}."
                ),
                failed_count=failed_streams, total_count=total_streams,
                sample_violations=violations,
            ))

    return results


# ── Baseline management ────────────────────────────────────────────────────────
def load_or_create_baseline(
    contract_id: str,
    records: list[dict],
    source_type: str,
) -> dict:
    """Load existing baseline or create one from current data."""
    _BASELINES_PATH.parent.mkdir(parents=True, exist_ok=True)

    existing: dict = {}
    if _BASELINES_PATH.exists():
        try:
            existing = json.loads(_BASELINES_PATH.read_text())
        except (json.JSONDecodeError, OSError):
            existing = {}

    if contract_id in existing:
        return existing[contract_id]

    # First run — compute baseline
    baseline = _compute_baseline(records, source_type)
    baseline["created_at"]    = datetime.datetime.now(datetime.timezone.utc).isoformat()
    baseline["record_count"]  = len(records)
    baseline["contract_id"]   = contract_id

    existing[contract_id] = baseline
    _BASELINES_PATH.write_text(json.dumps(existing, indent=2))
    return baseline


def _compute_baseline(records: list[dict], source_type: str) -> dict:
    """Compute numeric field stats for baseline."""
    import statistics as _stats

    field_stats: dict[str, dict] = {}

    # For week3 include confidence from nested facts
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
                "mean":          _stats.mean(vals),
                "std":           _stats.stdev(vals) if len(vals) > 1 else 0.0,
                "min":           min(vals),
                "max":           max(vals),
                "null_fraction": 0.0,
            }
        return {"field_stats": field_stats}

    # For week5: numeric top-level fields
    numeric_fields = ["stream_position", "global_position", "event_version"]
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
                "mean":          _stats.mean(vals),
                "std":           _stats.stdev(vals) if len(vals) > 1 else 0.0,
                "min":           min(vals),
                "max":           max(vals),
                "null_fraction": null_count / len(records),
            }

    return {"field_stats": field_stats}


# ── Baseline drift checks ──────────────────────────────────────────────────────
def run_baseline_drift_checks(
    records: list[dict],
    baseline: dict,
    contract_id: str,
) -> list[CheckResult]:
    results = []
    field_stats = baseline.get("field_stats", {})
    baseline_count = baseline.get("record_count", 0)

    # Volume drop check
    if baseline_count > 0:
        ratio = len(records) / baseline_count
        if ratio < 0.5:
            results.append(CheckResult(
                check_id="drift.volume_drop",
                field="*",
                rule="volume_not_dropped",
                status="FAIL",
                severity="WARNING",
                message=(
                    f"Record count dropped to {len(records)} from baseline {baseline_count} "
                    f"({ratio:.1%}). Possible data loss."
                ),
                total_count=len(records),
            ))

    # Per-field drift
    import statistics as _stats
    for fname, bstats in field_stats.items():
        b_mean = bstats.get("mean", 0.0)
        b_std  = bstats.get("std", 0.0)

        # Collect current values
        vals = []
        for rec in records:
            v = _safe_get(rec, fname)
            if v is not None:
                try:
                    vals.append(float(v))
                except (TypeError, ValueError):
                    pass

        if not vals:
            continue

        c_mean = _stats.mean(vals)
        c_std  = _stats.stdev(vals) if len(vals) > 1 else 0.0
        check_id = f"drift.{fname}_mean_drift"

        threshold = max(2 * b_std, 0.05)  # at least 5% absolute drift
        drift = abs(c_mean - b_mean)

        if drift > threshold:
            results.append(CheckResult(
                check_id=check_id, field=fname, rule=f"{fname}_mean_drift",
                status="FAIL", severity="WARNING",
                message=(
                    f"'{fname}' mean drifted: baseline={b_mean:.4f}, current={c_mean:.4f}, "
                    f"drift={drift:.4f} > threshold={threshold:.4f}."
                ),
                total_count=len(vals),
            ))
        else:
            results.append(CheckResult(
                check_id=check_id, field=fname, rule=f"{fname}_mean_drift",
                status="PASS", severity="INFO",
                message=f"'{fname}' mean within threshold. baseline={b_mean:.4f}, current={c_mean:.4f}.",
                total_count=len(vals),
            ))

        if c_std == 0.0 and b_std > 0.0:
            results.append(CheckResult(
                check_id=f"drift.{fname}_zero_variance",
                field=fname, rule=f"{fname}_zero_variance",
                status="FAIL", severity="WARNING",
                message=f"'{fname}' variance collapsed to 0 (was std={b_std:.4f} at baseline).",
                total_count=len(vals),
            ))

    return results


# ── Report assembly ────────────────────────────────────────────────────────────
def assemble_report(
    all_checks: list[CheckResult],
    contract_id: str,
    data_path: str,
) -> dict:
    total    = len(all_checks)
    passed   = sum(1 for c in all_checks if c.status == "PASS")
    failed   = sum(1 for c in all_checks if c.status == "FAIL")
    errors   = sum(1 for c in all_checks if c.status == "ERROR")
    skipped  = sum(1 for c in all_checks if c.status == "SKIP")
    breaking = sum(1 for c in all_checks if c.status == "FAIL" and c.severity == "BREAKING")

    return {
        "contract_id":   contract_id,
        "data_path":     data_path,
        "run_timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "summary": {
            "total_checks":       total,
            "passed":             passed,
            "failed":             failed,
            "errors":             errors,
            "skipped":            skipped,
            "breaking_violations": breaking,
        },
        "checks":    [c.to_dict() for c in all_checks],
        "exit_code": 1 if breaking > 0 else 0,
    }


# ── Main orchestrator ──────────────────────────────────────────────────────────
def run_validation(
    contract_path: str,
    data_path: str,
    output_path: str,
) -> dict:
    print(f"[runner] Loading contract: {contract_path}")
    contract = load_contract(contract_path)
    contract_id = contract.get("id", Path(contract_path).stem)
    source_type = contract.get("info", {}).get("sourceType", "")

    print(f"[runner] Loading data: {data_path}")
    records = load_data(data_path)
    print(f"[runner] {len(records)} records loaded")

    all_checks: list[CheckResult] = []

    print("[runner] Running structural checks ...")
    all_checks += run_structural_checks(records, contract)

    print("[runner] Running statistical checks ...")
    all_checks += run_statistical_checks(records, contract)

    print("[runner] Running cross-field checks ...")
    all_checks += run_cross_field_checks(records, contract, source_type)

    print("[runner] Loading / creating baseline ...")
    baseline = load_or_create_baseline(contract_id, records, source_type)

    print("[runner] Running baseline drift checks ...")
    all_checks += run_baseline_drift_checks(records, baseline, contract_id)

    report = assemble_report(all_checks, contract_id, data_path)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2))
    print(f"[runner] Report written: {output_path}")

    return report


# ── CLI ────────────────────────────────────────────────────────────────────────
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="runner",
        description="Run validation checks against a Bitol data contract",
    )
    p.add_argument("--contract", required=True, help="Path to Bitol YAML contract file")
    p.add_argument("--data",     required=True, help="Path to JSONL data file to validate")
    p.add_argument("--output",   required=True, help="Path to write JSON validation report")
    return p


def main() -> None:
    args = build_parser().parse_args()
    report = run_validation(
        contract_path=args.contract,
        data_path=args.data,
        output_path=args.output,
    )
    s = report["summary"]
    print(
        f"[runner] {s['total_checks']} checks | "
        f"{s['passed']} passed | "
        f"{s['failed']} failed | "
        f"{s['breaking_violations']} BREAKING"
    )
    sys.exit(report["exit_code"])


if __name__ == "__main__":
    main()
