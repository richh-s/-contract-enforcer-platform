"""
contracts/report_generator.py
Phase 1 + Phase 4B — Validation report generation and enforcer report.

Phase 1 (used by runner.py):
    build_json_report, build_markdown_report

Phase 4B (CLI):
    python contracts/report_generator.py \\
        --output enforcer_report/report_data.json

    Aggregates ALL real validation data into a single stakeholder report:
      - validation_reports/*.json  (runner outputs)
      - violation_log/violations.jsonl
      - validation_reports/ai_extensions.json
      - validation_reports/schema_evolution_*.json

    Health score formula (spec-required):
        base  = (total_passed / total_checks) * 100
        score = max(0, base - 20 * critical_failures)
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# ── JSON report ────────────────────────────────────────────────────────────────

def build_json_report(
    results: list[Any],
    contract: dict,
    data_path: str,
    output_path: str | None = None,
) -> dict:
    """
    Build a structured JSON validation report.

    Parameters
    ----------
    results : list[CheckResult]
        Output from contracts/runner.py.
    contract : dict
        The full Bitol contract dict.
    data_path : str
        Path to the validated data file.
    output_path : str | None
        If provided, write the report to this path.

    Returns
    -------
    dict
        The report as a plain dict (also written to output_path if given).
    """
    now = datetime.now(timezone.utc).isoformat()
    info = contract.get("info", {})

    passed  = [r for r in results if _status(r) == "PASS"]
    failed  = [r for r in results if _status(r) not in ("PASS", "SKIP")]
    skipped = [r for r in results if _status(r) == "SKIP"]

    overall = "PASS" if not failed else (
        "FAIL" if any(_severity(r) == "BREAKING" for r in failed) else "WARN"
    )

    report = {
        "report_generated_at": now,
        "contract_id":   contract.get("id"),
        "contract_title": info.get("title"),
        "data_path":     data_path,
        "overall_status": overall,
        "summary": {
            "total":   len(results),
            "passed":  len(passed),
            "failed":  len(failed),
            "skipped": len(skipped),
        },
        "checks": [_result_to_dict(r) for r in results],
    }

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(report, f, indent=2)

    return report


# ── Markdown report ────────────────────────────────────────────────────────────

def build_markdown_report(
    results: list[Any],
    contract: dict,
    data_path: str,
    output_path: str | None = None,
) -> str:
    """
    Build a human-readable Markdown validation report.

    Parameters
    ----------
    results : list[CheckResult]
        Output from contracts/runner.py.
    contract : dict
        The full Bitol contract dict.
    data_path : str
        Path to the validated data file.
    output_path : str | None
        If provided, write the Markdown to this path.

    Returns
    -------
    str
        The Markdown string.
    """
    now  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    info = contract.get("info", {})
    title = info.get("title", "Data Contract Validation Report")

    passed  = [r for r in results if _status(r) == "PASS"]
    failed  = [r for r in results if _status(r) not in ("PASS", "SKIP")]
    skipped = [r for r in results if _status(r) == "SKIP"]

    overall = "✅ PASS" if not failed else (
        "❌ FAIL" if any(_severity(r) == "BREAKING" for r in failed) else "⚠️ WARN"
    )

    lines = [
        f"# {title}",
        "",
        f"**Generated:** {now}  ",
        f"**Data file:** `{data_path}`  ",
        f"**Contract:** `{contract.get('id', 'unknown')}`  ",
        f"**Overall:** {overall}",
        "",
        f"| Metric | Count |",
        f"|--------|-------|",
        f"| Total checks | {len(results)} |",
        f"| Passed | {len(passed)} |",
        f"| Failed | {len(failed)} |",
        f"| Skipped | {len(skipped)} |",
        "",
    ]

    if failed:
        lines += ["## Failed Checks", ""]
        for r in failed:
            d = _result_to_dict(r)
            lines += [
                f"### `{d['check_id']}` — {d['severity']}",
                f"- **Field:** `{d['field']}`",
                f"- **Status:** {d['status']}",
                f"- **Message:** {d['message']}",
            ]
            if d.get("failed_count") is not None:
                lines.append(f"- **Failed rows:** {d['failed_count']} / {d['total_count']}")
            if d.get("sample_violations"):
                lines.append(f"- **Samples:** `{d['sample_violations'][:3]}`")
            lines.append("")

    if passed:
        lines += ["## Passed Checks", ""]
        for r in passed:
            d = _result_to_dict(r)
            lines.append(f"- ✅ `{d['check_id']}` ({d['field']})")
        lines.append("")

    if skipped:
        lines += ["## Skipped Checks", ""]
        for r in skipped:
            d = _result_to_dict(r)
            lines.append(f"- ⏭ `{d['check_id']}` — {d['message']}")
        lines.append("")

    md = "\n".join(lines)

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            f.write(md)

    return md


# ── Helpers ────────────────────────────────────────────────────────────────────

def _status(result: Any) -> str:
    if hasattr(result, "status"):
        return result.status
    return result.get("status", "UNKNOWN")


def _severity(result: Any) -> str:
    if hasattr(result, "severity"):
        return result.severity
    return result.get("severity", "")


def _result_to_dict(result: Any) -> dict:
    if hasattr(result, "__dict__"):
        return {
            "check_id":         result.check_id,
            "field":            result.field,
            "rule":             result.rule,
            "status":           result.status,
            "severity":         result.severity,
            "message":          result.message,
            "failed_count":     result.failed_count,
            "total_count":      result.total_count,
            "sample_violations": result.sample_violations,
        }
    return result


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 4B — ENFORCER REPORT GENERATOR
# ══════════════════════════════════════════════════════════════════════════════

_HERE = Path(__file__).resolve().parent.parent

# Severity penalty weights (spec-required)
_SEVERITY_PENALTY = {
    "CRITICAL": 20,
    "HIGH":     10,
    "MEDIUM":    5,
    "LOW":       1,
}

# Map runner report status to readable label
_STATUS_LABEL = {
    "PASS":  "passed",
    "FAIL":  "failed",
    "ERROR": "errored",
    "WARN":  "warned",
}


# ── Data loaders ──────────────────────────────────────────────────────────────

def _load_jsonl(path: Path) -> list[dict]:
    records = []
    if not path.exists():
        return records
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    with open(path) as fh:
        return json.load(fh)


def _load_validation_reports(reports_dir: Path) -> list[dict]:
    """
    Load all runner-generated JSON reports from validation_reports/.
    Excludes ai_extensions.json, schema_evolution_*, migration_impact_*.
    """
    reports = []
    for fpath in sorted(reports_dir.glob("*.json")):
        name = fpath.name
        if any(name.startswith(p) for p in (
            "ai_extensions", "schema_evolution", "migration_impact",
        )):
            continue
        data = _load_json(fpath)
        if "total_checks" in data:   # confirm it's a runner report
            reports.append(data)
    return reports


def _load_schema_evolution_reports(reports_dir: Path) -> list[dict]:
    return [
        _load_json(p)
        for p in sorted(reports_dir.glob("schema_evolution_*.json"))
        if p.exists()
    ]


# ── Health score ──────────────────────────────────────────────────────────────

def compute_health_score(
    reports: list[dict],
    violations: list[dict],
) -> tuple[int, dict]:
    """
    Compute data health score.

    Formula (spec-required):
        base  = (total_passed / total_checks) * 100
        score = max(0, base - 20 * critical_failures)

    Returns (score, breakdown_dict).
    """
    total_checks  = sum(r.get("total_checks", 0) for r in reports)
    total_passed  = sum(r.get("passed", 0)       for r in reports)
    total_failed  = sum(r.get("failed", 0)        for r in reports)
    total_errored = sum(r.get("errored", 0)       for r in reports)

    if total_checks == 0:
        return 0, {"total_checks": 0, "passed": 0, "base_score": 0, "penalty": 0}

    base_score = (total_passed / total_checks) * 100

    # Critical failures from violations log
    critical_failures = sum(
        1 for v in violations if v.get("severity") == "CRITICAL"
    )
    penalty = 20 * critical_failures
    score   = max(0, round(base_score - penalty))

    breakdown = {
        "total_checks":      total_checks,
        "passed":            total_passed,
        "failed":            total_failed,
        "errored":           total_errored,
        "base_score":        round(base_score, 1),
        "critical_failures": critical_failures,
        "penalty_applied":   penalty,
        "contracts_covered": len(reports),
    }
    return score, breakdown


# ── Plain-language violations ─────────────────────────────────────────────────

def build_top_violations(
    violations: list[dict],
    reports: list[dict],
    max_items: int = 10,
) -> list[dict]:
    """
    Build plain-language violation descriptions readable by a non-technical stakeholder.
    Pulls from violation_log (real runtime violations) first, then runner FAILs.
    """
    items: list[dict] = []

    # From violation_log
    for v in violations:
        items.append({
            "source":      "violation_log",
            "field":       v.get("column_name", "unknown"),
            "system":      v.get("contract_id", "unknown"),
            "severity":    v.get("severity", "UNKNOWN"),
            "description": v.get("message", ""),
            "impact": (
                f"Affects {v.get('records_failing', 0)} records. "
                + (
                    f"Downstream: {', '.join(v.get('blast_radius', {}).get('affected_nodes', []))}."
                    if v.get("blast_radius", {}).get("affected_nodes") else ""
                )
            ),
            "detected_at": v.get("detected_at"),
        })

    # From runner FAIL results
    for report in reports:
        for result in report.get("results", []):
            if result.get("status") not in ("FAIL", "ERROR"):
                continue
            field   = result.get("column_name", result.get("check_id", "unknown"))
            msg     = result.get("message", "")
            failing = result.get("records_failing", 0)
            items.append({
                "source":      "runner",
                "field":       field,
                "system":      report.get("contract_id", "unknown"),
                "severity":    result.get("severity", "UNKNOWN"),
                "description": msg,
                "impact": (
                    f"{failing} records failing this check."
                    if failing else "Check failed — see details."
                ),
                "check_id": result.get("check_id"),
            })

    # Sort by severity weight descending
    _weight = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1, "UNKNOWN": 0}
    items.sort(key=lambda x: _weight.get(x["severity"], 0), reverse=True)

    return items[:max_items]


def build_violations_by_severity(violations: list[dict], reports: list[dict]) -> dict:
    counts: dict[str, int] = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}

    for v in violations:
        sev = v.get("severity", "LOW")
        counts[sev] = counts.get(sev, 0) + 1

    for report in reports:
        for result in report.get("results", []):
            if result.get("status") in ("FAIL", "ERROR"):
                sev = result.get("severity", "LOW")
                counts[sev] = counts.get(sev, 0) + 1

    return counts


# ── Schema changes section ────────────────────────────────────────────────────

def build_schema_changes_section(evolution_reports: list[dict]) -> list[dict]:
    """Summarise each schema evolution report in plain language."""
    section = []
    for ev in evolution_reports:
        compat  = ev.get("overall_compatibility", {})
        verdict = compat.get("verdict", "UNKNOWN")
        changes = ev.get("changes", [])
        summary = ev.get("migration_summary", {})

        breaking = [c for c in changes if c.get("change_type") == "BREAKING"]
        critical = [c for c in changes if c.get("severity") == "CRITICAL"]

        plain_changes = []
        for c in changes[:5]:
            plain_changes.append(
                f"{c['field']}: {c.get('human_diff', c.get('category', ''))}"
            )

        section.append({
            "contract_id":         ev.get("contract_id"),
            "old_snapshot":        ev.get("old_snapshot"),
            "new_snapshot":        ev.get("new_snapshot"),
            "compatibility_verdict": verdict,
            "migration_required":  ev.get("migration_required", False),
            "total_changes":       summary.get("total_changes", len(changes)),
            "breaking_changes":    summary.get("breaking_changes", len(breaking)),
            "critical_issues":     summary.get("critical_issues", len(critical)),
            "plain_language_summary": (
                f"Schema for '{ev.get('contract_id')}' changed between "
                f"snapshots {ev.get('old_snapshot')} and {ev.get('new_snapshot')}. "
                f"Compatibility: {verdict}. "
                f"{summary.get('breaking_changes', 0)} breaking change(s), "
                f"{summary.get('critical_issues', 0)} critical issue(s). "
                + ("Migration is required before deploying." if ev.get("migration_required") else "No migration required.")
            ),
            "top_changes": plain_changes,
            "required_action": (
                "STOP deployment and execute migration checklist."
                if verdict == "BREAKING" else
                "Review changes before next release." if verdict != "FULLY_COMPATIBLE"
                else "No action required."
            ),
        })

    return section


# ── AI risk section ───────────────────────────────────────────────────────────

def build_ai_risk_section(ai_data: dict) -> dict:
    """
    Build AI risk assessment DERIVED FROM validation_reports/ai_extensions.json.
    Not recomputed — reads the already-generated artifact directly.
    """
    drift    = ai_data.get("embedding_drift", {})
    prompt   = ai_data.get("prompt_validation", {})
    llm_val  = ai_data.get("llm_output_validation", {})

    drift_status  = drift.get("status", "NOT_RUN")
    drift_score   = drift.get("drift_score")
    llm_rate      = llm_val.get("violation_rate", 0.0)
    llm_trend     = llm_val.get("trend", "unknown")
    quarantined   = prompt.get("quarantined_records", 0)

    # Overall AI risk level
    if drift_status == "FAIL" or llm_trend == "rising":
        ai_risk_level = "HIGH"
    elif quarantined > 0 or llm_rate > 0:
        ai_risk_level = "MEDIUM"
    else:
        ai_risk_level = "LOW"

    interpretation = []
    if drift_status == "BASELINE_SET":
        interpretation.append(
            "Embedding baseline established — drift will be measured on next run."
        )
    elif drift_status == "PASS":
        interpretation.append(
            f"Semantic content of extracted facts is stable "
            f"(drift={drift_score}, threshold={drift.get('threshold')})."
        )
    elif drift_status == "FAIL":
        interpretation.append(
            f"ALERT: Embedding drift detected (score={drift_score}). "
            "Extracted fact distribution has shifted — check extraction pipeline."
        )

    if quarantined:
        interpretation.append(
            f"{quarantined} extraction record(s) failed prompt schema validation "
            "and were quarantined. Review outputs/quarantine/quarantine.jsonl."
        )
    else:
        interpretation.append("All extraction records passed prompt schema validation.")

    if llm_rate == 0:
        interpretation.append("LLM output schema: all verdicts are valid (PASS/FAIL/WARN).")
    else:
        interpretation.append(
            f"LLM output violations: {llm_val.get('schema_violations', 0)} of "
            f"{llm_val.get('total_outputs', 0)} outputs ({llm_rate:.1%}) have invalid verdict values. "
            f"Trend: {llm_trend}."
        )

    return {
        "ai_risk_level":        ai_risk_level,
        "embedding_drift": {
            "status":      drift_status,
            "drift_score": drift_score,
            "threshold":   drift.get("threshold"),
            "backend":     drift.get("embedding_backend"),
        },
        "prompt_validation": {
            "status":             prompt.get("status", "NOT_RUN"),
            "quarantined_records": quarantined,
            "total_records":      prompt.get("total_records"),
        },
        "llm_output_validation": {
            "status":           llm_val.get("status", "NOT_RUN"),
            "violation_rate":   llm_rate,
            "trend":            llm_trend,
            "schema_violations": llm_val.get("schema_violations", 0),
        },
        "interpretation": " ".join(interpretation),
        "source_file": "validation_reports/ai_extensions.json",
    }


# ── Recommendations ───────────────────────────────────────────────────────────

def build_recommendations(
    violations: list[dict],
    evolution_reports: list[dict],
    ai_data: dict,
    health_score: int,
) -> list[dict]:
    """
    Build specific, actionable recommendations.
    Each recommendation references exact file, field, and fix.
    """
    recs: list[dict] = []
    priority = 1

    # From violations
    for v in violations:
        sev     = v.get("severity", "LOW")
        col     = v.get("column_name", "")
        ctype   = v.get("check_type", "")
        msg     = v.get("message", "")
        contract = v.get("contract_id", "")
        blame   = (v.get("blame_chain") or [{}])[0]

        if "confidence" in col and "range" in ctype:
            recs.append({
                "priority": priority,
                "severity": sev,
                "title": "Fix confidence scale: revert to 0.0–1.0 float range",
                "action": (
                    f"In the extraction pipeline (see {blame.get('file_path', 'scripts/inject_violation.py')}), "
                    "change the confidence output to emit float values in [0.0, 1.0]. "
                    "Current values are in [0, 100] which breaks week4 ranking weights by 100×."
                ),
                "field":    col,
                "contract": contract,
                "fix":      "confidence = raw_score / 100.0  # normalize before writing",
            })
        elif "variance" in ctype or "non_zero_variance" in ctype:
            recs.append({
                "priority": priority,
                "severity": sev,
                "title": "Fix zero-variance confidence: extractor returning hard-coded default",
                "action": (
                    "The extractor is emitting a constant confidence value (all values = 90.0). "
                    "Verify the model scoring logic in the extraction pipeline and ensure "
                    "per-fact confidence is computed rather than a static default."
                ),
                "field":    col,
                "contract": contract,
                "fix":      "Replace static `confidence = 0.9` with per-fact model logit score.",
            })
        else:
            recs.append({
                "priority": priority,
                "severity": sev,
                "title":    f"Resolve check failure: {col}",
                "action":   msg,
                "field":    col,
                "contract": contract,
            })
        priority += 1

    # From breaking schema changes
    for ev in evolution_reports:
        if not ev.get("migration_required"):
            continue
        cid = ev.get("contract_id", "")
        for change in ev.get("changes", []):
            if change.get("change_type") != "BREAKING":
                continue
            cat   = change.get("category", "")
            field = change.get("field", "")
            if cat == "CONFIDENCE_SCALE_BREAK":
                recs.append({
                    "priority": priority,
                    "severity": "CRITICAL",
                    "title": f"Schema: revert confidence scale in {cid}",
                    "action": (
                        f"Field '{field}' maximum changed from "
                        f"{change.get('old_value', {}).get('maximum')} to "
                        f"{change.get('new_value', {}).get('maximum')}. "
                        "Divide all confidence values by 100 before writing to downstream consumers."
                    ),
                    "field":    field,
                    "contract": cid,
                    "fix":      "confidence = value / 100.0",
                })
            elif cat in ("REMOVE_COLUMN", "RENAME_COLUMN"):
                recs.append({
                    "priority": priority,
                    "severity": "HIGH",
                    "title": f"Schema: add backward-compat alias for '{field}' in {cid}",
                    "action": (
                        f"Field '{field}' was {'removed' if cat == 'REMOVE_COLUMN' else 'renamed'}. "
                        f"Add an alias in the pipeline output so existing consumers "
                        "do not crash with KeyError."
                    ),
                    "field":    field,
                    "contract": cid,
                })
            priority += 1

    # From AI checks
    drift = ai_data.get("embedding_drift", {})
    if drift.get("status") == "FAIL":
        recs.append({
            "priority": priority,
            "severity": "HIGH",
            "title":    "AI: investigate semantic drift in extraction pipeline",
            "action": (
                f"Embedding drift score {drift.get('drift_score')} exceeds threshold "
                f"{drift.get('threshold')}. The distribution of extracted facts has shifted. "
                "Compare current outputs/week3/extractions.jsonl against baseline corpus. "
                "Check for prompt template changes or model version updates."
            ),
            "fix": "Pin extraction model version in pipeline config.",
        })
        priority += 1

    prompt_val = ai_data.get("prompt_validation", {})
    if prompt_val.get("quarantined_records", 0) > 0:
        recs.append({
            "priority": priority,
            "severity": "MEDIUM",
            "title":    "AI: review quarantined extraction records",
            "action": (
                f"{prompt_val['quarantined_records']} record(s) failed prompt schema validation. "
                "Inspect outputs/quarantine/quarantine.jsonl for specific errors. "
                "Fix the extraction pipeline to emit valid doc_id (64-char hex) and "
                "confidence values in [0.0, 1.0]."
            ),
            "fix": "Review and fix extraction pipeline output schema.",
        })
        priority += 1

    if health_score < 60:
        recs.append({
            "priority": priority,
            "severity": "HIGH",
            "title":    "System health critical — run full re-validation after fixes",
            "action": (
                f"Data health score is {health_score}/100 (below 60). "
                "After applying the above fixes, re-run: "
                "python contracts/runner.py --contract-id week3-document-refinery-extractions "
                "--source outputs/week3/extractions.jsonl and verify all CRITICAL checks pass."
            ),
        })

    return recs


# ── Health narrative ──────────────────────────────────────────────────────────

def build_health_narrative(
    score: int,
    breakdown: dict,
    violations: list[dict],
) -> str:
    total    = breakdown.get("total_checks", 0)
    passed   = breakdown.get("passed", 0)
    critical = breakdown.get("critical_failures", 0)
    penalty  = breakdown.get("penalty_applied", 0)
    contracts = breakdown.get("contracts_covered", 0)

    if score >= 90:
        rating = "excellent"
    elif score >= 75:
        rating = "good"
    elif score >= 60:
        rating = "needs attention"
    else:
        rating = "critical — immediate action required"

    parts = [
        f"Overall data health score is {score}/100 ({rating}). ",
        f"Across {contracts} contract(s), {passed} of {total} validation checks passed "
        f"({breakdown.get('base_score', 0):.0f}% pass rate). ",
    ]

    if critical:
        parts.append(
            f"There are {critical} critical violation(s) in the violation log, "
            f"each applying a 20-point penalty (total penalty: {penalty} points). "
        )

    failed = breakdown.get("failed", 0)
    errored = breakdown.get("errored", 0)
    if failed or errored:
        parts.append(
            f"{failed} check(s) failed and {errored} errored (missing fields). "
            "These are marked in the top violations section. "
        )

    if score < 60:
        parts.append(
            "The system is not in a deployable state. "
            "All CRITICAL violations must be resolved before the next release."
        )
    elif score < 80:
        parts.append(
            "The system is partially compliant. "
            "Resolve failing checks before the next production deployment."
        )

    return "".join(parts)


# ── Main enforcer report assembler ────────────────────────────────────────────

def generate_enforcer_report(
    reports_dir: Path,
    violations_path: Path,
    ai_extensions_path: Path,
    output_path: Path,
) -> dict:
    """
    Assemble the full enforcer report from real data outputs.
    Nothing is hardcoded — all values are computed from live artifacts.
    """
    # ── Load all inputs ───────────────────────────────────────────────────────
    validation_reports   = _load_validation_reports(reports_dir)
    violations           = _load_jsonl(violations_path)
    ai_data              = _load_json(ai_extensions_path)
    evolution_reports    = _load_schema_evolution_reports(reports_dir)

    if not validation_reports:
        print("[report_generator] WARNING: no runner validation reports found in "
              f"{reports_dir}", file=sys.stderr)

    if not ai_data:
        print(f"[report_generator] WARNING: ai_extensions.json not found at "
              f"{ai_extensions_path}. Run ai_extensions.py first.", file=sys.stderr)

    # ── Compute health score ──────────────────────────────────────────────────
    health_score, breakdown = compute_health_score(validation_reports, violations)

    # ── Assemble sections ─────────────────────────────────────────────────────
    top_violations = build_top_violations(violations, validation_reports)
    violations_by_severity = build_violations_by_severity(violations, validation_reports)
    schema_changes  = build_schema_changes_section(evolution_reports)
    ai_risk         = build_ai_risk_section(ai_data)
    recommendations = build_recommendations(violations, evolution_reports, ai_data, health_score)
    narrative       = build_health_narrative(health_score, breakdown, violations)

    report = {
        "report_id":         f"enforcer-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        "generated_at":      datetime.now(timezone.utc).isoformat(),
        "report_version":    "4.0.0",
        "data_health_score": health_score,
        "health_score_breakdown": breakdown,
        "health_narrative":  narrative,
        "top_violations":    top_violations,
        "total_violations_by_severity": violations_by_severity,
        "schema_changes":    schema_changes,
        "ai_risk_assessment": ai_risk,
        "recommendations":   recommendations,
        "sources": {
            "validation_reports":  str(reports_dir),
            "violations_log":      str(violations_path),
            "ai_extensions":       str(ai_extensions_path),
            "schema_evolutions":   [
                str(reports_dir / f"schema_evolution_{ev.get('contract_id')}.json")
                for ev in evolution_reports
            ],
        },
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as fh:
        json.dump(report, fh, indent=2, default=str)

    return report


# ── CLI ───────────────────────────────────────────────────────────────────────

def _enforcer_main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 4B — Enforcer Report Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  python contracts/report_generator.py \\
      --output enforcer_report/report_data.json
""",
    )
    parser.add_argument(
        "--output", default="enforcer_report/report_data.json",
        help="Output path (default: enforcer_report/report_data.json)",
    )
    parser.add_argument(
        "--reports-dir", default="validation_reports",
        help="Directory containing runner JSON reports",
    )
    parser.add_argument(
        "--violations", default="violation_log/violations.jsonl",
        help="Path to violations JSONL log",
    )
    parser.add_argument(
        "--ai-extensions", default="validation_reports/ai_extensions.json",
        help="Path to ai_extensions.json output",
    )
    args = parser.parse_args()

    root = _HERE
    output_path       = root / args.output
    reports_dir       = root / args.reports_dir
    violations_path   = root / args.violations
    ai_extensions_path = root / args.ai_extensions

    print("[report_generator] Building enforcer report from live data ...")
    print(f"  validation_reports : {reports_dir}")
    print(f"  violations         : {violations_path}")
    print(f"  ai_extensions      : {ai_extensions_path}")

    report = generate_enforcer_report(
        reports_dir=reports_dir,
        violations_path=violations_path,
        ai_extensions_path=ai_extensions_path,
        output_path=output_path,
    )

    score = report["data_health_score"]
    print(f"\n[report_generator] ── Summary ────────────────────────────────")
    print(f"  Health score  : {score}/100")
    print(f"  Violations    : {sum(report['total_violations_by_severity'].values())}")
    print(f"  Schema changes: {len(report['schema_changes'])}")
    print(f"  Recommendations: {len(report['recommendations'])}")
    print(f"\n[report_generator] Report written → {output_path}")


if __name__ == "__main__":
    _enforcer_main()
