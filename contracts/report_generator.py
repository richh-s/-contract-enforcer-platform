"""
contracts/report_generator.py
Phase 1 — Validation report generation.

Converts raw CheckResult lists from contracts/runner.py into
structured JSON reports and human-readable Markdown summaries.

Usage:
    from contracts.report_generator import build_json_report, build_markdown_report

    json_report = build_json_report(results, contract, data_path)
    md_report   = build_markdown_report(results, contract, data_path)
"""

from __future__ import annotations

import json
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
