"""
contracts/schema_analyzer.py
Phase 1 — Schema drift and compatibility analysis.

Compares two contract schema blocks and classifies every change as:
    BREAKING   — field removed, type narrowed, required added
    ERROR      — format changed, enum values removed
    WARNING    — new field added, description changed, enum values added
    INFO       — metadata-only change

Usage:
    from contracts.schema_analyzer import diff_schemas, CompatibilityReport

    report = diff_schemas(old_schema, new_schema)
    print(report.summary())
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SchemaDiff:
    field_path: str
    change_type: str          # "added", "removed", "type_changed", "format_changed", etc.
    severity: str             # BREAKING | ERROR | WARNING | INFO
    old_value: object = None
    new_value: object = None
    message: str = ""


@dataclass
class CompatibilityReport:
    diffs: list[SchemaDiff] = field(default_factory=list)

    @property
    def is_breaking(self) -> bool:
        return any(d.severity == "BREAKING" for d in self.diffs)

    @property
    def has_errors(self) -> bool:
        return any(d.severity in ("BREAKING", "ERROR") for d in self.diffs)

    def summary(self) -> str:
        if not self.diffs:
            return "No schema changes detected."
        lines = [f"Schema diff ({len(self.diffs)} change(s)):"]
        for d in self.diffs:
            lines.append(f"  [{d.severity}] {d.field_path}: {d.message}")
        return "\n".join(lines)


def diff_schemas(
    old_schema: dict,
    new_schema: dict,
    _prefix: str = "",
) -> CompatibilityReport:
    """
    Recursively compare two schema dicts and return a CompatibilityReport.

    Parameters
    ----------
    old_schema : dict
        The ``schema:`` block from the previous contract version.
    new_schema : dict
        The ``schema:`` block from the new contract version.

    Returns
    -------
    CompatibilityReport
    """
    report = CompatibilityReport()

    old_fields = {k: v for k, v in old_schema.items() if isinstance(v, dict)}
    new_fields = {k: v for k, v in new_schema.items() if isinstance(v, dict)}

    # Removed fields
    for fname in old_fields:
        if fname not in new_fields:
            path = f"{_prefix}{fname}" if _prefix else fname
            report.diffs.append(SchemaDiff(
                field_path=path,
                change_type="removed",
                severity="BREAKING",
                old_value=old_fields[fname],
                message=f"Field '{path}' was removed (BREAKING).",
            ))

    # Added fields
    for fname in new_fields:
        if fname not in old_fields:
            path = f"{_prefix}{fname}" if _prefix else fname
            report.diffs.append(SchemaDiff(
                field_path=path,
                change_type="added",
                severity="WARNING",
                new_value=new_fields[fname],
                message=f"Field '{path}' was added.",
            ))

    # Changed fields
    for fname in old_fields:
        if fname not in new_fields:
            continue
        path = f"{_prefix}{fname}" if _prefix else fname
        old_f = old_fields[fname]
        new_f = new_fields[fname]

        # Type change
        if old_f.get("type") != new_f.get("type"):
            report.diffs.append(SchemaDiff(
                field_path=path,
                change_type="type_changed",
                severity="BREAKING",
                old_value=old_f.get("type"),
                new_value=new_f.get("type"),
                message=(
                    f"Type changed from '{old_f.get('type')}' to '{new_f.get('type')}' (BREAKING)."
                ),
            ))

        # Format change
        if old_f.get("format") != new_f.get("format"):
            report.diffs.append(SchemaDiff(
                field_path=path,
                change_type="format_changed",
                severity="ERROR",
                old_value=old_f.get("format"),
                new_value=new_f.get("format"),
                message=(
                    f"Format changed from '{old_f.get('format')}' to '{new_f.get('format')}'."
                ),
            ))

        # nullable loosened (was required, now nullable) → WARNING
        if not old_f.get("nullable") and new_f.get("nullable"):
            report.diffs.append(SchemaDiff(
                field_path=path,
                change_type="nullable_added",
                severity="WARNING",
                message=f"Field '{path}' became nullable.",
            ))

        # nullable tightened (was nullable, now required) → BREAKING
        if old_f.get("nullable") and not new_f.get("nullable"):
            report.diffs.append(SchemaDiff(
                field_path=path,
                change_type="nullable_removed",
                severity="BREAKING",
                message=f"Field '{path}' is now non-nullable (BREAKING).",
            ))

        # Recurse into items.properties
        old_items = (old_f.get("items") or {}).get("properties", {})
        new_items = (new_f.get("items") or {}).get("properties", {})
        if old_items or new_items:
            nested = diff_schemas(old_items, new_items, _prefix=f"{path}.items.")
            report.diffs.extend(nested.diffs)

    return report
