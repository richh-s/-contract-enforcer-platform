"""
contracts/schema_analyzer.py
Phase 3 — Schema Evolution Analyzer

Loads consecutive schema snapshots, diffs them, classifies every change with
the full taxonomy, computes backward/forward/full compatibility, blast radius
from lineage, consumer failure analysis, and produces two output reports:

  1. <--output>                               (CLI-specified path)
  2. migration_impact_{contract_id}_{ts}.json (always written alongside output)

CLI usage:
    python contracts/schema_analyzer.py \\
        --contract-id week3-extractions \\
        --since "7 days ago" \\
        --output validation_reports/schema_evolution_week3.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import yaml

# ── Project root on sys.path ──────────────────────────────────────────────────
_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

# ── Constants ─────────────────────────────────────────────────────────────────

IGNORED_KEYS = {"description", "title", "examples", "llm_annotations"}

REQUIRED_CONTRACT_KEYS = {"kind", "apiVersion", "models"}

# Change taxonomy — standard + extended
TAXONOMY = {
    # Standard (spec-required)
    "ADD_NULLABLE_COLUMN",
    "ADD_NON_NULLABLE_COLUMN",
    "REMOVE_COLUMN",
    "RENAME_COLUMN",
    "TYPE_WIDENING",
    "TYPE_NARROWING",
    "ENUM_ADDITION",
    "ENUM_REMOVAL",
    # Extended
    "NULLABILITY_CHANGE",
    "RANGE_CHANGE",
    "FORMAT_CHANGE",
    "PATTERN_CHANGE",
    "CONFIDENCE_SCALE_BREAK",
    "NO_CHANGE",
}

# Compatibility rules per category
# (backward, forward)
_COMPAT = {
    "ADD_NULLABLE_COLUMN":    (True,  False),
    "ADD_NON_NULLABLE_COLUMN": (False, False),
    "REMOVE_COLUMN":          (False, True),
    "RENAME_COLUMN":          (False, False),
    "TYPE_WIDENING":          (True,  False),
    "TYPE_NARROWING":         (False, True),
    "ENUM_ADDITION":          (True,  False),
    "ENUM_REMOVAL":           (False, True),
    "NULLABILITY_CHANGE":     (False, False),
    "RANGE_CHANGE":           (False, False),
    "FORMAT_CHANGE":          (False, False),
    "PATTERN_CHANGE":         (False, False),
    "CONFIDENCE_SCALE_BREAK": (False, False),
    "NO_CHANGE":              (True,  True),
}

# Severity rules per category
_SEVERITY = {
    "CONFIDENCE_SCALE_BREAK": "CRITICAL",
    "ADD_NON_NULLABLE_COLUMN": "HIGH",
    "REMOVE_COLUMN":           "HIGH",
    "RENAME_COLUMN":           "HIGH",
    "TYPE_NARROWING":          "HIGH",
    "NULLABILITY_CHANGE":      "HIGH",
    "FORMAT_CHANGE":           "HIGH",
    "PATTERN_CHANGE":          "MEDIUM",
    "RANGE_CHANGE":            "MEDIUM",
    "TYPE_WIDENING":           "MEDIUM",
    "ENUM_REMOVAL":            "HIGH",
    "ENUM_ADDITION":           "LOW",
    "ADD_NULLABLE_COLUMN":     "LOW",
    "NO_CHANGE":               "INFO",
}

# Human-readable descriptions for each category
_HUMAN = {
    "ADD_NULLABLE_COLUMN":    "New nullable field added — backward compatible, consumers must handle absent values",
    "ADD_NON_NULLABLE_COLUMN": "New required field added — BREAKING for existing writers who don't populate it",
    "REMOVE_COLUMN":          "Field removed — BREAKING for all consumers that read this field",
    "RENAME_COLUMN":          "Field renamed — BREAKING; consumers must update field references",
    "TYPE_WIDENING":          "Type widened (e.g. int→number) — safe for readers, may break strict writers",
    "TYPE_NARROWING":         "Type narrowed (e.g. number→int) — BREAKING for consumers expecting wider type",
    "ENUM_ADDITION":          "New enum value added — safe for readers, consumers must handle the new value",
    "ENUM_REMOVAL":           "Enum value removed — BREAKING for consumers that produce or match that value",
    "NULLABILITY_CHANGE":     "Nullability constraint changed — may break strict consumers",
    "RANGE_CHANGE":           "Numeric range constraint changed — BREAKING if existing values fall outside new range",
    "FORMAT_CHANGE":          "Field format changed — may break format-aware parsers",
    "PATTERN_CHANGE":         "Regex pattern changed — BREAKING if existing values no longer match",
    "CONFIDENCE_SCALE_BREAK": "Confidence scale changed from 0.0–1.0 to 0–100 — CRITICAL: downstream ranking inflated by 100x",
    "NO_CHANGE":              "No semantic change detected",
}

# Type widening matrix: (old_type, new_type) → True if widening
_TYPE_WIDENING = {
    ("integer", "number"),
    ("integer", "string"),
    ("number",  "string"),
}


# ── Timestamp helpers ─────────────────────────────────────────────────────────

def extract_ts(filepath: Path) -> datetime:
    """Parse timestamp from snapshot filename: 20260401T123850Z.yaml"""
    stem = filepath.stem  # e.g. "20260401T123850Z"
    try:
        return datetime.strptime(stem, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        # Fallback: use file mtime
        return datetime.fromtimestamp(filepath.stat().st_mtime, tz=timezone.utc)


def parse_since(since_str: str | None) -> datetime | None:
    """Parse natural-language --since string to a UTC datetime."""
    if since_str is None:
        return None

    s = since_str.lower().strip()

    m = re.match(r"(\d+)\s+day", s)
    if m:
        return datetime.now(tz=timezone.utc) - timedelta(days=int(m.group(1)))

    m = re.match(r"(\d+)\s+week", s)
    if m:
        return datetime.now(tz=timezone.utc) - timedelta(weeks=int(m.group(1)))

    m = re.match(r"(\d+)\s+hour", s)
    if m:
        return datetime.now(tz=timezone.utc) - timedelta(hours=int(m.group(1)))

    raise ValueError(
        f"Unrecognised --since format: '{since_str}'. "
        "Use e.g. '7 days ago', '2 weeks ago', '24 hours ago'."
    )


# ── Snapshot I/O ──────────────────────────────────────────────────────────────

def validate_snapshots(snapshot_dir: Path) -> list[Path]:
    """Return sorted list of snapshot files; raise if fewer than 2."""
    files = sorted(snapshot_dir.glob("*.yaml"), key=extract_ts)

    if not files:
        raise ValueError(
            f"No schema snapshots found in '{snapshot_dir}'. "
            "Run the generator first:\n"
            "  python contracts/generator.py --contract-id <id> ..."
        )

    if len(files) < 2:
        raise ValueError(
            f"Phase 3 requires at least 2 schema snapshots in '{snapshot_dir}', "
            f"but only {len(files)} found. Run the generator again to produce a "
            "second snapshot after a schema change."
        )

    return files


def choose_pair(files: list[Path], since_dt: datetime | None) -> tuple[Path, Path]:
    """Select the two consecutive snapshots to diff."""
    if since_dt:
        filtered = [f for f in files if extract_ts(f) >= since_dt]
        if len(filtered) < 2:
            raise ValueError(
                f"Only {len(filtered)} snapshot(s) found after --since filter. "
                "Widen the window or omit --since to use the two most recent."
            )
        return filtered[-2], filtered[-1]

    return files[-2], files[-1]


def load_yaml(path: Path) -> dict:
    with open(path) as fh:
        return yaml.safe_load(fh)


# ── Schema validation ─────────────────────────────────────────────────────────

def validate_contract_structure(contract: dict, path: Path) -> None:
    """Ensure the snapshot looks like a valid Bitol contract."""
    missing = REQUIRED_CONTRACT_KEYS - set(contract.keys())
    if missing:
        raise ValueError(
            f"Snapshot '{path}' is missing required keys: {missing}. "
            "Ensure snapshots are generated by contracts/generator.py."
        )

    if not isinstance(contract.get("models"), dict):
        raise ValueError(
            f"Snapshot '{path}': 'models' must be a dict, "
            f"got {type(contract.get('models')).__name__}."
        )


# ── Normalisation + flattening ────────────────────────────────────────────────

def _strip_ignored(obj: Any) -> Any:
    """Recursively remove keys in IGNORED_KEYS from dicts."""
    if isinstance(obj, dict):
        return {
            k: _strip_ignored(v)
            for k, v in obj.items()
            if k not in IGNORED_KEYS
        }
    if isinstance(obj, list):
        return [_strip_ignored(i) for i in obj]
    return obj


def normalize_contract(contract: dict) -> dict:
    """Remove noise keys that should not affect diff semantics."""
    return _strip_ignored(contract)


def flatten_fields(fields: dict, prefix: str = "") -> dict[str, dict]:
    """
    Flatten a Bitol models.<name>.fields dict to a path→field_def mapping.

    Supports:
      - top-level fields:        doc_id → {"type": "string", ...}
      - dot-notation paths:      metadata.correlation_id (already flat in contract)
      - array item properties:   items.properties → extracted_facts[*].confidence
    """
    flat: dict[str, dict] = {}

    for fname, fdef in (fields or {}).items():
        path = f"{prefix}{fname}" if prefix else fname

        if not isinstance(fdef, dict):
            flat[path] = {"type": str(fdef)}
            continue

        # Recurse into items.properties (array fields)
        items_props = (fdef.get("items") or {}).get("properties")
        if items_props and isinstance(items_props, dict):
            flat[path] = {k: v for k, v in fdef.items() if k != "items"}
            nested = flatten_fields(items_props, prefix=f"{path}[*].")
            flat.update(nested)
        else:
            flat[path] = fdef

    return flat


def extract_model_fields(contract: dict) -> tuple[str, dict]:
    """Return (model_name, flat_fields) for the first model in the contract."""
    models = contract.get("models", {})
    if not models:
        return "", {}
    model_name = next(iter(models))
    fields = models[model_name].get("fields", {})
    return model_name, flatten_fields(fields)


# ── Rename detection ──────────────────────────────────────────────────────────

RENAME_SIMILARITY_THRESHOLD = 0.60   # Total score must clear this bar
RENAME_MIN_NAME_RATIO       = 0.35   # Name similarity alone must clear this — prevents
                                     # type-only matches from masquerading as renames.
                                     # Example guard: "doc_id" (string) vs "timestamp" (string)
                                     # share a type but name ratio ≈ 0.18 → correctly rejected.
RENAME_CONFIDENCE_GAP       = 0.12   # Best candidate must beat second-best by this margin;
                                     # ambiguous matches (two equally-similar adds) are left as
                                     # independent REMOVE + ADD instead.


def _field_similarity(name_a: str, def_a: dict, name_b: str, def_b: dict) -> float:
    """
    Score similarity between two field candidates for rename detection.

    Components (all in [0, 1] before capping):
      - name_score  : SequenceMatcher character-level ratio  (weight: 1.0)
      - type_bonus  : +0.15 if both fields share the same 'type'
      - struct_bonus: +0.10 × (shared constraint keys / union of constraint keys)

    The name ratio is checked independently against RENAME_MIN_NAME_RATIO before
    the total score is considered, ensuring structural similarity alone cannot
    produce a false rename.
    """
    name_score = SequenceMatcher(None, name_a, name_b).ratio()

    type_bonus = 0.15 if def_a.get("type") == def_b.get("type") else 0.0

    constraint_keys = {"minimum", "maximum", "format", "enum", "pattern", "nullable", "required"}
    a_keys = set(def_a.keys()) & constraint_keys
    b_keys = set(def_b.keys()) & constraint_keys
    union  = a_keys | b_keys
    struct_bonus = (len(a_keys & b_keys) / len(union) * 0.10) if union else 0.0

    return name_score + type_bonus + struct_bonus


def detect_renames(
    removed: dict[str, dict],
    added: dict[str, dict],
) -> list[tuple[str, str]]:
    """
    Match removed fields to added fields as renames.

    Algorithm:
      For each removed field, score every added field that hasn't been claimed.
      A rename pair is accepted only when ALL three conditions hold:
        1. name similarity ratio >= RENAME_MIN_NAME_RATIO   (name-level guard)
        2. total score > RENAME_SIMILARITY_THRESHOLD        (combined bar)
        3. best score exceeds second-best by RENAME_CONFIDENCE_GAP  (ambiguity guard)

      Each field participates in at most one rename pair (greedy, best-first).
    """
    renames: list[tuple[str, str]] = []
    used_added: set[str] = set()

    for old_name, old_def in removed.items():
        # Collect all candidates with their scores
        candidates: list[tuple[float, str]] = []
        for new_name, new_def in added.items():
            if new_name in used_added:
                continue
            name_ratio = SequenceMatcher(None, old_name, new_name).ratio()
            if name_ratio < RENAME_MIN_NAME_RATIO:
                continue  # fails name-level guard — reject before full score
            score = _field_similarity(old_name, old_def, new_name, new_def)
            if score > RENAME_SIMILARITY_THRESHOLD:
                candidates.append((score, new_name))

        if not candidates:
            continue

        candidates.sort(reverse=True)
        best_score, best_new = candidates[0]

        # Ambiguity guard: skip if two candidates are too close
        if len(candidates) >= 2:
            second_score = candidates[1][0]
            if best_score - second_score < RENAME_CONFIDENCE_GAP:
                continue  # ambiguous — treat as independent REMOVE + ADD

        renames.append((old_name, best_new))
        used_added.add(best_new)

    return renames


# ── Change classification ─────────────────────────────────────────────────────

def _detect_confidence_scale_break(
    field: str, old_def: dict, new_def: dict
) -> bool:
    """
    Detect the specific confidence scale bug: 0.0–1.0 → 0–100.

    Triggers when:
      - field name contains 'confidence' (or 'score', 'probability')
      - old maximum ≤ 1.0
      - new maximum > 10.0
    """
    name_lower = field.lower()
    is_score_field = any(
        kw in name_lower for kw in ("confidence", "score", "probability", "weight")
    )
    if not is_score_field:
        return False

    old_max = old_def.get("maximum")
    new_max = new_def.get("maximum")

    if old_max is None or new_max is None:
        return False

    return float(old_max) <= 1.0 and float(new_max) > 10.0


def _compatibility_for(category: str) -> dict:
    backward, forward = _COMPAT.get(category, (False, False))
    return {
        "backward": backward,
        "forward": forward,
        "full": backward and forward,
    }


def _severity_for(category: str) -> str:
    return _SEVERITY.get(category, "MEDIUM")


def _change_type_for(category: str) -> str:
    compat = _COMPAT.get(category, (False, False))
    return "COMPATIBLE" if all(compat) else (
        "BREAKING" if not any(compat) else "PARTIAL"
    )


def _spec_mapping(category: str) -> str:
    """Map extended categories back to the closest standard taxonomy entry."""
    mapping = {
        "NULLABILITY_CHANGE":      "TYPE_NARROWING",
        "RANGE_CHANGE":            "TYPE_NARROWING",
        "FORMAT_CHANGE":           "TYPE_NARROWING",
        "PATTERN_CHANGE":          "TYPE_NARROWING",
        "CONFIDENCE_SCALE_BREAK":  "TYPE_NARROWING",
        "NO_CHANGE":               "NO_CHANGE",
    }
    return mapping.get(category, category)


def classify_field_change(
    field: str,
    old_def: dict,
    new_def: dict,
) -> dict:
    """
    Produce a single change record comparing old_def → new_def for field.

    Field-by-field comparison (deterministic order):
      1. Confidence scale break (highest priority)
      2. Type change (widening vs narrowing)
      3. Nullability change
      4. Range constraints
      5. Format change
      6. Pattern change
      7. Enum changes
    """
    # 1. Confidence scale break
    if _detect_confidence_scale_break(field, old_def, new_def):
        category = "CONFIDENCE_SCALE_BREAK"
        human = (
            f"'{field}' maximum changed from {old_def.get('maximum')} to "
            f"{new_def.get('maximum')} — scale is now 0–100 instead of 0.0–1.0. "
            "CRITICAL: downstream ranking inflated by 100×."
        )
        return _make_change(field, category, old_def, new_def, human)

    # 2. Type change
    old_type = old_def.get("type")
    new_type = new_def.get("type")
    if old_type != new_type:
        if (old_type, new_type) in _TYPE_WIDENING:
            category = "TYPE_WIDENING"
        else:
            category = "TYPE_NARROWING"
        human = f"'{field}' type changed from '{old_type}' to '{new_type}'."
        return _make_change(field, category, old_def, new_def, human)

    # 3. Nullability change
    old_null = old_def.get("nullable", False)
    new_null = new_def.get("nullable", False)
    if old_null != new_null:
        category = "NULLABILITY_CHANGE"
        direction = "now nullable" if new_null else "now non-nullable (tightened)"
        human = f"'{field}' nullability changed: {direction}."
        return _make_change(field, category, old_def, new_def, human)

    # 4. Range constraints
    old_min = old_def.get("minimum")
    old_max = old_def.get("maximum")
    new_min = new_def.get("minimum")
    new_max = new_def.get("maximum")
    if old_min != new_min or old_max != new_max:
        category = "RANGE_CHANGE"
        human = (
            f"'{field}' range changed: [{old_min}, {old_max}] → [{new_min}, {new_max}]. "
            "Existing values outside the new range will fail validation."
        )
        return _make_change(field, category, old_def, new_def, human)

    # 5. Format change
    old_fmt = old_def.get("format")
    new_fmt = new_def.get("format")
    if old_fmt != new_fmt:
        category = "FORMAT_CHANGE"
        human = f"'{field}' format changed from '{old_fmt}' to '{new_fmt}'."
        return _make_change(field, category, old_def, new_def, human)

    # 6. Pattern change
    old_pat = old_def.get("pattern")
    new_pat = new_def.get("pattern")
    if old_pat != new_pat:
        category = "PATTERN_CHANGE"
        human = f"'{field}' pattern changed from '{old_pat}' to '{new_pat}'."
        return _make_change(field, category, old_def, new_def, human)

    # 7. Enum changes
    old_enum = set(old_def.get("enum") or [])
    new_enum = set(new_def.get("enum") or [])
    if old_enum != new_enum:
        added_vals = sorted(new_enum - old_enum)
        removed_vals = sorted(old_enum - new_enum)
        if removed_vals:
            category = "ENUM_REMOVAL"
            human = f"'{field}' enum values removed: {removed_vals}. Consumers producing these values will fail."
        else:
            category = "ENUM_ADDITION"
            human = f"'{field}' enum values added: {added_vals}. Consumers must handle new values."
        return _make_change(field, category, old_def, new_def, human)

    # No semantic change
    return _make_change(field, "NO_CHANGE", old_def, new_def, f"'{field}' — no semantic change.")


def _make_change(
    field: str,
    category: str,
    old_def: dict,
    new_def: dict,
    human: str,
) -> dict:
    compat = _compatibility_for(category)
    return {
        "field": field,
        "category": category,
        "mapped_to_spec": _spec_mapping(category),
        "change_type": _change_type_for(category),
        "severity": _severity_for(category),
        "compatibility": compat,
        "old_value": old_def,
        "new_value": new_def,
        "human_diff": human,
    }


# ── Full diff pipeline ────────────────────────────────────────────────────────

def diff_contracts(
    old_contract: dict,
    new_contract: dict,
) -> tuple[list[dict], dict]:
    """
    Run the full diff pipeline.

    Returns:
      changes   — list of change records
      exact_diff — {"added": [...], "removed": [...], "modified": [...]}
    """
    _, old_fields = extract_model_fields(old_contract)
    _, new_fields = extract_model_fields(new_contract)

    old_names = set(old_fields)
    new_names = set(new_fields)

    removed_names = old_names - new_names
    added_names   = new_names - old_names
    common_names  = old_names & new_names

    removed = {n: old_fields[n] for n in removed_names}
    added   = {n: new_fields[n] for n in added_names}

    # Rename detection — must run before REMOVE/ADD classification
    renames = detect_renames(removed, added)
    rename_old = {old for old, _ in renames}
    rename_new = {new for _, new in renames}

    changes: list[dict] = []
    exact_added:   list[str] = []
    exact_removed: list[str] = []
    exact_modified: list[dict] = []

    # Renamed fields
    for old_name, new_name in renames:
        change = _make_change(
            field=old_name,
            category="RENAME_COLUMN",
            old_def=old_fields[old_name],
            new_def=new_fields[new_name],
            human=(
                f"'{old_name}' renamed to '{new_name}'. "
                "All consumers referencing the old name will break."
            ),
        )
        change["renamed_to"] = new_name
        changes.append(change)
        exact_modified.append({"field": old_name, "renamed_to": new_name})

    # Purely removed fields (not part of a rename)
    for name in sorted(removed_names - rename_old):
        changes.append(_make_change(
            field=name,
            category="REMOVE_COLUMN",
            old_def=old_fields[name],
            new_def={},
            human=f"'{name}' removed — BREAKING for all consumers reading this field.",
        ))
        exact_removed.append(name)

    # Purely added fields (not part of a rename)
    for name in sorted(added_names - rename_new):
        fdef = new_fields[name]
        nullable = fdef.get("nullable", True)
        required = fdef.get("required", False)
        category = "ADD_NULLABLE_COLUMN" if (nullable or not required) else "ADD_NON_NULLABLE_COLUMN"
        changes.append(_make_change(
            field=name,
            category=category,
            old_def={},
            new_def=fdef,
            human=(
                f"'{name}' added ({'nullable' if nullable else 'required'}). "
                + ("Safe addition." if nullable else "BREAKING: existing writers must populate this field.")
            ),
        ))
        exact_added.append(name)

    # Modified fields (present in both)
    for name in sorted(common_names):
        change = classify_field_change(name, old_fields[name], new_fields[name])
        if change["category"] != "NO_CHANGE":
            changes.append(change)
            exact_modified.append({
                "field": name,
                "category": change["category"],
                "old": change["old_value"],
                "new": change["new_value"],
            })

    exact_diff = {
        "added":    exact_added,
        "removed":  exact_removed,
        "modified": exact_modified,
    }

    return changes, exact_diff


# ── Compatibility analysis ────────────────────────────────────────────────────

def overall_compatibility(changes: list[dict]) -> dict:
    if not changes:
        return {
            "verdict": "FULLY_COMPATIBLE",
            "backward": True,
            "forward": True,
            "full": True,
        }

    backward = all(c["compatibility"]["backward"] for c in changes)
    forward  = all(c["compatibility"]["forward"]  for c in changes)

    if backward and forward:
        verdict = "FULLY_COMPATIBLE"
    elif backward:
        verdict = "BACKWARD_COMPATIBLE"
    elif forward:
        verdict = "FORWARD_COMPATIBLE"
    else:
        verdict = "BREAKING"

    return {
        "verdict": verdict,
        "backward": backward,
        "forward": forward,
        "full": backward and forward,
    }


# ── Blast radius ──────────────────────────────────────────────────────────────

def compute_blast_radius(contract: dict, breaking_fields: set[str]) -> dict:
    """Compute which downstream nodes are affected by the breaking changes."""
    lineage = contract.get("lineage", {})
    downstream = lineage.get("downstream", []) or []

    affected_nodes: list[str] = []
    affected_pipelines: list[str] = []

    for node in downstream:
        node_id = node.get("id", "unknown")
        breaking_if_changed = set(node.get("breaking_if_changed") or [])
        fields_consumed     = set(node.get("fields_consumed") or [])

        impacted = breaking_fields & (breaking_if_changed | fields_consumed)
        if impacted:
            affected_nodes.append(node_id)
            affected_pipelines.append(node_id)

    return {
        "affected_nodes":           affected_nodes,
        "affected_pipelines":       affected_pipelines,
        "affected_consumers_count": len(affected_nodes),
        "source_lineage_snapshot":  lineage.get("snapshot_id"),
        "total_downstream_nodes":   len(downstream),
    }


# ── Consumer failure analysis ─────────────────────────────────────────────────

_FAILURE_TEMPLATES = {
    "CONFIDENCE_SCALE_BREAK": {
        "type": "SILENT_CORRUPTION",
        "description": (
            "Confidence values are now 0–100 instead of 0.0–1.0. "
            "Consumers applying a threshold of > 0.7 will now always pass (100 > 0.7), "
            "silently accepting all facts regardless of quality."
        ),
        "impact": "All downstream quality gates based on confidence threshold become no-ops.",
    },
    "REMOVE_COLUMN": {
        "type": "NULL_CRASH",
        "description": "Consumer references a field that no longer exists.",
        "impact": "KeyError / NullPointerException at runtime.",
    },
    "RENAME_COLUMN": {
        "type": "NULL_CRASH",
        "description": "Consumer references the old field name which is now absent.",
        "impact": "KeyError / NullPointerException at runtime.",
    },
    "TYPE_NARROWING": {
        "type": "PARSING_FAILURE",
        "description": "Field type narrowed; values the consumer previously wrote may no longer be valid.",
        "impact": "Schema validation errors on write; potential data loss.",
    },
    "ENUM_REMOVAL": {
        "type": "LOGIC_ERROR",
        "description": "A previously valid enum value is no longer allowed.",
        "impact": "Producers emitting the removed value will fail validation.",
    },
    "RANGE_CHANGE": {
        "type": "LOGIC_ERROR",
        "description": "Existing values may fall outside the new range constraint.",
        "impact": "Validation failures; silent truncation in some systems.",
    },
    "FORMAT_CHANGE": {
        "type": "PARSING_FAILURE",
        "description": "Format constraint changed; format-aware parsers may reject existing values.",
        "impact": "Deserialization errors in strict consumers.",
    },
}


def compute_consumer_impact(
    new_contract: dict, changes: list[dict]
) -> list[dict]:
    """
    Produce per-consumer failure analysis grounded in contract lineage data.

    Source of truth — NOT heuristic:
      Each consumer entry comes from contract["lineage"]["downstream"], which is
      written by the generator from the real week4 lineage_snapshots.jsonl.
      Two lineage fields drive the analysis:
        - fields_consumed:     what fields this consumer actually reads
        - breaking_if_changed: fields the consumer has declared as contract-critical

    A consumer appears in the output ONLY IF one of its declared fields overlaps
    with a detected BREAKING or PARTIAL change in this diff.  Failure mode types
    (SILENT_CORRUPTION, NULL_CRASH, etc.) are keyed on change category, not
    inferred — see _FAILURE_TEMPLATES above.
    """
    lineage   = new_contract.get("lineage", {})
    downstream = lineage.get("downstream", []) or []
    breaking_changes = [c for c in changes if c["change_type"] in ("BREAKING", "PARTIAL")]

    impact = []
    for node in downstream:
        node_id = node.get("id", "unknown")
        fields_consumed     = set(node.get("fields_consumed") or [])
        breaking_if_changed = set(node.get("breaking_if_changed") or [])
        watched = fields_consumed | breaking_if_changed

        relevant = [
            c for c in breaking_changes
            if c["field"] in watched
            or c.get("renamed_to") in watched
        ]

        if not relevant:
            continue

        failure_modes = []
        for c in relevant:
            template = _FAILURE_TEMPLATES.get(c["category"])
            if template:
                failure_modes.append({
                    "field": c["field"],
                    **template,
                })

        impact.append({
            "consumer_id":       node_id,
            "fields_consumed":   sorted(fields_consumed),
            "relevant_changes":  [c["field"] for c in relevant],
            "failure_modes":     failure_modes,
        })

    return impact


# ── Migration artefacts ───────────────────────────────────────────────────────

def migration_summary(changes: list[dict]) -> dict:
    total     = len(changes)
    breaking  = sum(1 for c in changes if c["change_type"] == "BREAKING")
    partial   = sum(1 for c in changes if c["change_type"] == "PARTIAL")
    compatible = total - breaking - partial
    critical  = sum(1 for c in changes if c["severity"] == "CRITICAL")
    high      = sum(1 for c in changes if c["severity"] == "HIGH")

    return {
        "total_changes":    total,
        "breaking_changes": breaking,
        "partial_changes":  partial,
        "compatible_changes": compatible,
        "critical_issues":  critical,
        "high_severity":    high,
    }


def generate_migration_checklist(
    changes: list[dict], blast: dict
) -> list[str]:
    """Return an ordered, severity-aware migration checklist."""
    has_critical = any(c["severity"] == "CRITICAL" for c in changes)
    has_breaking = any(c["change_type"] == "BREAKING" for c in changes)
    has_rename   = any(c["category"] == "RENAME_COLUMN" for c in changes)
    has_confidence_break = any(c["category"] == "CONFIDENCE_SCALE_BREAK" for c in changes)
    has_enum_removal = any(c["category"] == "ENUM_REMOVAL" for c in changes)
    has_remove   = any(c["category"] == "REMOVE_COLUMN" for c in changes)
    affected     = blast.get("affected_consumers_count", 0)

    steps: list[str] = []

    if has_critical:
        steps.append("IMMEDIATE: Halt all downstream deployments — CRITICAL breaking change detected")
        steps.append(
            "IMMEDIATE: Notify all " + str(affected) + " affected consumer team(s) of CRITICAL schema change"
        )

    if has_breaking:
        steps.append("Freeze schema in production until migration is complete")
        steps.append("Identify all consumers reading affected fields: " + str(
            sorted({c["field"] for c in changes if c["change_type"] == "BREAKING"})
        ))

    if has_confidence_break:
        steps.append(
            "CRITICAL FIX: Rescale confidence values — divide all values by 100 "
            "to restore 0.0–1.0 range before deploying to consumers"
        )
        steps.append(
            "Add contract validation gate: max(confidence) <= 1.0 to CI pipeline"
        )

    if has_rename:
        renamed = [(c["field"], c.get("renamed_to")) for c in changes if c["category"] == "RENAME_COLUMN"]
        for old, new in renamed:
            steps.append(f"Add backward-compat alias: expose '{old}' alongside '{new}' during migration window")
        steps.append("Set a deprecation deadline for old field names (recommend: 30 days)")

    if has_enum_removal:
        steps.append("For each removed enum value: verify no producers are still emitting it before deploying")

    if has_remove:
        removed = [c["field"] for c in changes if c["category"] == "REMOVE_COLUMN"]
        steps.append(f"Audit all consumers for references to removed fields: {removed}")

    steps.append("Deploy schema change behind a feature flag — enable for one consumer at a time")
    steps.append("Run ValidationRunner against new snapshot: python contracts/runner.py --contract-id ...")
    steps.append("Validate downstream pipeline outputs against expected distributions")
    steps.append("Monitor for anomalies 24 h post-deployment before enabling for all consumers")

    if has_rename or has_remove:
        steps.append("Remove deprecated aliases / tombstoned fields after migration window closes")

    return steps


def generate_rollback_plan() -> list[str]:
    return [
        "Restore previous schema snapshot by reverting the snapshot directory to last known-good commit",
        "Run: git revert <offending-commit> --no-edit  (do NOT force-push to main)",
        "Re-run ValidationRunner against restored snapshot to confirm it passes all checks",
        "Replay affected pipelines from the last valid checkpoint",
        "Notify downstream consumers that the rollback is complete and stable",
        "Open a post-mortem issue documenting the root cause and adding a CI guard to prevent recurrence",
    ]


# ── Temporal context ──────────────────────────────────────────────────────────

def temporal_context(old_path: Path, new_path: Path) -> dict:
    old_ts = extract_ts(old_path)
    new_ts = extract_ts(new_path)
    delta  = (new_ts - old_ts).total_seconds()
    return {
        "old_snapshot_timestamp":   old_ts.isoformat(),
        "new_snapshot_timestamp":   new_ts.isoformat(),
        "change_window_seconds":    int(delta),
        "change_window_human":      _seconds_to_human(int(delta)),
    }


def _seconds_to_human(s: int) -> str:
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m"
    if s < 86400:
        return f"{s // 3600}h"
    return f"{s // 86400}d"


# ── Report assembly ───────────────────────────────────────────────────────────

def build_report(
    contract_id:  str,
    old_path:     Path,
    new_path:     Path,
    new_contract: dict,
    changes:      list[dict],
    exact_diff:   dict,
) -> dict:
    breaking_fields = {c["field"] for c in changes if c["change_type"] == "BREAKING"}

    blast   = compute_blast_radius(new_contract, breaking_fields)
    summary = migration_summary(changes)
    compat  = overall_compatibility(changes)
    checklist = generate_migration_checklist(changes, blast)
    rollback  = generate_rollback_plan()
    consumers = compute_consumer_impact(new_contract, changes)
    temporal  = temporal_context(old_path, new_path)

    return {
        "contract_id":          contract_id,
        "old_snapshot":         old_path.name,
        "new_snapshot":         new_path.name,
        "overall_compatibility": compat,
        "changes":              changes,
        "migration_required":   compat["verdict"] != "FULLY_COMPATIBLE",
        "migration_summary":    summary,
        "blast_radius":         blast,
        "consumer_impact":      consumers,
        "migration_checklist":  checklist,
        "rollback_plan":        rollback,
        "exact_diff":           exact_diff,
        "temporal_context":     temporal,
        "generated_at":         datetime.now(tz=timezone.utc).isoformat(),
        "analyzer_version":     "3.0.0",
    }


# ── Output writers ────────────────────────────────────────────────────────────

def write_report(report: dict, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as fh:
        json.dump(report, fh, indent=2, default=str)
    print(f"[schema_analyzer] Report written → {output_path}")


def write_migration_impact(report: dict, contract_id: str, base_dir: Path) -> Path:
    ts  = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    cid = contract_id.replace("/", "_").replace(" ", "_")
    path = base_dir / f"migration_impact_{cid}_{ts}.json"
    write_report(report, path)
    return path


# ── CLI entry point ───────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 3 — Schema Evolution Analyzer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python contracts/schema_analyzer.py \\
      --contract-id week3-extractions \\
      --since "7 days ago" \\
      --output validation_reports/schema_evolution_week3.json

  python contracts/schema_analyzer.py \\
      --contract-id week5-events \\
      --output validation_reports/schema_evolution_week5.json
""",
    )
    parser.add_argument(
        "--contract-id", required=True,
        help="Contract ID matching a directory in schema_snapshots/",
    )
    parser.add_argument(
        "--since", default=None,
        help="Natural-language window filter, e.g. '7 days ago', '2 weeks ago'",
    )
    parser.add_argument(
        "--output", required=True,
        help="Path for the primary schema evolution report (JSON)",
    )

    args = parser.parse_args()

    project_root  = Path(__file__).resolve().parent.parent
    snapshot_root = project_root / "schema_snapshots" / args.contract_id
    output_path   = Path(args.output)
    if not output_path.is_absolute():
        output_path = project_root / output_path

    # ── 1. Parse --since ──────────────────────────────────────────────────────
    try:
        since_dt = parse_since(args.since)
    except ValueError as exc:
        print(f"[schema_analyzer] ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    # ── 2. Validate snapshot directory ────────────────────────────────────────
    if not snapshot_root.exists():
        print(
            f"[schema_analyzer] ERROR: snapshot directory not found: {snapshot_root}\n"
            f"  Run: python contracts/generator.py --contract-id {args.contract_id} ...",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        files = validate_snapshots(snapshot_root)
    except ValueError as exc:
        print(f"[schema_analyzer] ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    # ── 3. Choose consecutive pair ────────────────────────────────────────────
    try:
        old_path, new_path = choose_pair(files, since_dt)
    except ValueError as exc:
        print(f"[schema_analyzer] ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"[schema_analyzer] Diffing snapshots:")
    print(f"  OLD → {old_path.name}")
    print(f"  NEW → {new_path.name}")

    # ── 4. Load and validate ──────────────────────────────────────────────────
    old_raw = load_yaml(old_path)
    new_raw = load_yaml(new_path)

    try:
        validate_contract_structure(old_raw, old_path)
        validate_contract_structure(new_raw, new_path)
    except ValueError as exc:
        print(f"[schema_analyzer] ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    # ── 5. Normalize ──────────────────────────────────────────────────────────
    old_contract = normalize_contract(old_raw)
    new_contract = normalize_contract(new_raw)

    # ── 6. Diff ───────────────────────────────────────────────────────────────
    changes, exact_diff = diff_contracts(old_contract, new_contract)

    # ── 7. Build report ───────────────────────────────────────────────────────
    report = build_report(
        contract_id=args.contract_id,
        old_path=old_path,
        new_path=new_path,
        new_contract=new_contract,
        changes=changes,
        exact_diff=exact_diff,
    )

    # ── 8. Write outputs ──────────────────────────────────────────────────────
    write_report(report, output_path)

    migration_dir = project_root / "validation_reports"
    write_migration_impact(report, args.contract_id, migration_dir)

    # ── 9. Summary ────────────────────────────────────────────────────────────
    summary = report["migration_summary"]
    compat  = report["overall_compatibility"]
    print(
        f"\n[schema_analyzer] ── Summary ──────────────────────────────────────\n"
        f"  Contract:          {args.contract_id}\n"
        f"  Compatibility:     {compat['verdict']}\n"
        f"  Total changes:     {summary['total_changes']}\n"
        f"  Breaking:          {summary['breaking_changes']}\n"
        f"  Compatible:        {summary['compatible_changes']}\n"
        f"  Critical issues:   {summary['critical_issues']}\n"
        f"  Affected consumers:{report['blast_radius']['affected_consumers_count']}\n"
    )

    if compat["verdict"] == "BREAKING":
        print("[schema_analyzer] ⚠  BREAKING CHANGES DETECTED — migration required.")
        sys.exit(2)  # Non-zero so CI can catch it


if __name__ == "__main__":
    main()
