"""
contracts/generator.py
Phase 1 — Bitol v3 Data Contract Generator

CLI usage:
    python contracts/generator.py \\
        --source outputs/week3/extractions.jsonl \\
        --contract-id week3-extractions \\
        --lineage outputs/week4/lineage_snapshots.jsonl \\
        --output generated_contracts/

Produces:
    generated_contracts/{contract_id}.yaml       (Bitol ODCS v3, underscore filename)
    generated_contracts/{contract_id}_dbt.yml    (dbt schema.yml)
    schema_snapshots/{contract_id}/{utc_ts}.yaml (immutable snapshot)
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

# Ensure the project root is on sys.path so sibling imports work whether
# this file is run as `python contracts/generator.py` or `python -m contracts.generator`
_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from contracts._profiler import profile_dataframe                    # noqa: E402
from contracts._clauses  import (                                    # noqa: E402
    SOURCE_WEEK1, SOURCE_WEEK3, SOURCE_WEEK4, SOURCE_WEEK5, SOURCE_TRACES,
    CANONICAL_CONTRACT_IDS, CANONICAL_DESCRIPTIONS,
    infer_clauses_from_profile,
)

# ── Per-source contract metadata ───────────────────────────────────────────────

_TITLES: dict[str, str] = {
    SOURCE_WEEK1:  "Week 1 Intent Records",
    SOURCE_WEEK3:  "Week 3 Document Refinery — Extraction Records",
    SOURCE_WEEK4:  "Week 4 Lineage Snapshots",
    SOURCE_WEEK5:  "Week 5 Event Stream",
    SOURCE_TRACES: "LangSmith Trace Runs",
}

_OWNERS: dict[str, str] = {
    SOURCE_WEEK1:  "week1-team",
    SOURCE_WEEK3:  "week3-team",
    SOURCE_WEEK4:  "week4-team",
    SOURCE_WEEK5:  "week5-team",
    SOURCE_TRACES: "platform-team",
}

_TERMS_LIMITATIONS: dict[str, str] = {
    SOURCE_WEEK1: (
        "contentHash must be a SHA-256 hex digest (64 chars). "
        "mutationClass and mutationType must use declared enum values. "
        "Do not add new tools without updating the accepted_values clause."
    ),
    SOURCE_WEEK3: (
        "confidence must remain a float in [0.0, 1.0]. "
        "Do not change the scale to 0–100 — this breaks Week4 attribution weights. "
        "doc_id is a SHA-256 hex digest, not a UUID; do not reformat it."
    ),
    SOURCE_WEEK4: (
        "snapshot_id must be unique per run. "
        "Nodes and edges must not be empty arrays — a graph with zero nodes "
        "indicates a failed extraction and must not be written."
    ),
    SOURCE_WEEK5: (
        "stream_position must increment monotonically within each stream_id. "
        "schema_version must be present; coexistence of multiple schema versions "
        "in a single stream without a migration window is a breaking change."
    ),
    SOURCE_TRACES: (
        "end_time must be greater than start_time for all runs. "
        "total_tokens must equal prompt_tokens + completion_tokens. "
        "run_type must be one of: chain, llm, tool, retriever, embedding."
    ),
}

# Short model name used in SodaChecks header: "checks for {short_name}:"
_SODA_MODEL: dict[str, str] = {
    SOURCE_WEEK1:  "intent_records",
    SOURCE_WEEK3:  "extractions",
    SOURCE_WEEK4:  "lineage_snapshots",
    SOURCE_WEEK5:  "events",
    SOURCE_TRACES: "runs",
}
from contracts._lineage  import load_latest_lineage, enrich_with_git  # noqa: E402
from contracts._llm      import annotate_with_llm                    # noqa: E402
from contracts._dbt      import generate_dbt_yaml                    # noqa: E402


# ── Source detection ───────────────────────────────────────────────────────────

def detect_source_type(first_record: dict) -> str:
    keys = set(first_record.keys())
    if "doc_id" in keys and "extracted_facts" in keys:
        return SOURCE_WEEK3
    if "event_id" in keys and "stream_id" in keys:
        return SOURCE_WEEK5
    if "snapshot_id" in keys and "nodes" in keys and "edges" in keys:
        return SOURCE_WEEK4
    if "intentId" in keys and "mutationClass" in keys:
        return SOURCE_WEEK1
    if "run_type" in keys and ("prompt_tokens" in keys or "total_tokens" in keys):
        return SOURCE_TRACES
    raise ValueError(
        f"Cannot detect source type from keys: {sorted(keys)}. "
        "Expected week1, week3, week4, week5, or traces."
    )


# ── Load + flatten ─────────────────────────────────────────────────────────────

def load_jsonl(path: str) -> list[dict]:
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def flatten_for_profile(records: list[dict], source_type: str) -> pd.DataFrame:
    """
    Flatten nested JSONL for profiling only.
    The published contract preserves canonical nested schema.

    Week1:  top-level fields only (toolArgsSnapshot excluded).
    Week3:  explode extracted_facts[] — one profiling row per fact.
    Week4:  top-level scalar fields only (nodes/edges excluded).
    Week5:  flatten metadata.* into metadata.{key} columns.
    Traces: top-level scalar fields only (inputs/outputs excluded).
    """
    if source_type == SOURCE_WEEK1:
        rows = []
        for rec in records:
            rows.append({k: v for k, v in rec.items() if k != "toolArgsSnapshot"})
        return pd.DataFrame(rows)

    if source_type == SOURCE_WEEK3:
        rows = []
        for rec in records:
            doc_id = rec.get("doc_id", "")
            for fact in rec.get("extracted_facts", []):
                rows.append({
                    "doc_id":     doc_id,
                    "fact":       fact.get("fact", ""),
                    "confidence": fact.get("confidence"),
                })
        return pd.DataFrame(rows)

    if source_type == SOURCE_WEEK4:
        rows = []
        for rec in records:
            row = {k: v for k, v in rec.items() if k not in ("nodes", "edges")}
            row["node_count"] = len(rec.get("nodes", []))
            row["edge_count"] = len(rec.get("edges", []))
            rows.append(row)
        return pd.DataFrame(rows)

    if source_type == SOURCE_WEEK5:
        rows = []
        for rec in records:
            row = {k: v for k, v in rec.items() if k not in ("metadata", "payload")}
            for mk, mv in (rec.get("metadata") or {}).items():
                row[f"metadata.{mk}"] = mv
            rows.append(row)
        return pd.DataFrame(rows)

    if source_type == SOURCE_TRACES:
        rows = []
        for rec in records:
            row = {k: v for k, v in rec.items() if k not in ("inputs", "outputs", "tags")}
            rows.append(row)
        return pd.DataFrame(rows)

    raise ValueError(f"Unknown source_type: {source_type}")


# ── Schema block construction ──────────────────────────────────────────────────

def _build_schema_block(source_type: str, profile: dict[str, dict]) -> dict:
    """
    Build the `schema:` block that mirrors the canonical JSONL record structure.
    For nested types (arrays, objects) we emit structured sub-schemas.
    """
    if source_type == SOURCE_WEEK3:
        return {
            "doc_id": {
                "type":        "string",
                "pattern":     "^[a-f0-9]{64}$",
                "required":    True,
                "unique":      True,
                "description": (
                    "Primary key. SHA-256 hex digest (64 lowercase hex chars) of the "
                    "source document content. Stable across re-extractions of the same "
                    "source. NOT a UUID — do not validate as uuid format."
                ),
            },
            "source_hash": {
                "type":        "string",
                "pattern":     "^[a-f0-9]{64}$",
                "required":    True,
                "description": "SHA-256 of the source file. Changes iff the source content changes.",
            },
            "extracted_facts": {
                "type":     "array",
                "required": True,
                "items": {
                    "confidence": {
                        "type":    "number",
                        "minimum": 0.0,
                        "maximum": 1.0,
                        "required": True,
                        "description": (
                            "Extraction certainty score in [0.0, 1.0]. "
                            "1.0 = fully confident, 0.0 = uncertain."
                        ),
                    },
                    "fact_id": {
                        "type":        "string",
                        "format":      "uuid",
                        "unique":      True,
                        "description": "UUID v4 identifying this fact. Enables idempotent upserts.",
                    },
                    "fact": {
                        "type":     "string",
                        "required": True,
                        "description": "Extracted text fragment from the source document.",
                    },
                },
            },
            "extraction_model": {
                "type":        "string",
                "required":    True,
                "description": "Model identifier. Must match pattern claude-* or gpt-*.",
                "pattern":     "^(claude|gpt)-",
            },
            "extracted_at": {
                "type":        "string",
                "format":      "date-time",
                "required":    False,
                "nullable":    True,
                "description": (
                    "ISO-8601 timestamp of when extraction completed. "
                    "Used for freshness SLA monitoring. Nullable until all "
                    "pipeline versions emit this field."
                ),
            },
            "processing_time_ms": {
                "type":        "integer",
                "required":    False,
                "nullable":    True,
                "minimum":     0,
                "description": (
                    "Wall-clock extraction duration in milliseconds. "
                    "Used for latency SLA contracts. Nullable until instrumented."
                ),
            },
            "entities": {
                "type":     "array",
                "required": False,
                "nullable": True,
                "description": (
                    "Named entities identified in the document (PERSON, ORG, etc.). "
                    "Aspirational field — not present in current data but required "
                    "by canonical schema for downstream entity-resolution pipeline."
                ),
                "items": {
                    "entity_id": {
                        "type":   "string",
                        "format": "uuid",
                    },
                    "type": {
                        "type": "string",
                        "enum": ["PERSON", "ORG", "LOCATION", "DATE", "MONEY", "FACT", "OTHER"],
                    },
                    "text": {
                        "type": "string",
                    },
                },
            },
            "entity_refs": {
                "type":     "array",
                "required": False,
                "nullable": True,
                "description": (
                    "Cross-references linking extracted_facts items to entities items "
                    "via {fact_id, entity_id} pairs. Aspirational field."
                ),
                "items": {
                    "fact_id":   {"type": "string", "format": "uuid"},
                    "entity_id": {"type": "string", "format": "uuid"},
                },
            },
        }

    if source_type == SOURCE_WEEK5:
        return {
            "event_id": {
                "type":     "string",
                "format":   "uuid",
                "required": True,
                "nullable": False,
            },
            "event_type": {
                "type":     "string",
                "required": True,
                "nullable": False,
            },
            "stream_id": {
                "type":     "string",
                "required": True,
                "nullable": False,
            },
            "stream_position": {
                "type":     "integer",
                "required": True,
                "nullable": False,
            },
            "timestamp": {
                "type":     "string",
                "format":   "date-time",
                "required": True,
                "nullable": False,
            },
            "source_system": {
                "type":     "string",
                "required": True,
                "nullable": False,
            },
            "actor_id": {
                "type":     "string",
                "required": False,
                "nullable": True,
            },
            "schema_version": {
                "type":     "string",
                "required": True,
                "nullable": False,
            },
            "payload": {
                "type":     "object",
                "required": False,
                "nullable": True,
            },
            "metadata": {
                "type":     "object",
                "required": False,
                "nullable": True,
            },
        }

    if source_type == SOURCE_WEEK1:
        return {
            "id": {
                "type":     "string",
                "format":   "uuid",
                "required": True,
                "nullable": False,
            },
            "timestamp": {
                "type":     "string",
                "format":   "date-time",
                "required": True,
                "nullable": False,
            },
            "tool": {
                "type":     "string",
                "required": True,
                "nullable": False,
            },
            "intentId": {
                "type":     "string",
                "required": True,
                "nullable": False,
            },
            "mutationClass": {
                "type":     "string",
                "required": True,
                "nullable": False,
            },
            "mutationType": {
                "type":     "string",
                "required": True,
                "nullable": False,
            },
            "filePath": {
                "type":     "string",
                "required": False,
                "nullable": True,
            },
            "contentHash": {
                "type":     "string",
                "format":   "hex64-sha256",
                "required": False,
                "nullable": True,
            },
            "outcome": {
                "type":     "string",
                "required": True,
                "nullable": False,
            },
            "revisionId": {
                "type":     "string",
                "required": False,
                "nullable": True,
            },
            "fileSizeBytes": {
                "type":     "integer",
                "required": False,
                "nullable": True,
            },
        }

    if source_type == SOURCE_WEEK4:
        return {
            "snapshot_id": {
                "type":     "string",
                "required": True,
                "nullable": False,
            },
            "timestamp": {
                "type":     "string",
                "format":   "date-time",
                "required": True,
                "nullable": False,
            },
            "nodes": {
                "type":     "array",
                "required": True,
                "nullable": False,
                "items": {
                    "properties": {
                        "id":   {"type": "string"},
                        "type": {"type": "string"},
                    }
                },
            },
            "edges": {
                "type":     "array",
                "required": True,
                "nullable": False,
                "items": {
                    "properties": {
                        "source":   {"type": "string"},
                        "target":   {"type": "string"},
                        "relation": {"type": "string"},
                    }
                },
            },
        }

    if source_type == SOURCE_TRACES:
        return {
            "id": {
                "type":     "string",
                "format":   "uuid",
                "required": True,
                "nullable": False,
            },
            "name": {
                "type":     "string",
                "required": True,
                "nullable": False,
            },
            "run_type": {
                "type":     "string",
                "required": True,
                "nullable": False,
                "enum":     ["chain", "llm", "tool", "retriever", "embedding"],
            },
            "start_time": {
                "type":     "string",
                "format":   "date-time",
                "required": True,
                "nullable": False,
            },
            "end_time": {
                "type":     "string",
                "format":   "date-time",
                "required": False,
                "nullable": True,
            },
            "prompt_tokens": {
                "type":     "integer",
                "required": False,
                "nullable": True,
            },
            "completion_tokens": {
                "type":     "integer",
                "required": False,
                "nullable": True,
            },
            "total_tokens": {
                "type":     "integer",
                "required": False,
                "nullable": True,
            },
            "total_cost": {
                "type":     "number",
                "required": False,
                "nullable": True,
            },
            "error": {
                "type":     "string",
                "required": False,
                "nullable": True,
            },
            "inputs": {
                "type":     "object",
                "required": False,
                "nullable": True,
            },
            "outputs": {
                "type":     "object",
                "required": False,
                "nullable": True,
            },
        }

    # Fallback: build from profile
    fields: dict = {}
    for col, stats in profile.items():
        fields[col] = {
            "type":     stats["dtype"],
            "required": stats["null_fraction"] == 0.0,
            "nullable": stats["null_fraction"] > 0.0,
        }
    return fields


# ── Contract assembly ──────────────────────────────────────────────────────────

def build_bitol_contract(
    contract_id: str,
    source_type: str,
    source_path: str,
    profile: dict[str, dict],
    clauses: list[dict],
    lineage_info: dict | None,
    llm_annotations: dict,
) -> dict:
    """Construct the Bitol ODCS v3 contract dict."""
    now_iso = datetime.now(timezone.utc).isoformat()

    # Use canonical contract ID / title / owner from spec
    canonical_id = CANONICAL_CONTRACT_IDS.get(source_type, contract_id)
    description  = CANONICAL_DESCRIPTIONS.get(source_type, "")
    title        = _TITLES.get(source_type, contract_id.replace("-", " ").title())
    owner        = _OWNERS.get(source_type, "contract-enforcer-platform")
    soda_model   = _SODA_MODEL.get(source_type, contract_id.replace("-", "_"))

    # Separate quality clauses by type
    structural  = [c for c in clauses if c["type"] in ("structural", "pattern", "accepted_values")]
    statistical = [c for c in clauses if c["type"] == "statistical"]
    cross_field = [c for c in clauses if c["type"] == "cross_field"]

    # Quality SodaChecks block (plain-string list, matches spec format)
    soda_checks = _build_soda_checks(source_type)

    # Lineage block: expose only upstream/downstream (no snapshot metadata)
    if lineage_info:
        lineage_block = {
            "upstream":   lineage_info.get("upstream", []),
            "downstream": lineage_info.get("downstream", []),
        }
        # Attach git provenance as a sub-key, not top-level noise
        git_meta = {k: lineage_info[k] for k in
                    ("git_commit", "git_author", "git_committed_at", "git_message")
                    if k in lineage_info}
        if git_meta:
            lineage_block["git"] = git_meta
    else:
        lineage_block = {"upstream": [], "downstream": []}

    # Schema block (canonical nested structure)
    schema_block = _build_schema_block(source_type, profile)

    return {
        "kind":       "DataContract",
        "apiVersion": "v3.0.0",
        "id":         canonical_id,

        "info": {
            "title":       title,
            "version":     "1.0.0",
            "owner":       owner,
            "description": description,
            "generatedAt": now_iso,
            "sourceFile":  source_path,
            "sourceType":  source_type,
        },

        "servers": {
            "local": {
                "type":   "local",
                "path":   source_path,
                "format": "jsonl",
            }
        },

        "terms": {
            "usage":       "Internal inter-system data contract. Do not publish.",
            "limitations": _TERMS_LIMITATIONS.get(source_type, "No additional limitations."),
        },

        "schema": schema_block,

        "quality": {
            "type":          "SodaChecks",
            "specification": {"checks for " + soda_model: soda_checks},
            "structural":    structural,
            "statistical":   statistical,
            "crossField":    cross_field,
        },

        "lineage": lineage_block,

        "llm_annotations": llm_annotations,
    }


def _build_soda_checks(source_type: str) -> list:
    """Build machine-checkable SodaChecks as plain strings (matches spec format)."""
    checks: list[str] = ["row_count >= 1"]

    if source_type == SOURCE_WEEK3:
        checks += [
            # Top-level field checks (SodaChecks operates on top-level columns)
            "missing_count(doc_id) = 0",
            "duplicate_count(doc_id) = 0",
            # extracted_facts is a nested array — check presence at the top level
            "missing_count(extracted_facts) = 0",
            # extraction_model must be present (model provenance)
            "missing_count(extraction_model) = 0",
            # NOTE: fact and confidence are nested inside extracted_facts[].
            # SodaChecks cannot address nested fields directly.
            # Use the structural quality rules below for per-item validation.
            # Statistical bounds on confidence (evaluated after flattening):
            "min(confidence) >= 0.0",
            "max(confidence) <= 1.0",
        ]

    elif source_type == SOURCE_WEEK5:
        checks += [
            "missing_count(event_id) = 0",
            "duplicate_count(event_id) = 0",
            "missing_count(event_type) = 0",
            "missing_count(timestamp) = 0",
            "missing_count(source_system) = 0",
            "missing_count(schema_version) = 0",
        ]

    elif source_type == SOURCE_WEEK1:
        checks += [
            "missing_count(id) = 0",
            "duplicate_count(id) = 0",
            "missing_count(intentId) = 0",
            "missing_count(timestamp) = 0",
            "missing_count(tool) = 0",
        ]

    elif source_type == SOURCE_WEEK4:
        checks += [
            "missing_count(snapshot_id) = 0",
            "missing_count(timestamp) = 0",
        ]

    elif source_type == SOURCE_TRACES:
        checks += [
            "missing_count(id) = 0",
            "duplicate_count(id) = 0",
            "missing_count(name) = 0",
            "missing_count(run_type) = 0",
            "missing_count(start_time) = 0",
        ]

    return checks


# ── Write outputs ──────────────────────────────────────────────────────────────

def _safe_filename(contract_id: str) -> str:
    """Convert contract-id (hyphens) to safe filename (underscores)."""
    return contract_id.replace("-", "_")


def write_outputs(
    bitol: dict,
    dbt: dict,
    contract_id: str,
    output_dir: str | None = None,
) -> dict[str, str]:
    """Write Bitol YAML, dbt YAML, and schema snapshot. Return paths dict."""
    out = Path(output_dir) if output_dir else Path("generated_contracts")
    out.mkdir(parents=True, exist_ok=True)

    safe_name = _safe_filename(contract_id)

    # 1. Bitol YAML
    bitol_path = out / f"{safe_name}.yaml"
    with bitol_path.open("w") as f:
        yaml.dump(bitol, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    # 2. dbt YAML
    dbt_path = out / f"{safe_name}_dbt.yml"
    with dbt_path.open("w") as f:
        yaml.dump(dbt, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    # 3. Schema snapshot
    utc_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snap_dir = Path("schema_snapshots") / safe_name
    snap_dir.mkdir(parents=True, exist_ok=True)
    snap_path = snap_dir / f"{utc_ts}.yaml"
    shutil.copy(bitol_path, snap_path)

    return {
        "bitol_yaml":    str(bitol_path.resolve()),
        "dbt_yaml":      str(dbt_path.resolve()),
        "snapshot_yaml": str(snap_path.resolve()),
    }


# ── Main orchestrator ──────────────────────────────────────────────────────────

def generate_contract(
    source: str,
    contract_id: str,
    lineage: str | None = None,
    output: str | None = None,
) -> dict[str, str]:
    """
    Full generation pipeline. Returns dict of written file paths.

    Steps:
      A  load JSONL
      B  detect source type
      C  flatten for profiling
      D  profile dataframe
      E  infer + merge clauses
      F  load + enrich lineage
      G  LLM annotation (graceful fallback)
      H  build Bitol contract
      I  build dbt YAML
      J  write outputs
    """
    print(f"[generator] Loading {source} ...")
    records = load_jsonl(source)
    if not records:
        raise ValueError(f"No records found in {source}")

    # B — detect source type
    source_type = detect_source_type(records[0])
    print(f"[generator] Detected source type: {source_type} ({len(records)} records)")

    # C — flatten for profiling
    df = flatten_for_profile(records, source_type)
    print(f"[generator] Flattened to {len(df)} rows × {len(df.columns)} columns for profiling")

    # D — profile
    print("[generator] Profiling dataframe ...")
    prof = profile_dataframe(df, minimal=True)
    print(f"[generator] Profiled {len(prof)} columns")

    # E — infer + merge clauses
    clauses = infer_clauses_from_profile(prof, source_type)
    print(f"[generator] {len(clauses)} clauses (canonical + inferred)")

    # F — lineage
    lineage_info: dict | None = None
    if lineage:
        lineage_info = load_latest_lineage(lineage)
        if lineage_info:
            lineage_info = enrich_with_git(lineage_info)
            print(f"[generator] Lineage loaded: snapshot {lineage_info.get('snapshot_id')}")
        else:
            print("[generator] Lineage file not found or empty — continuing without lineage")
    else:
        print("[generator] No lineage path provided — continuing without lineage")

    # G — LLM annotation
    print("[generator] Attempting LLM annotation ...")
    llm_annotations = annotate_with_llm(clauses, prof, source_type, contract_id)
    print(f"[generator] LLM annotation status: {llm_annotations['status']}")

    # H — build Bitol contract
    bitol = build_bitol_contract(
        contract_id=contract_id,
        source_type=source_type,
        source_path=source,
        profile=prof,
        clauses=clauses,
        lineage_info=lineage_info,
        llm_annotations=llm_annotations,
    )

    # I — build dbt YAML
    model_name = contract_id.replace("-", "_")
    dbt = generate_dbt_yaml(contract_id, model_name, clauses)

    # J — write outputs
    paths = write_outputs(bitol, dbt, contract_id, output)
    for label, path in paths.items():
        print(f"[generator] {label}: {path}")

    return paths


# ── Backwards-compatible stub (used by tests/test_smoke.py) ───────────────────

def generate_sample_contract(name: str) -> str:
    """Generate a minimal sample YAML contract. Kept for smoke test compatibility."""
    contract = {
        "kind":          "DataContract",
        "apiVersion":    "v3.0.0",
        "contract_name": name,
        "version":       "1.0.0",
        "schema": {
            "type": "object",
            "properties": {
                "id":   {"type": "integer"},
                "data": {"type": "string"},
            },
            "required": ["id", "data"],
        },
    }
    os.makedirs("generated_contracts", exist_ok=True)
    file_path = f"generated_contracts/{name}_contract.yaml"
    with open(file_path, "w") as f:
        yaml.dump(contract, f)
    print(f"Contract generated at {file_path}")
    return file_path


# ── CLI entry point ────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="generator",
        description="Generate a Bitol v3 data contract from a JSONL source file",
    )
    p.add_argument("--source",      required=True,
                   help="Path to source JSONL")
    p.add_argument("--contract-id", required=False, dest="contract_id", default=None,
                   help="Contract identifier (inferred from source filename if omitted)")
    p.add_argument("--lineage",     required=False, default=None,
                   help="Path to week4/lineage_snapshots.jsonl (optional)")
    p.add_argument("--output",      required=False, default=None,
                   help="Output directory (default: generated_contracts/)")
    return p


def main() -> None:
    args = build_parser().parse_args()
    if args.contract_id:
        contract_id = args.contract_id
    else:
        src = Path(args.source)
        # Include parent dir name so "week3/extractions.jsonl" → "week3-extractions"
        parent = src.parent.name
        stem   = src.stem
        contract_id = f"{parent}-{stem}" if parent not in (".", "outputs", "") else stem
        contract_id = contract_id.replace("_", "-")
    generate_contract(
        source=args.source,
        contract_id=contract_id,
        lineage=args.lineage,
        output=args.output,
    )


if __name__ == "__main__":
    main()
