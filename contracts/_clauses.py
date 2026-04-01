"""
contracts/_clauses.py
Clause definitions and inference logic for Phase 1 data contract generation.

Two layers:
  Layer A — inferred from ydata/native profile
  Layer B — hardcoded canonical rules per source type
"""

from __future__ import annotations

import copy

SOURCE_WEEK1  = "week1"
SOURCE_WEEK3  = "week3"
SOURCE_WEEK4  = "week4"
SOURCE_WEEK5  = "week5"
SOURCE_TRACES = "traces"

# ── Canonical contract IDs per source type ─────────────────────────────────────

CANONICAL_CONTRACT_IDS: dict[str, str] = {
    SOURCE_WEEK1:  "week1-intent-records",
    SOURCE_WEEK3:  "week3-document-refinery-extractions",
    SOURCE_WEEK4:  "week4-lineage-snapshots",
    SOURCE_WEEK5:  "week5-event-stream",
    SOURCE_TRACES: "langsmith-traces",
}

# ── Canonical contract descriptions per source type ────────────────────────────

CANONICAL_DESCRIPTIONS: dict[str, str] = {
    SOURCE_WEEK1: (
        "Intent records produced by the Week 1 mutation-tracking pipeline. "
        "Each record captures a single tool invocation (apply_patch, bash, etc.) "
        "bound to a developer intent, with outcome, file path, and content hash. "
        "Downstream consumers use these records for attribution and audit."
    ),
    SOURCE_WEEK3: (
        "Document-refinery extraction output. Each record contains a doc_id "
        "(SHA-256 of the source document), an extraction_model identifier, and "
        "an extracted_facts array of {fact_id, fact, confidence} objects. "
        "Downstream cartographer (week4) ingests doc_id and extracted_facts as "
        "node metadata. confidence must remain in [0.0, 1.0]."
    ),
    SOURCE_WEEK4: (
        "Lineage graph snapshots produced by the Week 4 cartographer pipeline. "
        "Each snapshot captures the full node/edge graph at a point in time, "
        "keyed by snapshot_id and ISO timestamp. Used for drift detection and "
        "provenance tracing across pipeline runs."
    ),
    SOURCE_WEEK5: (
        "Event-stream records emitted by the Week 5 lending-decision pipeline. "
        "Each event captures a state transition (ApplicationSubmitted, "
        "CreditAnalysisCompleted, etc.) with a UUID event_id, stream_id, "
        "stream_position, actor_id, and nested payload. stream_position must "
        "increment monotonically within each stream_id."
    ),
    SOURCE_TRACES: (
        "LangSmith-compatible run traces exported from the agent orchestration "
        "layer. Each record is a chain/llm/tool run with inputs, outputs, token "
        "counts, cost, and timing. Used for latency profiling, cost attribution, "
        "and regression detection across pipeline versions."
    ),
}

# ── Layer B: Canonical hardcoded clauses ───────────────────────────────────────

CANONICAL_CLAUSES: dict[str, list[dict]] = {

    # ── Week 1 ─────────────────────────────────────────────────────────────────
    SOURCE_WEEK1: [
        {
            "rule":        "id_uuid_format",
            "field":       "id",
            "type":        "pattern",
            "pattern":     "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            "severity":    "BREAKING",
            "description": "id must be a valid UUID v4. Each intent record must be "
                           "globally unique.",
        },
        {
            "rule":        "id_not_null",
            "field":       "id",
            "type":        "structural",
            "check":       "not_null",
            "severity":    "BREAKING",
            "description": "id must be present on every record.",
        },
        {
            "rule":        "intent_id_not_null",
            "field":       "intentId",
            "type":        "structural",
            "check":       "not_null",
            "severity":    "BREAKING",
            "description": "intentId links the mutation to a developer goal. "
                           "Must be present for attribution.",
        },
        {
            "rule":        "timestamp_not_null",
            "field":       "timestamp",
            "type":        "structural",
            "check":       "not_null",
            "severity":    "BREAKING",
            "description": "timestamp is required for temporal ordering and replay.",
        },
        {
            "rule":        "tool_not_null",
            "field":       "tool",
            "type":        "structural",
            "check":       "not_null",
            "severity":    "BREAKING",
            "description": "tool identifies which agent action was taken.",
        },
        {
            "rule":        "content_hash_hex64",
            "field":       "contentHash",
            "type":        "pattern",
            "pattern":     "^[0-9a-f]{64}$",
            "severity":    "ERROR",
            "description": "contentHash must be a 64-character lowercase SHA-256 hex "
                           "digest of the file content at the time of mutation.",
        },
        {
            "rule":        "outcome_enum",
            "field":       "outcome",
            "type":        "accepted_values",
            "accepted_values": ["success", "failure", "partial"],
            "severity":    "ERROR",
            "description": "outcome must be one of the canonical result states.",
        },
        {
            "rule":        "mutation_type_enum",
            "field":       "mutationType",
            "type":        "accepted_values",
            "accepted_values": ["WRITE", "READ", "DELETE", "EXEC"],
            "severity":    "WARNING",
            "description": "mutationType must use the canonical enum for consistent "
                           "downstream filtering.",
        },
    ],

    # ── Week 3 ─────────────────────────────────────────────────────────────────
    SOURCE_WEEK3: [
        {
            "rule":        "doc_id_hex64",
            "field":       "doc_id",
            "type":        "pattern",
            "pattern":     "^[0-9a-f]{64}$",
            "severity":    "BREAKING",
            "description": "doc_id must be a 64-character lowercase SHA-256 hex digest. "
                           "Each document is identified by the SHA-256 hash of its content.",
        },
        {
            "rule":        "source_hash_hex64",
            "field":       "source_hash",
            "type":        "pattern",
            "pattern":     "^[0-9a-f]{64}$",
            "severity":    "BREAKING",
            "description": "source_hash is the SHA-256 digest of the raw source text "
                           "before extraction. Used to detect re-extraction of identical "
                           "content and enable deduplication.",
        },
        {
            "rule":        "extraction_model_pattern",
            "field":       "extraction_model",
            "type":        "pattern",
            "pattern":     "^(claude|gpt)-",
            "severity":    "ERROR",
            "description": "extraction_model must identify the LLM vendor prefix "
                           "(claude- or gpt-). Unknown prefixes indicate an undeclared "
                           "model was used.",
        },
        {
            "rule":        "confidence_range",
            "field":       "confidence",
            "type":        "range",
            "minimum":     0.0,
            "maximum":     1.0,
            "severity":    "BREAKING",
            "description": "confidence is the extractor's certainty score for a fact. "
                           "It must be a float in [0.0, 1.0]. Values outside this range "
                           "indicate a scale mismatch (e.g., 0–100 instead of 0–1).",
        },
        {
            "rule":        "confidence_non_zero_variance",
            "field":       "confidence",
            "type":        "statistical",
            "check":       "non_zero_variance",
            "severity":    "WARNING",
            "description": "If all confidence values are identical the extractor is likely "
                           "returning a hard-coded default. std=0 is a data quality signal.",
        },
        {
            "rule":        "fact_not_null",
            "field":       "fact",
            "type":        "structural",
            "check":       "not_null",
            "severity":    "BREAKING",
            "description": "fact is the extracted text fragment. It must be present and "
                           "non-empty for every row.",
        },
        {
            "rule":        "doc_id_not_null",
            "field":       "doc_id",
            "type":        "structural",
            "check":       "not_null",
            "severity":    "BREAKING",
            "description": "doc_id must be present on every row. It links every fact back "
                           "to its source document.",
        },
        {
            "rule":        "confidence_not_null",
            "field":       "confidence",
            "type":        "structural",
            "check":       "not_null",
            "severity":    "BREAKING",
            "description": "confidence must be present. Missing confidence cannot be "
                           "compared against threshold contracts.",
        },
        {
            "rule":        "fact_id_uuid",
            "field":       "extracted_facts.items.fact_id",
            "type":        "pattern",
            "pattern":     "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            "severity":    "ERROR",
            "description": "fact_id within each extracted_facts item must be a UUID v4. "
                           "Enables idempotent downstream upserts.",
        },
        # Forward-compat: entities field not in current data — aspirational spec
        {
            "rule":        "entities_type_enum",
            "field":       "entities",
            "type":        "accepted_values",
            "accepted_values": ["PERSON", "ORG", "LOCATION", "DATE", "MONEY", "FACT", "OTHER"],
            "severity":    "WARNING",
            "status":      "aspirational",
            "description": "Aspirational clause: once an 'entities' field is added to the "
                           "schema, entity types must use the canonical enum. Not enforced "
                           "until the field is present.",
        },
    ],

    # ── Week 4 ─────────────────────────────────────────────────────────────────
    SOURCE_WEEK4: [
        {
            "rule":        "snapshot_id_not_null",
            "field":       "snapshot_id",
            "type":        "structural",
            "check":       "not_null",
            "severity":    "BREAKING",
            "description": "snapshot_id must be present. It is the primary key for "
                           "lineage snapshots.",
        },
        {
            "rule":        "timestamp_not_null",
            "field":       "timestamp",
            "type":        "structural",
            "check":       "not_null",
            "severity":    "BREAKING",
            "description": "timestamp is required for ordering snapshots chronologically "
                           "and detecting drift.",
        },
        {
            "rule":        "node_count_positive",
            "field":       "node_count",
            "type":        "range",
            "minimum":     1,
            "maximum":     None,
            "severity":    "ERROR",
            "description": "node_count must be at least 1. A snapshot with 0 nodes "
                           "indicates a failed graph extraction.",
        },
        {
            "rule":        "edge_count_non_negative",
            "field":       "edge_count",
            "type":        "range",
            "minimum":     0,
            "maximum":     None,
            "severity":    "WARNING",
            "description": "edge_count must be >= 0. Negative edge counts indicate a "
                           "serialization error.",
        },
    ],

    # ── Week 5 ─────────────────────────────────────────────────────────────────
    SOURCE_WEEK5: [
        {
            "rule":        "event_id_uuid",
            "field":       "event_id",
            "type":        "pattern",
            "pattern":     "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            "severity":    "BREAKING",
            "description": "event_id must be a valid UUID v4. Each event must be globally "
                           "unique and immutable once written to the event log.",
        },
        {
            "rule":        "event_id_not_null",
            "field":       "event_id",
            "type":        "structural",
            "check":       "not_null",
            "severity":    "BREAKING",
            "description": "event_id must be present on every event record.",
        },
        {
            "rule":        "event_type_pascalcase",
            "field":       "event_type",
            "type":        "pattern",
            "pattern":     "^[A-Z][a-zA-Z0-9]+$",
            "severity":    "ERROR",
            "description": "event_type must follow PascalCase naming convention "
                           "(e.g., ApplicationSubmitted, CreditAnalysisCompleted). "
                           "Snake_case or lowercase variants indicate a pipeline schema drift.",
        },
        {
            "rule":        "event_type_enum",
            "field":       "event_type",
            "type":        "accepted_values",
            "accepted_values": "__from_profile__",   # replaced at generation time
            "severity":    "ERROR",
            "description": "event_type must be a known, declared event type. "
                           "Unknown event types indicate an undeclared producer.",
        },
        {
            "rule":        "stream_position_monotonic",
            "field":       "stream_position",
            "type":        "cross_field",
            "check":       "monotonic_per_group",
            "group_by":    "stream_id",
            "severity":    "ERROR",
            "description": "stream_position must increment by 1 within each stream_id. "
                           "Gaps or resets indicate a concurrent write collision or "
                           "event replay bug.",
        },
        {
            "rule":        "timestamp_gte_payload_at",
            "field":       "timestamp",
            "type":        "cross_field",
            "check":       "gte_payload_occurred_at",
            "severity":    "WARNING",
            "description": "The top-level timestamp (when the event was recorded) must be "
                           "greater than or equal to any payload.*_at field (when it occurred). "
                           "Violations indicate clock skew or mis-ordered fields.",
        },
        {
            "rule":        "timestamp_not_null",
            "field":       "timestamp",
            "type":        "structural",
            "check":       "not_null",
            "severity":    "BREAKING",
            "description": "Every event must have a timestamp. Missing timestamps make "
                           "temporal ordering and replay impossible.",
        },
        {
            "rule":        "source_system_not_null",
            "field":       "source_system",
            "type":        "structural",
            "check":       "not_null",
            "severity":    "BREAKING",
            "description": "source_system identifies which service emitted the event. "
                           "Required for attribution and blame-chain analysis.",
        },
        {
            "rule":        "schema_version_not_null",
            "field":       "schema_version",
            "type":        "structural",
            "check":       "not_null",
            "severity":    "ERROR",
            "description": "schema_version must be present so consumers can route to the "
                           "correct deserializer. Missing schema_version is a breaking "
                           "contract gap.",
        },
        {
            "rule":        "actor_id_not_null",
            "field":       "actor_id",
            "type":        "structural",
            "check":       "not_null",
            "severity":    "WARNING",
            "description": "actor_id identifies the agent or user who caused the event. "
                           "Required for audit trail and accountability.",
        },
    ],

    # ── Traces ─────────────────────────────────────────────────────────────────
    SOURCE_TRACES: [
        {
            "rule":        "id_uuid_format",
            "field":       "id",
            "type":        "pattern",
            "pattern":     "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            "severity":    "BREAKING",
            "description": "id must be a valid UUID v7/v4. Each run must be uniquely "
                           "addressable in LangSmith.",
        },
        {
            "rule":        "id_not_null",
            "field":       "id",
            "type":        "structural",
            "check":       "not_null",
            "severity":    "BREAKING",
            "description": "id must be present on every run record.",
        },
        {
            "rule":        "name_not_null",
            "field":       "name",
            "type":        "structural",
            "check":       "not_null",
            "severity":    "ERROR",
            "description": "name identifies the chain/tool/LLM step. Required for "
                           "latency attribution.",
        },
        {
            "rule":        "run_type_enum",
            "field":       "run_type",
            "type":        "accepted_values",
            "accepted_values": ["chain", "llm", "tool", "retriever", "embedding"],
            "severity":    "ERROR",
            "description": "run_type must be one of the LangSmith canonical run types.",
        },
        {
            "rule":        "start_time_not_null",
            "field":       "start_time",
            "type":        "structural",
            "check":       "not_null",
            "severity":    "BREAKING",
            "description": "start_time is required for latency calculation.",
        },
        {
            "rule":        "total_tokens_non_negative",
            "field":       "total_tokens",
            "type":        "range",
            "minimum":     0,
            "maximum":     None,
            "severity":    "WARNING",
            "description": "total_tokens must be >= 0. Negative values indicate a "
                           "serialization bug in the token counter.",
        },
        {
            "rule":        "total_cost_non_negative",
            "field":       "total_cost",
            "type":        "range",
            "minimum":     0.0,
            "maximum":     None,
            "severity":    "WARNING",
            "description": "total_cost must be >= 0.0. Negative cost indicates a "
                           "billing calculation error.",
        },
    ],
}


# ── Layer A: Infer clauses from profile ────────────────────────────────────────

def infer_clauses_from_profile(
    profile: dict[str, dict],
    source_type: str,
) -> list[dict]:
    """
    Convert a profile dict (from _profiler.profile_dataframe) into contract clauses,
    then merge with CANONICAL_CLAUSES for the given source_type.

    Canonical clauses take priority: any inferred clause whose (field, rule) pair
    matches a canonical clause is replaced by the canonical version.

    For week5 event_type_enum: replaces '__from_profile__' with observed enum values.
    """
    inferred = _infer_from_profile(profile)
    canonical = copy.deepcopy(CANONICAL_CLAUSES.get(source_type, []))

    # Replace __from_profile__ placeholder in event_type_enum
    if source_type == SOURCE_WEEK5:
        event_type_profile = profile.get("event_type", {})
        if event_type_profile.get("is_enum_candidate") and event_type_profile.get("enum_values"):
            for c in canonical:
                if c.get("accepted_values") == "__from_profile__":
                    c["accepted_values"] = event_type_profile["enum_values"]

    # Build index of canonical clauses by (field, rule)
    canonical_index = {(c["field"], c["rule"]): True for c in canonical}

    # Add inferred clauses not already covered by canonical
    merged = list(canonical)
    for clause in inferred:
        key = (clause["field"], clause["rule"])
        if key not in canonical_index:
            merged.append(clause)
            canonical_index[key] = True

    return merged


def _infer_from_profile(profile: dict[str, dict]) -> list[dict]:
    """Generate inferred clauses from profile stats."""
    clauses: list[dict] = []

    for col, stats in profile.items():
        # not_null
        if stats["null_fraction"] == 0.0:
            clauses.append({
                "rule":        f"{col}_not_null",
                "field":       col,
                "type":        "structural",
                "check":       "not_null",
                "severity":    "ERROR",
                "description": f"'{col}' has no null values in the observed data. "
                               f"Inferred as required.",
                "source":      "inferred",
            })

        # UUID format
        if stats.get("all_uuid"):
            clauses.append({
                "rule":        f"{col}_uuid_format",
                "field":       col,
                "type":        "pattern",
                "pattern":     "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
                "severity":    "ERROR",
                "description": f"'{col}' contains UUID v4 values in all observed rows. "
                               f"Inferred as UUID-format field.",
                "source":      "inferred",
            })

        # SHA-256 hex64 format
        if stats.get("all_hex64"):
            clauses.append({
                "rule":        f"{col}_hex64_format",
                "field":       col,
                "type":        "pattern",
                "pattern":     "^[0-9a-f]{64}$",
                "severity":    "ERROR",
                "description": f"'{col}' contains 64-char lowercase hex values in all "
                               f"observed rows. Inferred as SHA-256 digest field.",
                "source":      "inferred",
            })

        # PascalCase
        if stats.get("all_pascalcase"):
            clauses.append({
                "rule":        f"{col}_pascalcase",
                "field":       col,
                "type":        "pattern",
                "pattern":     "^[A-Z][a-zA-Z0-9]+$",
                "severity":    "WARNING",
                "description": f"'{col}' uses PascalCase in all observed values. "
                               f"Inferred naming convention.",
                "source":      "inferred",
            })

        # ISO datetime
        if stats.get("all_datetime"):
            clauses.append({
                "rule":        f"{col}_datetime_format",
                "field":       col,
                "type":        "structural",
                "check":       "datetime_format",
                "severity":    "WARNING",
                "description": f"'{col}' contains ISO-8601 datetime strings in all "
                               f"observed rows. Inferred as timestamp field.",
                "source":      "inferred",
            })

        # Enum
        if stats.get("is_enum_candidate") and stats.get("enum_values"):
            clauses.append({
                "rule":        f"{col}_accepted_values",
                "field":       col,
                "type":        "accepted_values",
                "accepted_values": stats["enum_values"],
                "severity":    "WARNING",
                "description": f"'{col}' has low cardinality ({stats['n_unique']} distinct values). "
                               f"Inferred as enum. Values outside this set indicate "
                               f"a new undeclared variant.",
                "source":      "inferred",
            })

        # Numeric range
        if stats["dtype"] in ("number", "integer") and stats["min"] is not None:
            clauses.append({
                "rule":        f"{col}_range",
                "field":       col,
                "type":        "range",
                "minimum":     stats["min"],
                "maximum":     stats["max"],
                "severity":    "WARNING",
                "description": f"'{col}' observed range is [{stats['min']}, {stats['max']}]. "
                               f"Values outside this range indicate unexpected input.",
                "source":      "inferred",
            })

        # Zero variance warning (numeric)
        if stats.get("zero_variance") and stats["dtype"] in ("number", "integer"):
            clauses.append({
                "rule":        f"{col}_zero_variance",
                "field":       col,
                "type":        "statistical",
                "check":       "non_zero_variance",
                "severity":    "WARNING",
                "description": f"'{col}' has zero variance (all values identical). "
                               f"This may indicate a hard-coded default value.",
                "source":      "inferred",
            })

    return clauses
