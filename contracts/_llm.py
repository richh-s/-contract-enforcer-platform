"""
contracts/_llm.py
Optional LLM annotation step for Phase 1 contract generation.

Sends ambiguous field context to Claude/OpenAI and requests
plain-English descriptions and business rules. Gracefully skips
if no API key is configured or if the SDK is not installed.

Fields that are never sent to the LLM (obvious by name/pattern):
    - *_id fields
    - *_at / *_time fields
    - confidence, schema_version, event_version, stream_position
    - doc_id, event_id, stream_id
"""

from __future__ import annotations

import importlib
import json
import os
from typing import Optional

_SKIP_FIELDS = {
    "doc_id", "event_id", "stream_id", "stream_position", "global_position",
    "confidence", "schema_version", "event_version", "timestamp", "start_time",
    "end_time", "recorded_at", "occurred_at",
}


def annotate_with_llm(
    clauses: list[dict],
    profile: dict[str, dict],
    source_type: str,
    contract_id: str,
) -> dict:
    """
    Attempt LLM annotation for ambiguous fields.

    Returns:
    {
        "status":      "ok" | "skipped" | "error",
        "model":       str | None,
        "annotations": list[dict] | None,
        "reason":      str | None,
    }
    """
    # Identify ambiguous fields (not obviously typed from name/pattern)
    ambiguous = _get_ambiguous_fields(profile, clauses)
    if not ambiguous:
        return {
            "status":      "skipped",
            "reason":      "no ambiguous fields requiring annotation",
            "model":       None,
            "annotations": [],
        }

    # Check API key
    api_key_anthropic = os.environ.get("ANTHROPIC_API_KEY", "")
    api_key_openai    = os.environ.get("OPENAI_API_KEY", "")

    if not api_key_anthropic and not api_key_openai:
        return {
            "status":      "skipped",
            "reason":      "missing_api_key",
            "model":       None,
            "annotations": [],
        }

    # Try Anthropic first, then OpenAI
    if api_key_anthropic:
        return _call_anthropic(api_key_anthropic, ambiguous, source_type, contract_id)
    return _call_openai(api_key_openai, ambiguous, source_type, contract_id)


def _get_ambiguous_fields(profile: dict, clauses: list[dict]) -> list[dict]:
    """
    Return fields that are not obviously typed and not in the skip list.
    Each entry is: {field, dtype, sample_values, adjacent_fields}
    """
    # Fields already covered by canonical/inferred clauses with clear semantics
    covered = {c["field"] for c in clauses if c["type"] in ("pattern", "accepted_values")}
    all_fields = list(profile.keys())

    ambiguous = []
    for field in all_fields:
        # Skip obvious fields
        if field in _SKIP_FIELDS:
            continue
        if field.endswith("_id") or field.endswith("_at") or field.endswith("_time"):
            continue
        if field in covered:
            continue

        stats = profile[field]
        # Only flag string fields with non-trivial cardinality
        if stats["dtype"] != "string":
            continue
        if stats["is_enum_candidate"]:
            continue  # Already handled by accepted_values inference

        ambiguous.append({
            "field":           field,
            "dtype":           stats["dtype"],
            "sample_values":   stats["sample_values"][:5],
            "adjacent_fields": all_fields,
        })

    return ambiguous[:8]  # Limit to 8 fields per call to control token cost


def _build_prompt(ambiguous: list[dict], source_type: str, contract_id: str) -> str:
    field_summaries = "\n".join(
        f"- field: {a['field']}\n"
        f"  dtype: {a['dtype']}\n"
        f"  sample_values: {a['sample_values']}\n"
        f"  adjacent_fields: {a['adjacent_fields']}"
        for a in ambiguous
    )
    return (
        f"You are a data contract engineer reviewing a dataset for contract: {contract_id} "
        f"(source type: {source_type}).\n\n"
        f"For each field below, provide:\n"
        f"1. A plain-English description (1–2 sentences, understandable by a non-engineer)\n"
        f"2. A validation rule as a simple boolean expression\n"
        f"3. Any cross-field relationship you can infer\n\n"
        f"Fields:\n{field_summaries}\n\n"
        f"Respond ONLY with a JSON array of objects matching exactly:\n"
        f'[{{"field": "name", "description": "...", "validation_rule": "...", '
        f'"cross_field_note": "..."}}]\n'
        f"Do not include any text outside the JSON array."
    )


def _call_anthropic(api_key: str, ambiguous: list[dict], source_type: str, contract_id: str) -> dict:
    try:
        anthropic = importlib.import_module("anthropic")
    except ImportError:
        return {"status": "skipped", "reason": "anthropic package not installed",
                "model": None, "annotations": []}
    try:
        client = anthropic.Anthropic(api_key=api_key)
        prompt = _build_prompt(ambiguous, source_type, contract_id)
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=600,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text.strip()
        annotations = json.loads(raw)
        return {"status": "ok", "model": "claude-haiku-4-5-20251001",
                "annotations": annotations, "reason": None}
    except Exception as e:
        return {"status": "error", "reason": str(e), "model": None, "annotations": []}


def _call_openai(api_key: str, ambiguous: list[dict], source_type: str, contract_id: str) -> dict:
    try:
        openai = importlib.import_module("openai")
    except ImportError:
        return {"status": "skipped", "reason": "openai package not installed",
                "model": None, "annotations": []}
    try:
        client = openai.OpenAI(api_key=api_key)
        prompt = _build_prompt(ambiguous, source_type, contract_id)
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=600,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.choices[0].message.content.strip()
        annotations = json.loads(raw)
        return {"status": "ok", "model": "gpt-4o-mini",
                "annotations": annotations, "reason": None}
    except Exception as e:
        return {"status": "error", "reason": str(e), "model": None, "annotations": []}
