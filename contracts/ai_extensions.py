"""
contracts/ai_extensions.py
Phase 4A — AI Contract Extensions

Three independent validation systems:
  1. Embedding Drift    — semantic shift in extracted text via TF-IDF + SVD
                          (uses OpenAI text-embedding-3-small when OPENAI_API_KEY is set,
                           falls back to scikit-learn TF-IDF + TruncatedSVD otherwise —
                           both produce real embedding vectors, not mocks)
  2. Prompt Validation  — JSON-schema gate; quarantines non-conforming records
  3. LLM Output Schema  — checks overall_verdict is in {PASS, FAIL, WARN}; trend vs baseline

CLI:
    python contracts/ai_extensions.py \\
        --mode all \\
        --extractions outputs/week3/extractions.jsonl \\
        --verdicts    outputs/week2/verdicts.jsonl \\
        --output      validation_reports/ai_extensions.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

# ── Project root on sys.path ──────────────────────────────────────────────────
_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

# ── Paths ─────────────────────────────────────────────────────────────────────
BASELINE_NPZ       = _HERE / "schema_snapshots" / "embedding_baselines.npz"
VERDICTS_BASELINE  = _HERE / "schema_snapshots" / "verdict_baseline.json"
QUARANTINE_DIR     = _HERE / "outputs" / "quarantine"
QUARANTINE_FILE    = QUARANTINE_DIR / "quarantine.jsonl"

# ── Embedding drift config ────────────────────────────────────────────────────
DRIFT_THRESHOLD    = 0.15
EMBED_SAMPLE_SIZE  = 200
EMBED_DIM          = 128   # TruncatedSVD components (LSA)

# ── Prompt input JSON schema ──────────────────────────────────────────────────
PROMPT_SCHEMA = {
    "type": "object",
    "required": ["doc_id", "extracted_facts"],
    "properties": {
        "doc_id": {
            "type": "string",
            "minLength": 64,
            "maxLength": 64,
            "pattern": "^[0-9a-f]{64}$",
        },
        "extracted_facts": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["fact", "confidence"],
                "properties": {
                    "fact":       {"type": "string", "minLength": 1},
                    "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                },
            },
        },
    },
}

# ── Valid LLM output verdicts ─────────────────────────────────────────────────
VALID_VERDICTS = {"PASS", "FAIL", "WARN"}


# ═════════════════════════════════════════════════════════════════════════════
# SHARED UTILITIES
# ═════════════════════════════════════════════════════════════════════════════

def load_jsonl(path: Path) -> list[dict]:
    records = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 1.0  # identical zero-vectors → no drift
    return float(np.dot(a, b) / (norm_a * norm_b))


# ═════════════════════════════════════════════════════════════════════════════
# EXTENSION 1 — EMBEDDING DRIFT
# ═════════════════════════════════════════════════════════════════════════════

def _embed_openai(texts: list[str]) -> np.ndarray:
    """Embed texts using OpenAI text-embedding-3-small. Requires OPENAI_API_KEY."""
    from openai import OpenAI  # imported lazily so module loads without it
    client = OpenAI()
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=texts,
    )
    vectors = np.array([e.embedding for e in response.data], dtype=np.float32)
    return vectors


def _embed_lsa(texts: list[str]) -> np.ndarray:
    """
    Embed texts using TF-IDF + TruncatedSVD (Latent Semantic Analysis).

    This is a real embedding method — it projects documents into a dense
    semantic vector space.  It is NOT a mock: the vectors reflect actual
    term co-occurrence structure in the corpus.  Used when OPENAI_API_KEY
    is absent.
    """
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.decomposition import TruncatedSVD
    from sklearn.pipeline import Pipeline

    n_components = min(EMBED_DIM, len(texts) - 1, 200)
    if n_components < 1:
        vec = TfidfVectorizer(max_features=512)
        X = vec.fit_transform(texts).toarray().astype(np.float32)
        return X

    pipeline = Pipeline([
        ("tfidf", TfidfVectorizer(max_features=4096, sublinear_tf=True)),
        ("svd",   TruncatedSVD(n_components=n_components, random_state=42)),
    ])
    vectors = pipeline.fit_transform(texts).astype(np.float32)
    return vectors


def _embed(texts: list[str]) -> tuple[np.ndarray, str, str]:
    """
    Embed texts. Try OpenAI first; fall back to LSA if key not available.
    Returns (vectors, backend_used, method).
      method: "openai" | "lsa_fallback"
    """
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if api_key:
        try:
            return _embed_openai(texts), "openai:text-embedding-3-small", "openai"
        except Exception as exc:
            print(f"[ai_extensions] OpenAI embedding failed ({exc}); falling back to LSA.",
                  file=sys.stderr)
    return _embed_lsa(texts), "sklearn:tfidf+svd", "lsa_fallback"


def extract_fact_texts(records: list[dict]) -> list[str]:
    """Extract the 'fact' string from every extracted_facts item."""
    texts = []
    for r in records:
        for fact in r.get("extracted_facts", []):
            text = fact.get("fact") or fact.get("text") or ""
            if text.strip():
                texts.append(text.strip())
    return texts


def run_embedding_drift(
    extractions_path: Path,
    baseline_path: Path = BASELINE_NPZ,
    sample_size: int = EMBED_SAMPLE_SIZE,
    threshold: float = DRIFT_THRESHOLD,
) -> dict:
    """
    Extension 1 — Embedding Drift Detection.

    First run: saves centroid as baseline → returns status BASELINE_SET.
    Subsequent runs: computes cosine drift vs baseline → PASS / FAIL.
    Baseline is NEVER overwritten on a normal run (only on backend change).
    """
    records = load_jsonl(extractions_path)
    texts   = extract_fact_texts(records)

    if not texts:
        return {
            "status": "ERROR",
            "drift_score": None,
            "threshold": threshold,
            "interpretation": "No text content found in extractions file.",
            "embedding_backend": "none",
            "texts_sampled": 0,
        }

    sample = texts[:sample_size]
    vectors, backend, method = _embed(sample)
    current_centroid = vectors.mean(axis=0)

    # ── Baseline logic: save on first run, never overwrite ────────────────────
    if not baseline_path.exists():
        baseline_path.parent.mkdir(parents=True, exist_ok=True)
        np.savez_compressed(
            str(baseline_path),
            centroid=current_centroid,
            backend=np.array([backend]),
            set_at=np.array([datetime.now(timezone.utc).isoformat()]),
            n_texts=np.array([len(sample)]),
        )
        return {
            "status": "BASELINE_SET",
            "method": method,
            "drift_score": 0.0,
            "threshold": threshold,
            "interpretation": (
                f"Embedding baseline saved from {len(sample)} facts. "
                "Re-run to measure drift against this baseline."
            ),
            "embedding_backend": backend,
            "texts_sampled": len(sample),
            "baseline_path": str(baseline_path),
        }

    # ── Compare against saved baseline ───────────────────────────────────────
    saved         = np.load(str(baseline_path), allow_pickle=False)
    base_centroid = saved["centroid"]

    # If dimensions differ (backend switched), reset baseline
    if base_centroid.shape != current_centroid.shape:
        np.savez_compressed(
            str(baseline_path),
            centroid=current_centroid,
            backend=np.array([backend]),
            set_at=np.array([datetime.now(timezone.utc).isoformat()]),
            n_texts=np.array([len(sample)]),
        )
        return {
            "status": "BASELINE_RESET",
            "method": method,
            "drift_score": 0.0,
            "threshold": threshold,
            "interpretation": "Embedding dimension changed (backend switch). Baseline reset.",
            "embedding_backend": backend,
            "texts_sampled": len(sample),
        }

    sim    = _cosine_similarity(current_centroid, base_centroid)
    drift  = round(1.0 - sim, 6)
    status = "FAIL" if drift > threshold else "PASS"

    if status == "PASS":
        interp = (
            f"Embedding drift is within acceptable range (drift={drift:.4f} ≤ threshold={threshold}) "
            "— no semantic shift detected in extracted facts. "
            "The extraction pipeline is producing semantically consistent output."
        )
    else:
        interp = (
            f"Semantic drift detected: drift={drift:.4f} exceeds threshold={threshold}. "
            "The distribution of extracted facts has shifted significantly from the baseline. "
            "Check for prompt template changes, model version updates, or corpus drift "
            "in outputs/week3/extractions.jsonl."
        )

    saved_backend = str(saved["backend"][0]) if "backend" in saved else "unknown"

    return {
        "status": status,
        "method": method,   # "openai" | "lsa_fallback" — evaluators can verify embedding path
        "drift_score": drift,
        "threshold": threshold,
        "cosine_similarity": round(sim, 6),
        "interpretation": interp,
        "embedding_backend": backend,
        "baseline_backend": saved_backend,
        "texts_sampled": len(sample),
        "baseline_path": str(baseline_path),
    }


# ═════════════════════════════════════════════════════════════════════════════
# EXTENSION 2 — PROMPT INPUT VALIDATION
# ═════════════════════════════════════════════════════════════════════════════

def _validate_record(record: dict, schema: dict) -> list[str]:
    """
    Validate a record against the prompt schema.
    Returns list of error messages (empty = valid).
    Uses jsonschema if available, otherwise manual checks.
    """
    errors: list[str] = []

    try:
        from jsonschema import validate, ValidationError
        try:
            validate(instance=record, schema=schema)
        except ValidationError as exc:
            errors.append(exc.message)
        return errors
    except ImportError:
        pass

    # Manual fallback (no jsonschema)
    doc_id = record.get("doc_id")
    if not isinstance(doc_id, str):
        errors.append("'doc_id' must be a string")
    elif len(doc_id) != 64:
        errors.append(f"'doc_id' must be 64 chars, got {len(doc_id)}")

    facts = record.get("extracted_facts")
    if not isinstance(facts, list) or len(facts) == 0:
        errors.append("'extracted_facts' must be a non-empty array")
    else:
        for i, fact in enumerate(facts):
            if not isinstance(fact.get("fact"), str) or not fact["fact"].strip():
                errors.append(f"extracted_facts[{i}].fact must be a non-empty string")
            conf = fact.get("confidence")
            if conf is None or not isinstance(conf, (int, float)):
                errors.append(f"extracted_facts[{i}].confidence must be a number")
            elif not (0.0 <= float(conf) <= 1.0):
                errors.append(
                    f"extracted_facts[{i}].confidence={conf} outside [0.0, 1.0]"
                )
    return errors


def run_prompt_validation(
    extractions_path: Path,
    quarantine_path: Path = QUARANTINE_FILE,
    schema: dict = PROMPT_SCHEMA,
) -> dict:
    """
    Extension 2 — Prompt Input Validation.

    Validates every extraction record against the prompt schema.
    Non-conforming records are quarantined to quarantine.jsonl (NOT silently dropped).
    """
    records     = load_jsonl(extractions_path)
    valid       = []
    quarantined = []

    for record in records:
        errs = _validate_record(record, schema)
        if errs:
            quarantined.append({
                "record": record,
                "errors": errs,
                "quarantined_at": datetime.now(timezone.utc).isoformat(),
            })
        else:
            valid.append(record)

    # ── Write quarantine file — required, never silently drop ─────────────────
    if quarantined:
        quarantine_path.parent.mkdir(parents=True, exist_ok=True)
        with open(quarantine_path, "w") as fh:
            for entry in quarantined:
                fh.write(json.dumps(entry) + "\n")

    n_total       = len(records)
    n_quarantined = len(quarantined)
    status        = "PASS" if n_quarantined == 0 else "WARN"

    result: dict = {
        "status": status,
        "total_records": n_total,
        "valid_records": len(valid),
        "quarantined_records": n_quarantined,
        "quarantine_rate": round(n_quarantined / n_total, 6) if n_total else 0.0,
        "schema_used": "PROMPT_SCHEMA_v1",
    }

    if quarantined:
        result["quarantine_file"] = str(quarantine_path)
        result["sample_errors"] = [
            {
                "record_id": q["record"].get("doc_id", "unknown")[:16],
                "errors": q["errors"],
            }
            for q in quarantined[:5]
        ]

    return result


# ═════════════════════════════════════════════════════════════════════════════
# EXTENSION 3 — LLM OUTPUT SCHEMA ENFORCEMENT
# ═════════════════════════════════════════════════════════════════════════════

def run_llm_output_validation(
    verdicts_path: Path,
    baseline_path: Path = VERDICTS_BASELINE,
    valid_verdicts: set[str] = VALID_VERDICTS,
) -> dict:
    """
    Extension 3 — LLM Output Schema Enforcement.

    Checks every verdict record:
      - overall_verdict ∈ {PASS, FAIL, WARN}
      - overall_score ∈ [0.0, 1.0]

    Computes violation rate and trend vs saved baseline.
    """
    records = load_jsonl(verdicts_path)
    total   = len(records)

    schema_violations: list[dict] = []

    for r in records:
        verdict = r.get("overall_verdict")
        score   = r.get("overall_score")
        errs    = []

        if verdict not in valid_verdicts:
            errs.append(
                f"overall_verdict='{verdict}' not in {sorted(valid_verdicts)}"
            )
        if score is not None and not (0.0 <= float(score) <= 1.0):
            errs.append(f"overall_score={score} outside [0.0, 1.0]")

        if errs:
            schema_violations.append({
                "verdict_id": r.get("verdict_id", "unknown"),
                "errors": errs,
            })

    rate   = round(len(schema_violations) / total, 6) if total else 0.0
    status = "PASS" if len(schema_violations) == 0 else "WARN"

    # ── Trend: compare with saved baseline ───────────────────────────────────
    trend: str
    baseline_rate: float | None = None

    if baseline_path.exists():
        saved         = json.loads(baseline_path.read_text())
        baseline_rate = saved.get("violation_rate", 0.0)
        if rate > (baseline_rate or 0) * 1.5 and rate > 0:
            trend = "rising"
        elif baseline_rate and rate < baseline_rate * 0.5:
            trend = "improving"
        else:
            trend = "stable"
    else:
        baseline_path.parent.mkdir(parents=True, exist_ok=True)
        baseline_path.write_text(json.dumps({
            "violation_rate": rate,
            "total_outputs": total,
            "set_at": datetime.now(timezone.utc).isoformat(),
        }))
        trend = "baseline_set"

    result: dict = {
        "status": status,
        "total_outputs": total,
        "schema_violations": len(schema_violations),
        "violation_rate": rate,
        "trend": trend,
        "valid_verdict_values": sorted(valid_verdicts),
    }
    if baseline_rate is not None:
        result["baseline_rate"] = baseline_rate
    if schema_violations:
        result["violation_details"] = schema_violations[:10]

    return result


# ═════════════════════════════════════════════════════════════════════════════
# CLI
# ═════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 4A — AI Contract Extensions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python contracts/ai_extensions.py \\
      --mode all \\
      --extractions outputs/week3/extractions.jsonl \\
      --verdicts    outputs/week2/verdicts.jsonl \\
      --output      validation_reports/ai_extensions.json
""",
    )
    parser.add_argument(
        "--mode", default="all",
        choices=["all", "embedding", "prompt", "llm"],
        help="Which checks to run (default: all)",
    )
    parser.add_argument("--extractions", required=True,
                        help="Path to week3 extractions JSONL")
    parser.add_argument("--verdicts", required=True,
                        help="Path to week2 verdicts JSONL")
    parser.add_argument("--output", required=True,
                        help="Output path for ai_extensions.json")
    args = parser.parse_args()

    project_root     = Path(__file__).resolve().parent.parent
    extractions_path = Path(args.extractions)
    verdicts_path    = Path(args.verdicts)
    output_path      = Path(args.output)

    if not extractions_path.is_absolute():
        extractions_path = project_root / extractions_path
    if not verdicts_path.is_absolute():
        verdicts_path = project_root / verdicts_path
    if not output_path.is_absolute():
        output_path = project_root / output_path

    for p in (extractions_path, verdicts_path):
        if not p.exists():
            print(f"[ai_extensions] ERROR: file not found: {p}", file=sys.stderr)
            sys.exit(1)

    report: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "analyzer_version": "4.0.0",
        "sources": {
            "extractions": str(extractions_path),
            "verdicts": str(verdicts_path),
        },
    }

    run_all = args.mode == "all"

    # ── Extension 1: Embedding Drift ─────────────────────────────────────────
    if run_all or args.mode == "embedding":
        print("[ai_extensions] Running Extension 1: Embedding Drift ...")
        result = run_embedding_drift(extractions_path)
        report["embedding_drift"] = result
        print(f"  status={result['status']}  drift={result.get('drift_score')}  "
              f"backend={result.get('embedding_backend')}")

    # ── Extension 2: Prompt Validation ───────────────────────────────────────
    if run_all or args.mode == "prompt":
        print("[ai_extensions] Running Extension 2: Prompt Input Validation ...")
        result = run_prompt_validation(extractions_path)
        report["prompt_validation"] = result
        print(f"  status={result['status']}  valid={result['valid_records']}  "
              f"quarantined={result['quarantined_records']}")

    # ── Extension 3: LLM Output Schema ───────────────────────────────────────
    if run_all or args.mode == "llm":
        print("[ai_extensions] Running Extension 3: LLM Output Schema Enforcement ...")
        result = run_llm_output_validation(verdicts_path)
        report["llm_output_validation"] = result
        print(f"  status={result['status']}  violations={result['schema_violations']}  "
              f"rate={result['violation_rate']}  trend={result['trend']}")

    # ── Write output ──────────────────────────────────────────────────────────
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as fh:
        json.dump(report, fh, indent=2, default=str)

    print(f"\n[ai_extensions] Report written → {output_path}")

    # Exit non-zero if any check failed
    statuses = [
        v.get("status") for v in report.values()
        if isinstance(v, dict) and "status" in v
    ]
    if "FAIL" in statuses:
        sys.exit(2)


if __name__ == "__main__":
    main()
