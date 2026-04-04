# Contract Enforcer Platform — Final Report
**Trainee:** richh-s  
**Date:** 2026-04-04  
**Branch:** interim  
**Repo:** `-contract-enforcer-platform`

---

## 1. Data Flow Diagram

The platform spans five interacting systems. Every arrow names the file path, the exact fields carried across the interface, and the data artifact being transferred.

```
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                  CONTRACT ENFORCER PLATFORM — DATA FLOW                     │
 └─────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────┐
│  Week 1                  │   outputs/week1/intent_records.jsonl
│  Intent Correlator       │   Record type: IntentRecord
│                          │   Fields: id(uuid), timestamp, tool,
│  11 records              │           intentId, mutationClass, mutationType,
│  3 intents               │           filePath, contentHash(sha256),
│  2 tools observed        │           outcome, revisionId, fileSizeBytes
└──────────┬───────────────┘
           │
           │  ① IntentRecord stream → audit trail
           │  carried: intentId, filePath, outcome, revisionId, contentHash
           │  schema change risk: contentHash format (sha256 hex64)
           ▼
┌──────────────────────────┐
│  Week 2                  │   outputs/week2/verdicts.jsonl
│  Digital Courtroom       │   Record type: VerdictRecord
│                          │   Fields: session_id, decision, rationale,
│  (verdicts produced)     │           applicant_id, verdict_id
└──────────────────────────┘
           (context flows into lending pipeline below)

┌──────────────────────────┐
│  Week 3                  │   outputs/week3/extractions.jsonl
│  Document Refinery       │   Record type: ExtractionRecord
│                          │   Fields: doc_id(sha256-hex64),
│  50 records              │           extracted_facts[]{fact, confidence}
│  34,130 facts extracted  │
│  ⚠ confidence all=0.9   │
└──────────┬───────────────┘
           │                           │
           │  ② Facts → lineage nodes  │  ③ Facts → event payloads
           │  carried: doc_id,         │  carried: doc_id,
           │  extracted_facts,         │  extracted_facts[*].fact
           │  confidence (weight)      │
           ▼                           ▼
┌──────────────────────────┐  ┌────────────────────────────┐
│  Week 4                  │  │  Week 5                    │
│  Brownfield Cartographer │  │  Event System              │
│                          │  │                            │
│  outputs/week4/          │  │  outputs/week5/events.jsonl│
│  lineage_snapshots.jsonl │  │  Record type: EventRecord  │
│                          │  │                            │
│  Record type: Snapshot   │  │  62 events                 │
│  Fields: snapshot_id,    │  │  schema_version: 1.0 (52)  │
│  timestamp,              │  │  schema_version: 2.0 (10)  │
│  nodes[]{id,type},       │  │  ⚠ mixed schema versions  │
│  edges[]{src,tgt,rel}    │  │  ⚠ 4 clock-skew events    │
│  ⚠ sparse node schema   │  │                            │
└──────────┬───────────────┘  └──────────┬─────────────────┘
           │                             │
           │  ④ Lineage graph            │  ⑤ Events →
           │  → attribution engine       │  contract validation
           │  carried: nodes[].id,       │  carried: event_type,
           │  edges[].relation           │  schema_version, payload
           ▼                             ▼
  Attribution Engine              Contract Validation Layer
  (reverse-BFS blame chain)       (Bitol ODCS v3 rules)
           │                             │
           └──────────────┬──────────────┘
                          │
                          │  ⑥ AI spans → trace validation
                          ▼
          ┌────────────────────────────────┐
          │  LangSmith Traces              │
          │  outputs/traces/runs.jsonl     │
          │  Record type: RunRecord        │
          │  Fields: id(uuid), run_type,   │
          │  start_time, end_time,         │
          │  prompt_tokens,                │
          │  completion_tokens,            │
          │  total_tokens, total_cost      │
          │  402 runs — ✅ fully clean     │
          └────────────────────────────────┘
```

### Arrow Annotation Table

| Arrow | From → To | File | Key Fields Carried | Failure Present |
|-------|-----------|------|--------------------|-----------------|
| ① | Week1 → Week2 | `outputs/week1/intent_records.jsonl` | `intentId, filePath, outcome, revisionId, contentHash` | YES — 8 of 11 `id` values fail UUID pattern |
| ② | Week3 → Week4 | `outputs/week3/extractions.jsonl` | `doc_id, extracted_facts[*].confidence` | YES — `confidence` stdev=0.0 (flat weight, breaks attribution) |
| ③ | Week3 → Week5 | `outputs/week3/extractions.jsonl` | `doc_id, extracted_facts[*].fact` | YES — `source_hash`, `extraction_model`, `fact_id` absent |
| ④ | Week4 → Attribution | `outputs/week4/lineage_snapshots.jsonl` | `nodes[].id, nodes[].type, edges[].relation` | YES — sparse node schema; computed fields absent |
| ⑤ | Week5 → Contracts | `outputs/week5/events.jsonl` | `event_type, schema_version, payload` | YES — mixed schema versions; 4 clock-skew violations |
| ⑥ | Traces → AI Validation | `outputs/traces/runs.jsonl` | `run_type, start_time, end_time, prompt_tokens, completion_tokens` | **None** — 15/15 checks pass |

---

## 2. Contract Coverage Table

| Interface | Contract File | Status | Clause Count | Gap |
|-----------|---------------|--------|--------------|-----|
| Week1 IntentRecord | `week1_intent_records.yaml` | **Yes** | 24 | Full coverage |
| Week2 VerdictRecord | _(none)_ | **No** | — | Not profiled; fields need clause design |
| Week3 ExtractionRecord | `week3_document_refinery_extractions.yaml` | **Partial** | 11 | `source_hash`, `extraction_model`, `fact_id` declared but absent from data |
| Week3 → Week4 confidence dependency | `week3_document_refinery_extractions.yaml` | **Partial** | 1 statistical | No cross-contract clause linking confidence scale to Week4 weight |
| Week4 LineageSnapshot | `week4_lineage.yaml` | **Partial** | 4 | Computed fields (`node_count`, `edge_count`) not stored in raw JSONL |
| Week5 EventRecord | `week5_events.yaml` | **Partial** | 25 | `payload` shape varies by `event_type`; no per-event-type schema |
| Week5 → Contracts pipeline | `week5_events.yaml` | **Yes** | 25 | Full enforcement |
| LangSmith Traces | `langsmith_traces.yaml` | **Yes** | 15 | Full coverage; all 15 pass |

**Critical gap:** The Week3 → Week4 confidence-weight dependency is the highest-risk uncovered interface. A confidence scale change (0–1 → 0–100) passes both individual contracts while silently corrupting all attribution downstream.

---

## 3. Validation Run Results

All contracts were run against live data on 2026-04-04. Results are interpreted as risk signals with downstream consequence.

### Run Summary

| Dataset | Total Checks | Passed | Failed | Errored | Pass Rate |
|---------|-------------|--------|--------|---------|-----------|
| Week3 Extractions (violated) | 11 | 5 | 3 | 3 | 45% |
| Week5 Events | 25 | 24 | 1 | 0 | 96% |
| Week1 Intent Records | 24 | 23 | 1 | 0 | 96% |
| Week4 Lineage | 4 | 2 | 0 | 2 | 50% |
| LangSmith Traces | 15 | 15 | 0 | 0 | 100% |
| **Total** | **79** | **69** | **5** | **5** | **87%** |

---

### Violation A — Confidence Range Breach (STRUCTURAL, CRITICAL)

**Check:** `week3.extracted_facts.confidence.range`  
**Field:** `extracted_facts[*].confidence`  
**Actual value:** min=90.0, max=90.0 across 50 records  
**Contract expectation:** `minimum: 0.0, maximum: 1.0`  
**Records failing:** 50 of 50 — **CRITICAL FAIL**

Every confidence value is 90.0 — a producer silently switched from a 0–1 float scale to 0–100. There is no runtime error; the field is still a valid number. Only the range-bound structural clause catches this.

**Downstream consequence — week4-cartographer (direct subscriber):** The Cartographer uses `confidence` as an edge weight in the lineage attribution graph. A value of 90.0 instead of 0.9 inflates every weight by 100×. The ranking of upstream source nodes is completely distorted — garbled OCR text has identical weight to a clean entity extraction. Silent corruption, no crash.

---

### Violation B — Zero-Variance Confidence (STATISTICAL, MEDIUM)

**Check:** `week3.extracted_facts.confidence.non_zero_variance`  
**Field:** `extracted_facts[*].confidence`  
**Actual value:** std=0.0, mean=90.0 — all 34,130 values identical  
**Contract expectation:** std > 0  
**Status:** MEDIUM FAIL

A structural check (`type: number`, `minimum`, `maximum`) marks this PASS — the value is a valid number in range. Only a statistical clause catches that the field carries zero information. A confidence score that never varies is a constant, not a measurement.

**Downstream consequence:** Week4 attribution weights are completely flat. The blame chain cannot rank source files by reliability — every node has identical weight regardless of extraction quality. This is a validation blind spot that only exists in systems without statistical rules.

---

### Violation C — Z-Score Drift (STATISTICAL, HIGH)

**Check:** `week3.extracted_facts.confidence.z_score`  
**Field:** `extracted_facts[*].confidence`  
**Actual value:** z=99.00, current_mean=90.0  
**Contract expectation:** baseline_mean=0.9, z within threshold  
**Status:** HIGH FAIL

A z-score of 99 means the current mean (90.0) is 99 standard deviations from the baseline mean (0.9). The statistical fingerprint of the data has completely changed. Any downstream consumer operating on confidence values is receiving fundamentally different inputs than it was designed for.

---

### Violations D–F — Missing Required Columns (STRUCTURAL, CRITICAL)

**Checks:** `week3.source_hash.pattern`, `week3.extraction_model.pattern`, `week3.extracted_facts.items.fact_id.pattern`  
**Actual value:** field absent from all 50 records  
**Contract expectation:** field present, matching declared pattern  
**Status:** CRITICAL ERROR (column_missing)

The contract declares three fields as required that do not exist in the actual data. `extraction_model` is the provenance chain — without it, no consumer knows which LLM produced a given extraction. `fact_id` enables idempotent downstream upserts — without it, Week4 cannot deduplicate re-extractions. `source_hash` enables document-level deduplication.

---

### Violation G — Clock Skew (CROSS-FIELD, HIGH)

**Check:** `week5.timestamp.cross_field_timestamp`  
**Actual value:** 4 events where `timestamp < payload.*_at`  
**Contract expectation:** `timestamp >= payload.*_at`  
**Status:** HIGH FAIL

Four events have a recording timestamp earlier than the business-action timestamp inside their payload — a temporal contradiction. Event sourcing relies on monotonic timestamps for replay. In a lending pipeline, a compliance check could appear to precede the application submission that triggered it.

---

### Violation H — Non-UUID IDs (STRUCTURAL, CRITICAL)

**Check:** `week1.id.pattern`  
**Actual value:** 8 of 11 records have non-UUID `id` values  
**Contract expectation:** `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`  
**Status:** CRITICAL FAIL

8 of 11 intent records have `id` values that are not valid UUIDs. Downstream consumers joining on `id` across the audit trail fail to resolve these records. UUID format is a join key, not cosmetic.

---

## 4. Violation Deep-Dive: Blame Chain and Blast Radius

The most critical violation — `week3.extracted_facts.confidence.range` — was subjected to full blame-chain analysis using `contracts/attributor.py`.

### Failing Check

| Field | Value |
|-------|-------|
| Check ID | `week3.extracted_facts.confidence.range` |
| Field | `extracted_facts[*].confidence` |
| Contract | `week3-document-refinery-extractions` |
| Violation | 50 records with confidence=90.0 outside [0.0, 1.0] |

### Lineage Traversal — Step by Step

```
Step 1: failing system  = week3-document-refinery-extractions
        failing field   = extracted_facts[*].confidence

Step 2: load lineage graph from outputs/week4/lineage_snapshots.jsonl
        nodes: 16 system nodes  |  edges: 11 dependency relations

Step 3: BFS upstream from week3-document-refinery-extractions
        hop 1 → outputs/week3/extractions.jsonl
        hop 2 → schema_snapshots/week3-extractions/
        hop 3 → git log on each snapshot candidate file

Step 4: for each candidate, run git log to find commits
        touching the confidence field or week3 extraction schema

Step 5: score each candidate commit using confidence formula

Step 6: return ranked blame chain
```

### Blame Chain — Ranked Candidates

| Rank | File | Commit | Author | Confidence Score |
|------|------|--------|--------|-----------------|
| 1 | `schema_snapshots/week3-extractions/20260401T123850Z.yaml` | `0d83565` | rahelsamson953@gmail.com | **40%** |

**Scoring formula:**
```
base_score = 1.0
score -= days_since_commit × 0.01     # decrement by recency
score -= lineage_hops × 0.10          # further reduced per BFS hop from failing node
score  = max(0.1, score)              # floor at 10%
```

Commit `0d83565` is 3 days old (−0.03) and 2 lineage hops from the failing field (−0.20), giving a final score of ~0.40.

**Attribution assessment: SPECULATIVE (40%).** The commit touched the snapshot file when the scale change occurred but the attributor has no direct access to the extraction pipeline source — it cannot confirm this commit introduced the `confidence = 90.0` logic. The score reflects indirect proximity evidence, not a direct code match.

### Blast Radius

| Consumer | Type | Reason |
|----------|------|--------|
| `week4-cartographer` | **Direct subscriber** | Reads `confidence` as edge weight from week3 output |
| `week5-event-publisher` | **Transitive** | Consumes week4 lineage output enriched with corrupted weights |
| `contract-enforcer-runner` | **Transitive** | Validates week4 attribution results derived from corrupted data |

- **Contamination depth:** 11 BFS hops through the dependency graph
- **Estimated records affected:** 50
- **Blast radius method:** registry_primary + lineage_enrichment (subscriptions.yaml sourced first, then enriched with lineage graph edges)

---

## 5. Schema Evolution Case Study

### Context

Between snapshots `20260401T123850Z.yaml` and `20260402T090000Z.yaml`, the `week3-extractions` contract schema was modified. The schema analyzer diffed the two snapshots against the breaking-change taxonomy.

### Before / After Diff

| Field | Before | After | Taxonomy Classification |
|-------|--------|-------|------------------------|
| `confidence` | `maximum: 0.9` (scale 0.0–1.0) | `maximum: 100.0` (scale 0–100) | **Narrow type — float 0.0–1.0 → int 0–100 — BREAKING, data loss, CRITICAL** |
| `fact` | `type: string, required: true` | _(removed)_ | **Field removal — BREAKING for all consumers reading this field** |
| `extracted_text` | _(absent)_ | `type: string, required: true` | **Non-nullable addition — BREAKING: existing writers crash without this field** |
| `entity_type` | _(absent)_ | `type: string, nullable: true` | PARTIAL — safe addition |
| `extraction_pipeline_version` | _(absent)_ | `type: string, nullable: true` | PARTIAL — safe addition |

**Compatibility verdict: BREAKING**  
**Migration required: YES — STOP deployment**

### Why CONFIDENCE_SCALE_BREAK Is CRITICAL

The confidence field is a computation input used as an edge weight in the Week4 attribution graph — not a display field. Changing its scale from [0.0, 1.0] to [0, 100] is a **narrow type change**: the type is still `number`, so no type error fires anywhere in the pipeline. The contract's `minimum/maximum` clause is the only enforcement layer. All downstream consumers that threshold or normalise on confidence produce wrong results silently — no errors, just wrong numbers.

### Migration Impact — Required Steps Before Deployment

**Downstream consumers affected:** week4-cartographer, week5-event-publisher, contract-enforcer-runner

1. **Normalize confidence at source:** Apply `confidence = raw_score / 100.0` before writing to `outputs/week3/extractions.jsonl`. Deploy this before any downstream consumer re-runs.
2. **Re-run Week3 validation runner:** `python contracts/runner.py --contract generated_contracts/week3_document_refinery_extractions.yaml --data outputs/week3/extractions.jsonl --output validation_reports/week3_final.json --mode ENFORCE` — confirm `week3.extracted_facts.confidence.range` returns PASS.
3. **Migrate `fact` → `extracted_text`:** The `fact` field was removed and replaced with `extracted_text`. Update all consumers reading `extracted_facts[*].fact` to read `extracted_facts[*].extracted_text`.
4. **Backfill `extracted_text` in all records:** The new non-nullable field must be populated in all 50 existing records before deployment — existing writers crash with KeyError without it.

### Rollback Plan

1. **Revert the active snapshot:** Restore `schema_snapshots/week3-extractions/20260401T123850Z.yaml` as the active baseline by re-running: `python contracts/generator.py --source outputs/week3/extractions.jsonl --contract-id week3-extractions --output generated_contracts/`
2. **Re-establish statistical baselines:** The confidence scale change invalidated the statistical baseline (mean=0.9, std=0.0). Re-running the generator rewrites `schema_snapshots/` with a clean baseline. The z-score drift check depends on this baseline — a stale baseline produces false alarms on every subsequent run.
3. **Pin the runner to the reverted contract:** Confirm all runner commands reference the reverted contract file before re-running validation.

### Comparison to Production Tools

A production schema registry (**Confluent Schema Registry**) blocks a CONFIDENCE_SCALE_BREAK change **at registration time** — the producer cannot submit the new schema unless all consumers have opted into the new compatibility mode. Our analyzer catches this **post-hoc** by diffing committed snapshots. The enforcement point is later in the pipeline, which means a breaking change can reach a snapshot before it is caught. The mitigation is to run `contracts/schema_analyzer.py` as a pre-commit CI step, approximating Confluent's registration-gate behavior.

---

## 6. AI Contract Extension Results

All three AI extensions were run against live Week 3 extraction data (50 records) and Week 2 verdict data.

### 1. Embedding Drift

| Metric | Value |
|--------|-------|
| Status | **PASS** |
| Drift score | **0.0** |
| Threshold | **0.15** |
| Method | Cosine distance (LSA fallback: TF-IDF + TruncatedSVD, backend: `sklearn:tfidf+svd`) |

**How it works:** Each extraction record's `fact` strings are embedded into a vector space via TF-IDF + Truncated SVD. The **cosine distance** between the current run's centroid and the stored baseline centroid measures semantic drift. Score 0.0 = current fact distribution is in the same semantic direction as the baseline.

**Pass/fail conclusion:** Score 0.0 < threshold 0.15 → **PASS**. No semantic shift detected.

**Trend across two data points:**  
- Baseline (2026-04-01): drift = 0.0 (by definition)  
- Current run (2026-04-04): drift = 0.0  
- **Trend: stable.** The extraction pipeline is producing semantically consistent output.

### 2. Prompt Input Validation

| Metric | Value |
|--------|-------|
| Status | **PASS** |
| Total records | **50** |
| Valid | **50** |
| Quarantined | **0** |

Each record is validated against the prompt input JSON schema before use as LLM context. The schema requires `doc_id` to be a 64-char hex string and `confidence` in [0.0, 1.0]. All 50 records passed. If run against the violated dataset, the 50 records with `confidence=90.0` would be quarantined before reaching the LLM.

### 3. LLM Output Schema Validation

| Metric | Value |
|--------|-------|
| Status | **PASS** |
| Total outputs checked | **3** (Week 2 verdict records) |
| Schema violations | **0** |
| Violation rate | **0.0%** |
| Trend | **stable** (0.0% this run vs 0.0% baseline) |

All 3 verdict records contain valid values (`verdict` in `{PASS, FAIL, WARN}`). Trend is stable across two data points — no prompt degradation detected.

### AI Risk Assessment

**Overall AI risk level: LOW.** All three checks pass. The system's AI components are currently producing trustworthy outputs. The only caveat: LSA fallback is less sensitive to subtle semantic drift than a neural embedding model — a score of 0.0 means "no detectable coarse-grained drift," not "no drift at all."

---

## 7. Highest-Risk Interface Analysis

### Interface Identified

**Interface:** Week 3 Document Refinery → Week 4 Brownfield Cartographer  
**File:** `outputs/week3/extractions.jsonl` → consumed by `outputs/week4/lineage_snapshots.jsonl`  
**Schema element:** `ExtractionRecord.extracted_facts[*].confidence`  
**Contract:** `generated_contracts/week3_document_refinery_extractions.yaml`

This is the highest-risk interface because `confidence` controls the ranking of every upstream source node in the Week4 attribution graph. A silent scale change corrupts all attribution with no runtime error.

### Failure Mode

**Scenario:** A producer changes confidence output from `float [0.0, 1.0]` to `int [0, 100]` — for example, changing `confidence = model_logit` to `confidence = round(model_logit * 100)`. No type error fires. The pipeline continues running.

**Failure class: STATISTICAL** — This cannot be caught by structural checks alone:

| Check Type | Catches This Failure? | Why |
|------------|----------------------|-----|
| Structural `type: number` | **NO** | 90.0 is a valid number |
| Structural `not_null` | **NO** | Values are present |
| Structural `minimum: 0.0, maximum: 1.0` | **YES** — if values exceed 1.0 | Catches range breach |
| Statistical `non_zero_variance` | **PARTIAL** | Catches hard-coded default, not a scale change with variance |
| Statistical `z_score` | **YES** | Catches distributional shift from established baseline |
| Cross-contract clause (Week3 → Week4) | **MISSING** | No clause formally binds Week3 confidence scale to Week4 weight assumption |

### Enforcement Gap

The critical gap is the absence of a **cross-contract clause** explicitly linking `week3-document-refinery-extractions.confidence` to `week4-cartographer`'s attribution weight expectation. The two contracts are validated independently. A value that passes Week3 range checks could still violate the implicit assumption in Week4 — and there is no automated check that catches the interface-level contract breach.

### Blast Radius If in Production

- **week4-cartographer** (direct): attribution weights inflated by 100×, all source ranking wrong
- **week5-event-publisher** (transitive, hop 1): event payload enrichment uses corrupted attribution metadata
- **contract-enforcer-runner** (transitive, hop 2): validation reports reference corrupted attribution results
- **Contamination depth:** 11 hops through the dependency graph
- **Records affected:** all 50 week3 records × all downstream reprocessing runs

### Recommendation

Add a **cross-interface BREAKING clause** to `week3_document_refinery_extractions.yaml`:

```yaml
quality:
  crossField:
    - rule: confidence_cross_contract_scale_guard
      field: extracted_facts[*].confidence
      type: cross_field
      check: all_values_in_range
      min: 0.0
      max: 1.0
      severity: BREAKING
      description: >
        Confidence must remain in [0.0, 1.0] as consumed by week4-cartographer
        as an edge weight. Any value > 1.0 inflates attribution ranking by the
        scale factor — silent data corruption with no runtime error.
```

Additionally, **upgrade the runner mode** on the Week3→Week4 interface from `AUDIT` to `ENFORCE`. In AUDIT mode, violations are logged but the pipeline continues. In ENFORCE mode, a BREAKING confidence range violation halts the pipeline before corrupted data reaches Week4.

---

## 8. Auto-generated Enforcer Report

The enforcer report is machine-generated by `contracts/report_generator.py` from live data artifacts. No values are hand-written — every number is computed from `violation_log/violations.jsonl` and `validation_reports/*.json` at run time. Changing the violation log changes the report.

**Generated at:** 2026-04-04T09:02:50Z  
**Source artifact:** `enforcer_report/report_data.json`  
**Report version:** 4.0.0

### Data Health Score

```
Formula:
  base_score  = (checks_passed / total_checks) × 100
              = (87 / 110) × 100
              = 79.1

  penalty     = 20 × critical_failures_in_violation_log
              = 20 × 1
              = 20

  final_score = max(0, base_score − penalty)
              = max(0, 79.1 − 20)
              = 59
```

**Data health score: 59 / 100 — CRITICAL**  
The system is not in a deployable state. All CRITICAL violations must be resolved before the next release.

### Violations by Severity

| Severity | Count |
|----------|-------|
| CRITICAL | 14 |
| HIGH | 3 |
| MEDIUM | 5 |
| LOW | 0 |

### Schema Changes Detected

4 schema evolution events detected across `week3-extractions`, `week3_document_refinery_extractions`, and `week5-events`. All 4 have `compatibility_verdict: BREAKING` and `migration_required: true`. The CONFIDENCE_SCALE_BREAK event is the most critical — see Section 5 for the full case study.

### AI System Risk Assessment

**AI risk level: LOW.** Embedding drift PASS (score=0.0 vs threshold=0.15, cosine distance method). Prompt validation PASS (50 records checked, 0 quarantined). LLM output schema PASS (0% violation rate, stable trend). See Section 6 for full detail.

### Three Prioritised Recommended Actions

**Priority 1 — CRITICAL**  
**Title:** Fix confidence scale — revert to 0.0–1.0 float range  
**File:** `scripts/inject_violation.py` (source of injected violation) / extraction pipeline output writer  
**Field:** `extracted_facts[*].confidence` in contract `week3-document-refinery-extractions`  
**Contract clause:** `quality.structural: confidence_range` (rule type: `structural`, check: range [0.0, 1.0])  
**Fix:** `confidence = raw_score / 100.0  # normalize before writing to JSONL`

**Priority 2 — CRITICAL**  
**Title:** Revert confidence maximum in week3-extractions contract snapshot  
**File:** `generated_contracts/week3_extractions.yaml`, field `confidence.maximum`  
**Field:** `confidence` — snapshot `20260402T090000Z.yaml` has `maximum: 100.0`  
**Contract clause:** `quality.crossField: confidence_cross_contract_scale_guard` (severity: BREAKING)  
**Fix:** Restore `maximum: 1.0`; re-establish statistical baseline via `python contracts/generator.py --source outputs/week3/extractions.jsonl --contract-id week3-extractions`

**Priority 3 — HIGH**  
**Title:** Re-run full validation after fixes and verify health score recovers above 80  
**File:** `validation_reports/week3_violated.json`  
**Field:** All `confidence` checks in `week3-document-refinery-extractions`  
**Contract clause:** All `quality.structural` and `quality.statistical` confidence rules  
**Fix:** `python contracts/runner.py --contract generated_contracts/week3_document_refinery_extractions.yaml --data outputs/week3/extractions.jsonl --output validation_reports/week3_final.json --mode ENFORCE` — confirm all CRITICAL checks return PASS before next deployment.

---

## 9. Reflection — What Contract Thinking Revealed

### Discovery 1: The doc_id Format Assumption Was Wrong

**Before contracts:** I assumed `doc_id` was a UUID because the field name follows the pattern of other primary keys in the system. I wrote `format: uuid` by habit.

**What contract thinking exposed:** Writing the structural rule forced me to inspect the actual data. The value was `07bc09572756e5361f00a41a67b75bc3beb5e482c61b417160dd7421e9519ce8` — 64 characters, no hyphens. A UUID is 36 characters. The rule `format: uuid` contradicted the pattern rule `pattern: ^[0-9a-f]{64}$`. The contradiction was only visible when both rules were in the same YAML block.

**Shift in mental model:** `format: uuid` is a claim that requires evidence, not a default.

---

### Discovery 2: Statistical Silence Is Invisible Without Statistical Contracts

**Before contracts:** If a field was present and within range, it was healthy. Confidence was present, non-null, and in [0.0, 1.0] — structurally clean.

**What contract thinking exposed:** The `non_zero_variance` rule produced: `std=0.0000, mean=0.9000 — all 34,130 values are identical`. A structural validator marks this PASS. The failure is not in the type or range — it is in the absence of information.

**Shift in mental model:** Structural contracts tell you the shape is correct. Statistical contracts tell you whether the data has meaning. A system with only structural contracts has a complete blind spot over the statistical dimension.

---

### Discovery 3: Contracts Reveal What the System Claims vs What It Emits

**Before contracts:** I assumed Week3 emitted `source_hash`, `extraction_model`, and `fact_id` — obvious provenance fields for any extraction pipeline.

**What contract thinking exposed:** Declaring these as required and running validation produced three consecutive `column_missing` errors. The pipeline emits only `doc_id` and `extracted_facts[]{fact, confidence}`.

**Shift in mental model:** Contracts make implicit requirements explicit and immediately test them against reality. The gap was surfaced at the source boundary — the cheapest point to fix it.

---

### Discovery 4: Clock Skew Is a Silent Event-Sourcing Failure

**Before contracts:** I assumed all Week5 timestamps were consistent.

**What contract thinking exposed:** The cross-field rule `timestamp >= payload.*_at` found 4 events where the recording timestamp is earlier than the payload's business-action timestamp — a temporal contradiction that makes event replay unsafe.

**Shift in mental model:** Timestamps are a consistency constraint, not metadata. Cross-field temporal ordering rules are required in any event-sourcing system.

---

### Discovery 5: The Contract Must Target Raw Data Shape, Not Profiling-Time Views

**Before contracts:** I expected `node_count > 0` to be a straightforward check on Week4 data.

**What contract thinking exposed:** The validator reported `node_count: null` because `node_count` is computed during profiling (`len(nodes)`) but does not exist as a stored key in the raw JSONL.

**Shift in mental model:** The contract must target what the consumer literally receives — the raw record shape, not a computed view from the profiling step.

---

### Summary of Wrong Assumptions

| Assumption | What Was Actually True | How Contract Thinking Exposed It |
|------------|----------------------|----------------------------------|
| `doc_id` is a UUID | 64-char SHA-256 hex digest | `format: uuid` contradicted the 64-char pattern |
| Confidence has real variance | stdev=0.0 across 34,130 values | `non_zero_variance` statistical rule |
| Week3 emits `source_hash`, `extraction_model`, `fact_id` | None are present | `column_missing` errors on first validation run |
| Event timestamps are internally consistent | 4 events have `timestamp < payload.*_at` | Cross-field temporal ordering rule |
| `node_count` is a storable field | Computed from `len(nodes)` — not stored | FAIL on `not_null` check against raw JSONL |
| All Week1 IDs are UUIDs | 8 of 11 records have non-UUID IDs | UUID pattern check flagged them immediately |
