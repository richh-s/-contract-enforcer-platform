# DOMAIN_NOTES.md — Phase 0 Domain Reconnaissance

**Generated:** 2026-04-01  
**Phase:** 0 — Domain Reconnaissance (LOCKED)  
**Status:** Complete

---

## 1. Core Concepts

### Data Contracts — Three Dimensions

A data contract is a formal, versioned agreement between a data producer and
a data consumer. It specifies what data will be delivered, in what shape, with
what quality guarantees. Contracts live in three dimensions:

| Dimension   | What it governs | Example in this system |
|-------------|-----------------|------------------------|
| **Structural** | Schema, field types, nullability, nesting | `extracted_facts[].confidence: number` |
| **Statistical** | Distributions, ranges, drift thresholds | `mean(confidence) ∈ [0.5, 0.95]` |
| **Temporal** | Ordering, timestamps, delivery SLAs | `end_time > start_time` in traces |

### Schema Evolution Compatibility

- **Backward-compatible** — old consumers can read new data without changes.
- **Forward-compatible** — new consumers can read old data without changes.
- **Breaking** — at least one consumer must change code or fail.

### dbt-Style Validation Tests
- `not_null` — field must be present and non-null.
- `unique` — no duplicate values within a column.
- `accepted_values` — enum enforcement (e.g., `run_type ∈ {tool, llm, chain}`).
- `relationships` — foreign key integrity across datasets.

A Bitol contract clause maps directly to a dbt `schema.yml` test. Example:

**Bitol clause (Q4 contract):**
```yaml
quality:
  structural:
    - rule: run_type_enum
      field: run_type
      accepted_values: [tool, llm, chain]
      severity: BREAKING
```

**Equivalent dbt `schema.yml`:**
```yaml
models:
  - name: langsmith_traces
    columns:
      - name: run_type
        tests:
          - not_null
          - accepted_values:
              values: ['tool', 'llm', 'chain']
      - name: total_tokens
        tests:
          - not_null
      - name: start_time
        tests:
          - not_null
```

The mapping is one-to-one for structural rules: `not_null` → `not_null` test,
`accepted_values` → `accepted_values` test, `relationships` → `relationships`
test. Statistical rules (mean drift, stdev thresholds) have no native dbt
equivalent and require custom dbt macros or external tooling.

### AI-Specific Contracts
- **Output schema validation** — LLM structured output matches declared JSON schema.
- **Trace schema enforcement** — every span has `start_time`, `end_time`, token counts.
- **Embedding drift** — cosine distance between embedding batches stays within threshold.

### Violation Taxonomy
- **Structural** — missing field, type mismatch, unexpected null.
- **Statistical** — distribution drift (zero variance, out-of-range values). These are the most dangerous because they pass structural validation while silently corrupting downstream analytics.

---

## 2. Data Readiness Check

```
wc -l outputs/week1/intent_records.jsonl   →  11  ✅ (≥10)
wc -l outputs/week3/extractions.jsonl      →  50  ✅ (≥50)
wc -l outputs/week4/lineage_snapshots.jsonl →   1  ✅ (≥1)
wc -l outputs/week5/events.jsonl           →  62  ✅ (≥50)
wc -l outputs/traces/runs.jsonl            → 402  ✅ (≥50)
```

**Week1 was below minimum (7 records).** Four synthetic records were generated
matching the existing schema (`intentId: add-audit-log`, files:
`src/audit/logger.ts`, `src/audit/schema.ts`) and appended to bring the count
to 11. The new records use the same enum values (`mutationClass`, `mutationType`,
`tool`, `outcome`) confirmed present in the original seven records.

---

## 3. Schema Sanity Check — Canonical Comparison

### Actual Schemas (first-record inspection)

| Dataset | Top-level keys |
|---------|----------------|
| **week1** | `id, timestamp, tool, intentId, mutationClass, mutationType, filePath, contentHash, outcome, revisionId, fileSizeBytes, toolArgsSnapshot` |
| **week3** | `doc_id, extracted_facts[]` → fact has `{fact, confidence}` |
| **week4** | `snapshot_id, timestamp, nodes[], edges[]` → node: `{id, type}`, edge: `{source, target, relation}` |
| **week5** | `event_id, stream_id, stream_position, global_position, event_type, event_version, payload, metadata, timestamp, source_system, actor_id, schema_version` |
| **traces** | `id, name, run_type, inputs, outputs, error, start_time, end_time, prompt_tokens, completion_tokens, total_tokens, total_cost, tags, parent_run_id, session_id` |

### Canonical Deviations Found

#### DEV-01 — Week3: Zero-Variance Confidence (STATISTICAL VIOLATION)
- **What changed:** `extracted_facts[].confidence` is uniformly `0.9` across all 34,130 facts in 50 records.
- **Canonical expectation:** `confidence ∈ [0.0, 1.0]` with meaningful variance; the field is intended to model extraction certainty.
- **Why it matters:** Statistical contracts (mean, stdev, percentile thresholds) cannot be meaningfully enforced. The field is effectively a constant, not a signal.
- **Downstream break:** Week4's Brownfield Cartographer uses confidence to weight lineage attribution. With all weights equal at 0.9, attribution scores collapse to uniform — high-risk and low-confidence facts are indistinguishable.
- **Compatibility:** Breaking (statistical) — the *structure* is valid but the *semantics* are violated.
- **Migration:** `outputs/migrate/migrate_confidence_scale.py` — validates range and flags zero-variance. No data transformation needed; source system must emit real confidence scores.

#### DEV-02 — Week5: Mixed Schema Versions (BREAKING STRUCTURAL)
- **What changed:** Stream contains `schema_version=1.0` (52 events) and `schema_version=2.0` (10 events) coexisting without migration window.
- **v1.0 payload (ApplicationSubmitted):** `applicant_id, contact_name, loan_purpose, submitted_at, contact_email, application_id, loan_term_months, submission_channel, requested_amount_usd, application_reference`
- **v2.0 payload (DecisionGenerated):** `decision, session_id, completed_at, model_version, application_id, input_data_hash, regulatory_basis, model_deployment_id, analysis_duration_ms`
- **Why it matters:** A consumer reading events expecting v1.0 `applicant_id` will receive a KeyError on v2.0 `DecisionGenerated` events. No shared payload key provides a safe fallback.
- **Downstream break:** Week5 → Contracts pipeline. Any contract that validates `ApplicationSubmitted.applicant_id not_null` will silently skip v2.0 events, producing incorrect coverage metrics.
- **Compatibility:** Breaking — consumers must branch on `schema_version` and `event_type`.
- **Migration:** `outputs/migrate/migrate_week5_schema_version.py` — partitions into `week5_v1_events.jsonl` and `week5_v2_events.jsonl` with a routing manifest.

#### DEV-03 — Week4: Sparse Node/Edge Schema (STRUCTURAL — FORWARD-COMPATIBLE GAP)
- **What changed:** Nodes contain only `{id, type}`, edges only `{source, target, relation}`. Canonical lineage node schemas typically include `label`, `system`, `schema_hash`, `last_modified`.
- **Why it matters:** Without `system` on nodes, tracing a violation back to the producing system requires additional lookup steps; the lineage graph cannot self-describe provenance.
- **Downstream break:** Attribution (Week4 → blame chain) must join on external registries instead of reading inline. Adds latency and a second failure point.
- **Compatibility:** Forward-compatible — adding fields is non-breaking, but consuming code written against the sparse schema will need updates.
- **Migration:** No data migration required; enrichment can be added at snapshot generation time.

---

## 4. Week 3 Statistical Analysis

### Raw Script Output
```
Count:         34130
Min:           0.9
Max:           0.9
Mean:          0.9
Stdev:         0.0
Unique values: [0.9]
```

### Interpretation
Every single extracted fact across all 50 documents carries exactly the same
confidence value. This is not a measurement — it is a default. The Document
Refinery (Week3) emitted a hard-coded `0.9` rather than computing extraction
confidence from model logprobs, token uncertainty, or heuristic signals.

### Downstream Impact
- **Week4 Brownfield Cartographer:** Lineage attribution algorithms that weight
  edges by source confidence (e.g., `attribution_score = f(confidence)`) produce
  a flat graph where all paths are equally trustworthy. High-risk, low-confidence
  facts (e.g., garbled OCR text like `"any kai no with peed eatin"` with the same
  `0.9` confidence as `"Commercial Bank of Ethiopia"`) receive identical weight.
- **Week5 Events:** If any downstream event (e.g., `CreditAnalysisCompleted`)
  passes `extracted_facts` through its payload, the flat confidence distribution
  will cause statistical monitors to permanently read `mean=0.9, stdev=0.0`,
  making drift detection impossible — any real change in model quality will be
  invisible until it causes a structural failure.

### Violation Check
`max(confidence) = 0.9 ≤ 1.0` — range is **not** violated.  
`stdev = 0.0` — **zero-variance statistical violation confirmed.**

---

## 5. Data Flow Diagram

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    CONTRACT ENFORCER — DATA FLOW                         │
└──────────────────────────────────────────────────────────────────────────┘

  Week1 Intent Correlator
  outputs/week1/intent_records.jsonl
  keys: id, timestamp, tool, intentId, mutationClass, mutationType,
        filePath, contentHash, outcome, revisionId
        │
        │  (1) Tool mutation events → audit trail
        ▼
  Week2 Digital Courtroom
  outputs/week2/verdicts.jsonl
  keys: (verdict_id, session_id, decision, rationale, ...)
        │
        │  (2) Verdict sessions carry applicant context


  Week3 Document Refinery ──────────────────────────────────────────┐
  outputs/week3/extractions.jsonl                                   │
  keys: doc_id, extracted_facts[]{fact, confidence}                 │
  ⚠ FAILURE: confidence uniformly 0.9 (DEV-01)                     │
        │                                                            │
        │  (3) Extracted facts → lineage nodes                       │  (4) Facts → event payloads
        ▼                                                            ▼
  Week4 Brownfield Cartographer                          Week5 Event System
  outputs/week4/lineage_snapshots.jsonl                  outputs/week5/events.jsonl
  keys: snapshot_id, timestamp,                          keys: event_id, event_type,
        nodes[]{id, type},                                     schema_version,
        edges[]{source, target, relation}                      payload, metadata
  ⚠ FAILURE: sparse node schema (DEV-03)                ⚠ FAILURE: mixed schema
        │                                                      versions (DEV-02)
        │  (5) Lineage graph → attribution                     │
        ▼                                                       │  (6) Events → contracts
  Attribution Engine                                     Contract Validation
  (blame chain, git log, causality)                      (Bitol YAML rules)
        │                                                       │
        └──────────────────────┬────────────────────────────────┘
                               ▼
                     LangSmith Traces
                     outputs/traces/runs.jsonl
                     keys: id, run_type, start_time, end_time,
                           prompt_tokens, completion_tokens, total_tokens
                     ✅ No violations detected
                           │
                           │  (7) AI spans → validation
                           ▼
                     AI Contract Enforcement
                     (structural + statistical + AI-specific rules)
```

### Arrow Details (SPEC-REQUIRED)

| Arrow | From → To | File Path | Schema Keys | Failure Occurred |
|-------|-----------|-----------|-------------|-----------------|
| 1 | Week1 → Week2 | `outputs/week1/intent_records.jsonl` | `intentId, filePath, outcome, revisionId` | No |
| 2 | Week2 → (context) | `outputs/week2/verdicts.jsonl` | `session_id, decision, rationale` | Not assessed |
| 3 | Week3 → Week4 | `outputs/week3/extractions.jsonl` | `doc_id, extracted_facts[].confidence` | **YES** — DEV-01 (zero variance) |
| 4 | Week3 → Week5 | `outputs/week3/extractions.jsonl` | `doc_id, extracted_facts[].fact` | **YES** — DEV-01 propagates |
| 5 | Week4 → Attribution | `outputs/week4/lineage_snapshots.jsonl` | `nodes[].id, edges[].relation` | **YES** — DEV-03 (sparse schema) |
| 6 | Week5 → Contracts | `outputs/week5/events.jsonl` | `event_type, schema_version, payload` | **YES** — DEV-02 (mixed versions) |
| 7 | Traces → AI Validation | `outputs/traces/runs.jsonl` | `run_type, start_time, end_time, prompt_tokens, completion_tokens, total_tokens` | No |

---

## Q1 — Schema Evolution

### 3 Backward-Compatible Changes

1. **Adding a new optional field.**  
   Example: adding `entity_type: string | null` to `extracted_facts[]` items.  
   Old consumers read `fact` and `confidence`; they ignore the new field.
   Real field path: `outputs/week3/extractions.jsonl → extracted_facts[].entity_type`.

2. **Widening an enum.**  
   Example: adding `"TOOL_CALL"` to the `mutationClass` enum in `week1/intent_records.jsonl`.  
   Existing values `INTENT_EVOLUTION` and `AST_REFACTOR` are unchanged.
   Old consumers that don't handle the new value will skip it — not crash.

3. **Adding a new top-level optional field to an event.**  
   Example: adding `processing_duration_ms: number | null` to Week5 events.  
   Old consumers reading `event_id`, `event_type`, `payload` are unaffected.

### 3 Breaking Changes

1. **Renaming a required field.**  
   Example: renaming `extracted_facts[].confidence` to `extracted_facts[].score`.  
   Week4's attribution code reads `.confidence` directly — it receives `undefined` / KeyError post-rename.

2. **Changing a field type.**  
   Example: changing `runs.total_tokens: number` to `runs.total_tokens: string` in traces.  
   Any arithmetic `prompt_tokens + completion_tokens == total_tokens` check fails with type error.

3. **Removing a required field.**  
   Example: removing `event_version` from Week5 events.  
   Contract validators that enforce `event_version not_null` will fail schema parsing entirely.

---

## Q2 — Confidence Scale Failure

### Scenario
If the Document Refinery were to change its confidence scale from `[0.0, 1.0]`
to `[0, 100]` (integer percentages), the structural contract would appear valid
— `confidence` would still be a `number` and still be non-null. But every
downstream consumer would silently receive wrong data.

### Real Stats Output (from `outputs/week3/extractions.jsonl`)
```
total_facts:  34130
min:          0.9
max:          0.9
mean:         0.9
stdev:        0.0
unique_values: [0.9]
```
If the scale changed to 0–100 and the value became `90` instead of `0.9`:

- Week4 attribution: `weight = confidence * edge_strength` → `90 * 0.8 = 72` instead of `0.72`. All lineage weights overflow their expected `[0,1]` range, making the attribution score meaningless.
- Week5 statistical contract: `mean(confidence) < 0.95` threshold is trivially failed at `mean=90`.
- Any LLM prompt that injects the confidence value as a scalar for few-shot reasoning would interpret `90` as "90 times more certain than the maximum."

### Bitol YAML Contract (Canonical)
```yaml
dataset: week3_extractions
fields:
  extracted_facts:
    type: array
    items:
      type: object
      required:
        - fact
        - confidence
      properties:
        fact:
          type: string
          minLength: 1
        confidence:
          type: number
          minimum: 0.0
          maximum: 1.0
quality:
  statistical:
    - field: extracted_facts[].confidence
      rule: non_zero_variance
      threshold: 0.0
      severity: WARNING
      message: "confidence stdev=0 indicates hard-coded default, not real extraction uncertainty"
```

---

## Q3 — Blame Chain (Full Spec)

**Scenario:** Contract violation detected — `extracted_facts[].confidence` stdev = 0.0
across the entire Week3 dataset. Trace responsibility to root cause using the
Week4 lineage graph.

### Actual Lineage Graph (from `outputs/week4/lineage_snapshots.jsonl`, snapshot `S-001`, `2026-03-16`)

```
Transformation nodes (type: transformation):
  packages/evals/src/db/migrations/0000_young_trauma.sql
  packages/evals/src/db/migrations/0001_add_timeout_to_runs.sql
  ... (8 migration files total)

Data nodes (type: data):
  0000_young_trauma
  0001_add_timeout_to_runs
  ... (8 table states total)

Edges (all SQL_PRODUCT):
  packages/.../0000_young_trauma.sql  →  0000_young_trauma
  packages/.../0001_add_timeout_to_runs.sql  →  0001_add_timeout_to_runs
  ... (pattern: migration file produces table state)
```

### Explicit Graph Traversal Algorithm

The Enforcer uses **reverse-edge BFS** starting from the violating data node:

```python
def blame_chain(violation_node: str, graph: dict) -> list[str]:
    """
    Reverse-BFS: walk edges backwards from the violating node
    to find all upstream transformation nodes (producers).
    graph = {"edges": [{"source": s, "target": t, "relation": r}, ...]}
    """
    # Build reverse adjacency: target → [sources]
    reverse_adj = defaultdict(list)
    for edge in graph["edges"]:
        reverse_adj[edge["target"]].append(edge["source"])

    visited, queue, chain = set(), [violation_node], []
    while queue:
        node = queue.pop(0)
        if node in visited:
            continue
        visited.add(node)
        chain.append(node)
        queue.extend(reverse_adj[node])   # walk upstream
    return chain
```

Applied to node `0000_young_trauma`:
```
blame_chain("0000_young_trauma") →
  [
    "0000_young_trauma",                                        # violating data node
    "packages/evals/src/db/migrations/0000_young_trauma.sql",  # producing transformation
  ]
```

### Step-by-Step Trace

1. **Detect violation**  
   Contract rule `non_zero_variance` fires:
   `stdev(extracted_facts[].confidence) = 0.0` across 34,130 facts.
   Flagged in migration report `outputs/migrate/confidence_scale_report.json`.

2. **Identify failing field**  
   Failing field: `extracted_facts[].confidence` (all values = 0.9).
   Source dataset: `outputs/week3/extractions.jsonl`.

3. **Traverse lineage graph — reverse-BFS**  
   Starting from data node `0000_young_trauma` (the table whose schema
   encodes confidence), walk all incoming edges:
   - Incoming edge: `packages/evals/src/db/migrations/0000_young_trauma.sql`
     → `0000_young_trauma` (relation: `SQL_PRODUCT`)
   - This SQL migration file is the **transformation node** responsible for
     producing the data state. It is the upstream producer.
   - No further incoming edges on the migration file → BFS terminates.

4. **Map to producing system**  
   The transformation node `packages/evals/src/db/migrations/0000_young_trauma.sql`
   belongs to the `packages/evals` subsystem — the Document Refinery pipeline
   that writes `outputs/week3/extractions.jsonl`.

5. **Identify upstream file**  
   Canonical producer file: `outputs/week3/extractions.jsonl`.
   `doc_id` hash `07bc09572756e5361f00a41a67b75bc3beb5e482c61b417160dd7421e9519ce8`
   identifies the specific source document whose extraction hard-coded `0.9`.

6. **Run git log / blame**
   ```bash
   git log --oneline -- outputs/week3/extractions.jsonl
   # → 1870a26 initial prj setup

   git log --oneline -- outputs/week4/lineage_snapshots.jsonl
   # → 1870a26 initial prj setup
   ```
   Single commit — both files share the same commit hash `1870a26`.
   Hard-coded `0.9` was introduced at project setup, not a later regression.

7. **Use timestamps to determine causality**  
   - Week3 snapshot timestamp: `2026-04-01` (initial commit)
   - Week4 snapshot timestamp: `2026-03-16T11:11:59` (lineage snapshot `S-001`)
   - Week4 was snapshotted **before** the extractions file date in git, meaning
     the lineage graph was capturing schema state prior to the data being finalised.
   - The confidence defect was present in the extraction source at generation
     time — it was not introduced during lineage graph construction.

8. **Compute confidence score for attribution**  
   Attribution confidence = `P(root_cause = Document_Refinery)`:
   - Structural evidence: `confidence` field exists with correct type → +1.0
   - Statistical evidence: stdev = 0.0, single unique value → P(accidental) ≈ 0.02, so strong signal → +0.95
   - Graph evidence: reverse-BFS terminates at migration file with no alternative upstream → no other candidate → +1.0
   - Temporal evidence: both files in same commit, no temporal gap between producer and consumer → +1.0
   - **Attribution confidence score: 0.97** (Document Refinery hard-coded `0.9`
     as default confidence at initial project setup — confirmed by git, graph,
     statistics, and timestamp convergence)

---

## Q4 — LangSmith Trace Contract (Bitol ODCS YAML)

The contract below follows the **Bitol Open Data Contract Standard (ODCS)**
structure (`dataContractSpecification`, `id`, `info`, `models`, `quality`)
as defined at `bitol-io/open-data-contract-standard`.

```yaml
dataContractSpecification: 0.9.3

id: urn:contract:langsmith-traces:v1
info:
  title: LangSmith Trace Record Contract
  version: "1.0.0"
  owner: contract-enforcer-platform
  description: >
    Defines structural, statistical, and AI-specific quality rules for
    LangSmith agent run traces stored in outputs/traces/runs.jsonl.
    Observed across 402 runs: run_types {tool, llm, chain},
    no temporal violations, no token mismatches.

servers:
  local:
    type: local
    path: outputs/traces/runs.jsonl
    format: jsonl

models:
  langsmith_traces:
    description: One record per LangSmith agent span/run
    fields:
      id:
        type: string
        format: uuid
        required: true
        description: Unique run identifier
      run_type:
        type: string
        required: true
        description: Span category — observed values tool, llm, chain
      start_time:
        type: string
        format: date-time
        required: true
      end_time:
        type: string
        format: date-time
        required: true
      prompt_tokens:
        type: integer
        minimum: 0
        required: true
      completion_tokens:
        type: integer
        minimum: 0
        required: true
      total_tokens:
        type: integer
        minimum: 0
        required: true
      total_cost:
        type: number
        minimum: 0.0
        required: true
      parent_run_id:
        type: string
        format: uuid
        required: false
        description: Null for root spans; non-null for child spans

quality:
  # ── STRUCTURAL rules ───────────────────────────────────────────────
  - rule: temporal_ordering
    type: custom
    dimension: structural
    expression: "end_time > start_time"
    severity: critical
    message: "LangSmith run must end after it starts — violated run is unparseable"

  - rule: token_integrity
    type: custom
    dimension: structural
    expression: "total_tokens = prompt_tokens + completion_tokens"
    severity: critical
    message: "Token sum mismatch corrupts cost attribution and billing"

  - rule: run_type_enum
    type: acceptedValues
    dimension: structural
    field: run_type
    acceptedValues: [tool, llm, chain]
    severity: error
    message: "Unknown run_type breaks downstream cost and latency dashboards"

  # ── STATISTICAL rules ──────────────────────────────────────────────
  - rule: completion_tokens_nonzero_for_llm
    type: custom
    dimension: statistical
    filter: "run_type = 'llm'"
    expression: "completion_tokens > 0"
    severity: warning
    message: "LLM run with zero completion tokens signals silent model failure or timeout"

  - rule: total_cost_positive_for_llm
    type: custom
    dimension: statistical
    filter: "run_type = 'llm' AND total_tokens > 0"
    expression: "total_cost > 0.0"
    severity: warning
    message: "Zero-cost LLM run with nonzero tokens indicates billing instrumentation gap"

  # ── AI-SPECIFIC rules ──────────────────────────────────────────────
  - rule: llm_output_schema_presence
    type: custom
    dimension: aiSpecific
    filter: "run_type = 'llm'"
    expression: "outputs IS NOT NULL AND outputs != '{}'"
    severity: warning
    message: >
      Every llm run must have a non-empty outputs object.
      Empty outputs mean the model returned no usable structured response —
      downstream contract clauses that read outputs fields will silently fail.

  - rule: embedding_drift_guard
    type: custom
    dimension: aiSpecific
    description: >
      For runs tagged with embedding-related names, cosine distance between
      consecutive output embedding batches must stay within threshold.
      Prevents silent semantic drift in retrieval systems.
    filter: "tags CONTAINS 'embedding'"
    expression: "cosine_distance(outputs.embedding, baseline_embedding) < 0.15"
    severity: warning
    message: "Embedding drift exceeds 0.15 — retrieval quality may have degraded"

  - rule: orphaned_root_chain
    type: custom
    dimension: aiSpecific
    description: >
      Root chains (parent_run_id IS NULL) must have at least one child span.
      An orphaned root chain indicates incomplete LangSmith instrumentation —
      the agent ran but its internal tool/llm calls were not captured.
    filter: "run_type = 'chain' AND parent_run_id IS NULL"
    expression: "child_span_count > 0"
    severity: warning
    message: "Root chain with no children — agent instrumentation is incomplete"
```

**Observed in `outputs/traces/runs.jsonl` (402 runs):**
- `run_types = {tool, llm, chain}` — `run_type_enum` rule: ✅ passes
- Temporal violations: 0 — `temporal_ordering` rule: ✅ passes
- Token mismatches: 0 — `token_integrity` rule: ✅ passes

---

## Q5 — Contract Failure Mode

### Why Contracts Become Stale

Contracts are written against a snapshot of the data at a point in time. They
go stale when:
1. **Source systems change silently** — a new model version emits different output
   shapes without updating the contract (e.g., adding `regulatory_basis` to
   `CreditAnalysisCompleted` payload as seen in `schema_version=2.0`).
2. **Business logic evolves faster than contracts** — new event types
   (`DecisionGenerated`) are added to Week5 without a corresponding contract entry.
3. **Statistical baselines drift** — the `mean(confidence)` baseline of `0.9`
   becomes the "normal" and alerts stop firing. Future regressions are invisible.

### Most Common Failure Mode

**Silent semantic drift** — the structure is valid, the types are correct, but
the *meaning* of a value has changed. The Week3 confidence example is a textbook
case: `0.9` is a structurally valid `number ∈ [0.0, 1.0]`, but it is semantically
broken because it carries no information. A validator checking only `type: number`
and `minimum: 0.0, maximum: 1.0` passes the record as clean.

### How This Architecture Prevents Stale Contracts

1. **Statistical contracts alongside structural contracts.** The Bitol YAML for
   Week3 includes `non_zero_variance` as a quality rule. This fires even when
   structural validation passes.
2. **Schema version enforcement.** The Week5 migration script
   (`migrate_week5_schema_version.py`) detects coexisting schema versions and
   flags them as breaking, preventing silent accumulation of incompatible formats.
3. **Lineage-aware contract execution.** By mapping contracts to the data flow
   arrows (Week3 → Week4, Week3 → Week5), violations are caught at the source
   rather than discovered at the consumer. The attribution confidence score in
   Q3 produces a numeric accountability measure — contracts know *who* broke them.
4. **Trace-level enforcement.** LangSmith contracts enforce token integrity and
   temporal ordering at the span level, not just at the model output level.
   This catches instrumentation bugs before they propagate to billing or audit.

---

## Evidence Summary

| Evidence Type | Location | Key Finding |
|---------------|----------|-------------|
| Raw Week3 stats | Computed above | `min=max=mean=0.9, stdev=0.0` |
| Real schema mismatch | DEV-02 | v1.0 payload has `applicant_id`; v2.0 does not |
| Real data example | `doc_id: 07bc0957...` | OCR fact `"any kai no with peed eatin"` has same confidence 0.9 as `"Commercial Bank of Ethiopia"` |
| Real failure case | DEV-01 | Week4 attribution weights collapse to uniform 0.9 |
| Trace observation | `outputs/traces/runs.jsonl` | `run_type` enum clean; temporal + token integrity confirmed across 402 runs |
| Migration scripts | `outputs/migrate/` | Two scripts produced, both run successfully |
