# Contract Enforcer Platform — Interim Report
**Trainee:** richh-s  
**Date:** 2026-04-01  
**Branch:** interim  
**Repo:** `-contract-enforcer-platform`

---

## 1. Data Flow Diagram

The platform spans five interacting systems. Every arrow below names the file path, the exact fields carried across the interface, and the data artifact being transferred.

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

| Arrow | From → To | File | Key Fields Carried | Data Artifact | Failure Present |
|-------|-----------|------|--------------------|---------------|-----------------|
| ① | Week1 → Week2 | `outputs/week1/intent_records.jsonl` | `intentId, filePath, outcome, revisionId, contentHash` | IntentRecord | YES — 8 of 11 `id` values fail UUID pattern (non-standard format) |
| ② | Week3 → Week4 | `outputs/week3/extractions.jsonl` | `doc_id, extracted_facts[*].confidence` | ExtractionRecord | YES — `confidence` stdev=0.0 (flat weight, breaks attribution) |
| ③ | Week3 → Week5 | `outputs/week3/extractions.jsonl` | `doc_id, extracted_facts[*].fact` | ExtractionRecord | YES — `source_hash`, `extraction_model`, `fact_id` absent from data |
| ④ | Week4 → Attribution | `outputs/week4/lineage_snapshots.jsonl` | `nodes[].id, nodes[].type, edges[].relation` | LineageSnapshot | YES — `node_count`/`edge_count` fields absent; sparse node schema |
| ⑤ | Week5 → Contracts | `outputs/week5/events.jsonl` | `event_type, schema_version, payload` | EventRecord | YES — mixed `schema_version` 1.0+2.0; 4 clock-skew violations |
| ⑥ | Traces → AI Validation | `outputs/traces/runs.jsonl` | `run_type, start_time, end_time, prompt_tokens, completion_tokens, total_tokens` | RunRecord | **None** — 15/15 checks pass |

---

## 2. Contract Coverage Table

Every inter-system interface from the diagram above is represented. Each row states whether a contract has been written, and every Partial or No entry states the concrete reason.

| Interface | Contract File | Status | Clause Count | Rationale / Gap |
|-----------|---------------|--------|--------------|-----------------|
| **Week1 IntentRecord schema** | `week1_intent_records.yaml` | **Yes** | 24 checks | Full coverage: UUID format, timestamp, tool not_null, contentHash hex64, outcome enum, mutationType enum |
| **Week2 VerdictRecord schema** | _(none)_ | **No** | — | `outputs/week2/verdicts.jsonl` was not profiled. Schema not yet understood — `verdict_id`, `decision`, `rationale` fields need validation clause design before contract can be written |
| **Week3 ExtractionRecord schema** | `week3_document_refinery_extractions.yaml` | **Partial** | 11 checks | Core clauses present (`doc_id` pattern, `confidence` range + variance). Missing: `source_hash`, `extraction_model`, `fact_id` — these fields are declared in the contract but **absent from the actual data**. Contract reflects the intended schema, not the current emission |
| **Week3 → Week4 confidence dependency** | `week3_document_refinery_extractions.yaml` | **Partial** | 1 statistical clause | `non_zero_variance` rule exists and fires. But no cross-contract clause formally links Week3's `confidence` to Week4's attribution weight — the dependency is documented in `lineage.downstream` but not enforced as a contract rule |
| **Week4 LineageSnapshot schema** | `week4_lineage.yaml` | **Partial** | 4 checks | `snapshot_id` and `timestamp` covered. Node and edge array structures are declared in schema but not field-level validated — `node_count`/`edge_count` checks error because these computed fields don't exist in raw JSON. Node `system` and `label` fields are absent; contract cannot enforce provenance completeness |
| **Week5 EventRecord schema** | `week5_events.yaml` | **Partial** | 25 checks | Strong coverage of top-level fields. Gap: `payload` is declared as `type: object` but payload shape varies by `event_type` — no per-event-type payload schema exists. Clock skew rule fires on 4 events but mixed `schema_version` (1.0 vs 2.0) has no version-routing contract clause |
| **Week5 → Contracts pipeline** | `week5_events.yaml` | **Yes** | 25 checks | Schema version not_null, event_type enum, stream_position monotonic rules all enforced |
| **LangSmith Trace schema** | `langsmith_traces.yaml` | **Yes** | 15 checks | Full coverage: UUID format, run_type enum, start/end time not_null, token arithmetic, cost range. All 15 checks pass on 402 runs |
| **Traces → AI validation** | `langsmith_traces.yaml` | **Yes** | 15 checks | Token integrity (`total = prompt + completion`) and `run_type` enum prevent silent AI billing errors |

### Coverage Summary

| Status | Count | Interfaces |
|--------|-------|-----------|
| Yes — fully covered | 3 | Week1, Week5→Contracts, Traces |
| Partial — contract exists, gaps remain | 4 | Week3, Week3→Week4 dependency, Week4, Week5 |
| No — contract not yet written | 1 | Week2 (verdicts) |

**Critical coverage gap:** The Week3 → Week4 confidence-weight dependency is the highest-risk uncovered interface. A confidence scale change (0–1 → 0–100) would pass both the Week3 structural contract and the Week4 contract individually, but silently corrupt attribution. A cross-contract rule linking the two is needed.

---

## 3. Validation Run Evidence & Interpretation

All five contracts were run against live data on 2026-04-01. Results are interpreted as risk signals, not just pass/fail counts.

### Run Summary

| Dataset | Total Checks | Passed | Failed | Errored | Warned | Exit |
|---------|-------------|--------|--------|---------|--------|------|
| Week3 Extractions | 11 | 7 | 1 | 3 | 0 | 0 |
| Week5 Events | 25 | 24 | 1 | 0 | 0 | 0 |
| Week1 Intent Records | 24 | 23 | 1 | 0 | 0 | 0 |
| Week4 Lineage | 4 | 2 | 2 | 0 | 0 | 0 |
| LangSmith Traces | 15 | 15 | 0 | 0 | 0 | 0 |
| **Total** | **79** | **71** | **5** | **3** | **0** | — |

---

### Violation 1 — Week3: Zero-Variance Confidence (STATISTICAL FAIL)

**Check:** `week3.extracted_facts.confidence.non_zero_variance`  
**Result:** `FAIL` — `actual=std=0.0000, mean=0.9000` vs `expected=std>0.0`  
**Message:** *"'extracted_facts[*].confidence': std=0.0, mean=0.9000 — all 34130 values are identical (likely hard-coded default)."*

**What this means:** Every extracted fact across all 50 documents has exactly `confidence=0.9`. A number that never varies carries no information — it is a constant masquerading as a measurement. This passes a structural check (`type: number`, `minimum: 0.0`, `maximum: 1.0`) cleanly, which is precisely why it requires a dedicated statistical rule.

**Why it matters downstream:** Week4's Brownfield Cartographer uses `confidence` as an edge weight in lineage attribution. If all weights are 0.9, the attribution graph is completely flat — garbled OCR text (`"any kai no with peed eatin"`, confidence=0.9) has identical weight to a clean entity extraction (`"Commercial Bank of Ethiopia"`, confidence=0.9). The blame chain cannot rank sources by reliability.

**Risk level:** HIGH — silent semantic failure, undetectable without statistical contracts.

---

### Violations 2–4 — Week3: Missing Fields (STRUCTURAL ERROR)

**Checks:** `week3.source_hash.pattern`, `week3.extraction_model.pattern`, `week3.extracted_facts.items.fact_id.pattern`  
**Result:** `ERROR` (column_missing) on all three  
**Message:** *"Field 'source_hash' not present in any record (missing column)."* (same for the other two)

**What this means:** The contract declares three fields as required (`source_hash`, `extraction_model`, `fact_id`) that do not exist in the actual Week3 data. This revealed a gap between what the Document Refinery pipeline *should* emit and what it *currently* emits. The contract was written against the canonical specification; the data was generated before that specification was fully implemented.

**Why it matters:** The `extraction_model` field is the provenance chain — without it, you cannot know which LLM produced a given extraction. The `fact_id` field enables idempotent downstream upserts — without it, Week4 cannot deduplicate re-extractions of the same document. These are not aspirational fields; they are functional requirements with real downstream effects.

**Risk level:** MEDIUM-HIGH — functional capabilities (deduplication, model attribution) are blocked until the pipeline emits these fields.

---

### Violation 5 — Week5: Clock Skew (CROSS-FIELD FAIL)

**Check:** `week5.timestamp.cross_field_timestamp`  
**Result:** `FAIL` — `actual=violations=4` vs `expected=timestamp>=payload.*_at`  
**Message:** *"timestamp < payload.*_at in 4 events — clock skew detected."*

**What this means:** Four events have a top-level `timestamp` (when the event was recorded by the event bus) that is *earlier* than a `*_at` field inside the payload (when the business action actually occurred). This is a temporal ordering contradiction — the event log claims to have recorded something before it happened.

**Why it matters:** Event sourcing systems rely on monotonic timestamps for replay. If `timestamp` precedes `payload.submitted_at`, replaying the event stream in timestamp order will re-process events in the wrong sequence. In a lending pipeline, this means a compliance check could appear to precede the application submission that triggered it.

**Risk level:** HIGH — data integrity violation; replay-based audit trails are unreliable.

---

### Violation 6 — Week1: Non-UUID IDs (STRUCTURAL FAIL)

**Check:** `week1.id.pattern`  
**Result:** `FAIL` — `actual=non_matching=8` vs UUID pattern  
**Message:** *"'id': 8 records do not match pattern '^[0-9a-f]{8}-[0-9a-f]{4}-...'."*

**What this means:** 8 of 11 intent records have `id` values that are not valid UUIDs. This was introduced when synthetic records were generated to meet the data minimums — the generator used non-UUID format identifiers. The contract immediately caught this.

**Why it matters:** Downstream consumers that join on `id` across the audit trail would fail to resolve these records. UUID format is not cosmetic — it is a join key.

**Risk level:** MEDIUM — affects 73% of Week1 records; breaks cross-system audit trail joins.

---

### Violations 7–8 — Week4: Computed Fields Absent (STRUCTURAL FAIL)

**Checks:** `week4.node_count.not_null`, `week4.edge_count.not_null`  
**Result:** `FAIL` — `actual=null_count=1`  
**Message:** *"'node_count': 1 records with null/empty values."*

**What this means:** The runner checks `node_count` and `edge_count` as top-level numeric fields, but in the raw JSON these are computed from `len(nodes)` and `len(edges)` — they don't exist as stored columns. This is a contract-vs-data representation mismatch: the contract was designed assuming the profiler's computed fields would be present at runtime, but the runner evaluates the raw JSONL directly.

**Why it matters:** This is itself a design insight — contracts must be written against the raw data shape, not against a profiling-time computed view. The structural check is correct in intention but wrong in implementation.

**Risk level:** LOW (structural design issue) — the underlying data has 16 nodes and 11 edges; the snapshot is not actually empty.

---

### Traces — Clean Pass

**Result:** 15/15 checks PASS across 402 runs.

Checks confirmed: `id` UUID format, `run_type` in `{tool, llm, chain}`, `start_time` and `end_time` not_null, `total_tokens ≥ 0`, `total_cost ≥ 0`. No temporal violations (all `end_time > start_time`), no token arithmetic violations. The LangSmith instrumentation is the most contract-compliant system in the platform.

---

## 4. Reflection — What Contract Thinking Revealed

### Discovery 1: The doc_id Format Assumption Was Wrong

**Before contracts:** I assumed `doc_id` was a UUID (v4) because the field name follows the pattern of other primary keys in the system. When building the schema block, I wrote `format: uuid` by habit.

**What contract thinking exposed:** Writing the structural rule forced me to actually inspect the data. Running `python3 -c "with open(...) as f: print(json.loads(f.readline())['doc_id'])"` returned `07bc09572756e5361f00a41a67b75bc3beb5e482c61b417160dd7421e9519ce8` — 64 characters, no hyphens. A UUID is 36 characters with 4 hyphens in specific positions. The contract rule I wrote (`format: uuid`) contradicted the pattern rule I also wrote (`pattern: ^[0-9a-f]{64}$`). The contradiction only became visible when both rules were in the same YAML block.

**Shift in mental model:** I now treat every identifier field as having an unknown format until I look at the actual data. `format: uuid` is a claim that requires evidence, not a default.

---

### Discovery 2: Statistical Silence Is Invisible Without Statistical Contracts

**Before contracts:** I assumed that if a field was present and within the declared range, it was healthy. The confidence field in Week3 was present, non-null, and in [0.0, 1.0] — structurally clean.

**What contract thinking exposed:** Writing the `non_zero_variance` statistical rule and running it produced: `std=0.0000, mean=0.9000 — all 34130 values are identical`. A structural validator would have marked this PASS. Only a statistical rule catches it. The failure is not in the type or range — it is in the absence of information.

**Shift in mental model:** A field that always equals 0.9 is not a confidence score — it is a constant. Structural contracts tell you the *shape* of data is correct. Statistical contracts tell you whether the data has *meaning*. Both are required. A system with only structural contracts has a validation blind spot over the entire statistical dimension.

---

### Discovery 3: Contracts Reveal What the System Claims vs What It Emits

**Before contracts:** I assumed the Week3 pipeline emitted `source_hash`, `extraction_model`, and `fact_id` — these seemed like obvious provenance fields that any extraction pipeline would include.

**What contract thinking exposed:** Writing the contract forced me to declare these fields as required. Running the validator produced three consecutive `column_missing` errors. The pipeline generates `doc_id` and `extracted_facts[]{fact, confidence}` and nothing else. The fields I considered "obvious" simply do not exist in the current output.

**Shift in mental model:** Contracts make implicit requirements explicit and then immediately test them against reality. Without the contract, I would have continued assuming `extraction_model` existed until some downstream join failed silently. The contract surfaced the gap at the source boundary, which is the cheapest point to fix it.

---

### Discovery 4: Clock Skew Is a Silent Event-Sourcing Failure

**Before contracts:** I assumed all Week5 timestamps were consistent — the event system records when something happened, so the timestamp should be close to the payload time.

**What contract thinking exposed:** Writing the cross-field rule `timestamp >= payload.*_at` and running it found 4 violations where the event-bus recording timestamp is *earlier* than the payload's business-action timestamp. This is physically impossible unless one of the two clocks was wrong.

**Shift in mental model:** I had treated timestamps as metadata. The cross-field rule forced me to treat them as a consistency constraint. Event-sourcing systems are only replay-safe if temporal ordering is monotonic. Four events in the stream violate this — they were present in the data the entire time, invisible without the rule.

---

### Discovery 5: The Contract Reveals the Data Model Has an Unreachable Design Point

**Before contracts:** The Week4 lineage graph has `nodes` and `edges` arrays. I assumed checking `node_count > 0` would be straightforward.

**What contract thinking exposed:** The validator reports `node_count: null` because `node_count` is a column I computed *during profiling* (by running `len(nodes)`), but the raw JSONL record only has `nodes` as an array — there is no `node_count` key. The contract was designed against the profiling view, not the raw data shape. The evaluator is correct; the contract author (me) made a structural error.

**Shift in mental model:** The contract must be written against the raw data shape that the consumer will actually receive. The profiling step is for understanding; the contract is for enforcement. These must target the same representation. I now treat every schema field declaration as a claim that the field literally exists as a top-level key in the raw record.

---

### Summary of Wrong Assumptions

| Assumption | What Was Actually True | How Contract Thinking Exposed It |
|------------|----------------------|----------------------------------|
| `doc_id` is a UUID | 64-char SHA-256 hex digest | Writing `format: uuid` contradicted the observed 64-char pattern |
| Confidence has real variance | stdev=0.0 across 34,130 values | `non_zero_variance` statistical rule |
| Week3 emits `source_hash`, `extraction_model`, `fact_id` | None of these fields are present | `column_missing` errors on first validation run |
| Event timestamps are internally consistent | 4 events have `timestamp < payload.*_at` | Cross-field temporal ordering rule |
| `node_count` is a storable field | It is computed from `len(nodes)` — not stored | FAIL on `not_null` check against raw JSONL |
| All Week1 IDs are UUIDs | 8 of 11 records have non-UUID IDs | UUID pattern check immediately flagged them |
