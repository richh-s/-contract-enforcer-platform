# Data Contract Enforcer

An end-to-end data contract enforcement platform implementing the Bitol ODCS v3 spec.
Generates contracts from raw data, validates at the consumer boundary, detects schema
evolution, runs AI-powered checks, and produces a stakeholder enforcer report.

## Quick Start (full reproduction)

```bash
git clone <repo>
cd contract-enforcer-platform
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

---

## Phase 1 — Generate Contracts

Generate Bitol YAML + dbt schema.yml + schema snapshot from raw JSONL:

```bash
# Week 3 extractions
python contracts/generator.py \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/

# Week 5 events
python contracts/generator.py \
  --source outputs/week5/events.jsonl \
  --contract-id week5-event-stream \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/
```

**Outputs:**
- `generated_contracts/week3_document_refinery_extractions.yaml` — Bitol contract
- `generated_contracts/week3_document_refinery_extractions_dbt.yml` — dbt schema tests
- `schema_snapshots/week3-document-refinery-extractions/<ts>.yaml` — immutable snapshot

---

## Phase 2 — Run Validation (Consumer-Side Enforcement)

The runner is a **consumer-side** tool — it enforces what consumers expect before data is processed.

```bash
# Validate clean data
python contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/week3_final.json

# Validate violated data (confidence scale injected as 0-100)
python contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions_violated.jsonl \
  --output validation_reports/week3_violated.json

# Week 5 events
python contracts/runner.py \
  --contract generated_contracts/week5_events.yaml \
  --data outputs/week5/events.jsonl \
  --output validation_reports/week5_final.json
```

**Expected violations on violated data:**
- `CRITICAL` — confidence range: 50 records outside [0.0, 1.0] (actual: 90.0)
- `HIGH`     — z-score drift: all confidence values identical (std=0)
- `MEDIUM`   — non-zero variance: extractor returning hard-coded default

---

## Phase 3 — Schema Evolution Analysis

Diff consecutive snapshots, classify changes, compute blast radius via registry:

```bash
# Week 3 — detects CRITICAL confidence scale break (0.9 → 100.0)
python contracts/schema_analyzer.py \
  --contract-id week3-extractions \
  --since "7 days ago" \
  --output validation_reports/schema_evolution_week3.json

# Week 5 — detects ENUM_ADDITION (LoanApproved/LoanRejected) + REMOVE_COLUMN
python contracts/schema_analyzer.py \
  --contract-id week5-events \
  --output validation_reports/schema_evolution_week5.json

# Week 3 document refinery (older schema: format)
python contracts/schema_analyzer.py \
  --contract-id week3_document_refinery_extractions \
  --output validation_reports/schema_evolution_week3_refinery.json
```

**Outputs per run:**
- `validation_reports/schema_evolution_<id>.json` — full evolution report
- `validation_reports/migration_impact_<id>_<ts>.json` — migration impact artifact

**Exit codes:** `0` = compatible, `2` = breaking changes detected (CI-catchable)

---

## Phase 4A — AI Contract Extensions

Three AI validation checks against real data:

```bash
python contracts/ai_extensions.py \
  --mode all \
  --extractions outputs/week3/extractions.jsonl \
  --verdicts    outputs/week2/verdicts.jsonl \
  --output      validation_reports/ai_extensions.json
```

| Extension | What it detects |
|-----------|----------------|
| Embedding Drift | Semantic shift in extracted text (LSA / OpenAI when key set) |
| Prompt Validation | Records with invalid doc_id, confidence outside [0.0,1.0]; quarantined to `outputs/quarantine/quarantine.jsonl` |
| LLM Output Schema | Verdicts outside {PASS,FAIL,WARN}; trend vs baseline |

**To verify drift fires on violated data:**
```bash
# First — set baseline from clean data
python contracts/ai_extensions.py --mode prompt \
  --extractions outputs/week3/extractions_violated.jsonl \
  --verdicts outputs/week2/verdicts.jsonl \
  --output /tmp/ai_violated.json
# Expected: quarantined_records=50, status=WARN
```

---

## Phase 4B — Enforcer Report

Aggregates all real validation data into one stakeholder report:

```bash
python contracts/report_generator.py \
  --output enforcer_report/report_data.json
```

**Inputs consumed (nothing hardcoded):**
- `validation_reports/*.json` — all runner outputs
- `violation_log/violations.jsonl` — runtime violations
- `validation_reports/ai_extensions.json` — AI check results
- `validation_reports/schema_evolution_*.json` — schema diff reports

**Health score formula:**
```
base  = (passed / total_checks) × 100
score = max(0, base − 20 × critical_failures)
```

---

## Full End-to-End Reproduction

```bash
# 1. Setup
python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt

# 2. Generate contracts
python contracts/generator.py --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl --output generated_contracts/

# 3. Run validation (clean + violated)
python contracts/runner.py --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions.jsonl --output validation_reports/week3_final.json

python contracts/runner.py --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions_violated.jsonl --output validation_reports/week3_violated.json

# 4. Schema evolution
python contracts/schema_analyzer.py --contract-id week3-extractions \
  --output validation_reports/schema_evolution_week3.json

python contracts/schema_analyzer.py --contract-id week5-events \
  --output validation_reports/schema_evolution_week5.json

# 5. AI extensions
python contracts/ai_extensions.py --mode all \
  --extractions outputs/week3/extractions.jsonl \
  --verdicts outputs/week2/verdicts.jsonl \
  --output validation_reports/ai_extensions.json

# 6. Enforcer report
python contracts/report_generator.py --output enforcer_report/report_data.json
```

---

## Project Structure

```
contracts/
  generator.py          Phase 1 — contract + dbt + snapshot generator
  runner.py             Phase 2 — consumer-side validation runner
  schema_analyzer.py    Phase 3 — schema evolution analyzer (registry-first blast radius)
  registry.py           Contract registry — PRIMARY blast radius source
  ai_extensions.py      Phase 4A — embedding drift, prompt validation, LLM output schema
  report_generator.py   Phase 4B — enforcer report aggregator

generated_contracts/    Bitol YAML + dbt schema.yml per contract
schema_snapshots/       Immutable timestamped contract snapshots (≥2 per contract)
validation_reports/     Runner JSON reports + schema evolution + AI extension results
violation_log/          Runtime violations.jsonl (blame chain + blast radius)
enforcer_report/        report_data.json — stakeholder enforcer report
outputs/
  week3/extractions.jsonl         Clean data
  week3/extractions_violated.jsonl Injected violation (confidence=90, not 0.9)
  quarantine/quarantine.jsonl      Records blocked by prompt validation
```

## Key Artifacts (pre-generated, committed)

| Artifact | Location |
|----------|----------|
| Bitol contracts | `generated_contracts/*.yaml` |
| dbt schema files | `generated_contracts/*_dbt.yml` |
| Schema evolution (week3) | `validation_reports/schema_evolution_week3.json` |
| Schema evolution (week5) | `validation_reports/schema_evolution_week5.json` |
| AI extensions | `validation_reports/ai_extensions.json` |
| Enforcer report | `enforcer_report/report_data.json` |
| Violations (3 records) | `violation_log/violations.jsonl` |

## Development

```bash
ruff check .    # lint
black .         # format
pytest          # tests
```
