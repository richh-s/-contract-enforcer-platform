# Data Contract Enforcer

An end-to-end data contract enforcement platform implementing the Bitol ODCS v3 spec.
Generates contracts from raw data, validates at the consumer boundary, detects schema
evolution, runs AI-powered checks, and produces a stakeholder enforcer report.

## Quick Start (fresh clone)

```bash
git clone <repo>
cd contract-enforcer-platform
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

---

## Script 1 — Generate Contracts (`contracts/generator.py`)

Generates a Bitol YAML contract + dbt schema.yml + immutable snapshot from raw JSONL.

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

**Expected output:**
```
[generator] Profiling outputs/week3/extractions.jsonl ...
[generator] 100 records, 5 fields
[generator] Contract written : generated_contracts/week3_document_refinery_extractions.yaml
[generator] dbt schema written: generated_contracts/week3_document_refinery_extractions_dbt.yml
[generator] Snapshot written : schema_snapshots/week3_document_refinery_extractions/20260402T....yaml
```

**Artifacts produced:**
- `generated_contracts/week3_document_refinery_extractions.yaml` — Bitol ODCS v3 contract
- `generated_contracts/week3_document_refinery_extractions_dbt.yml` — dbt schema tests
- `schema_snapshots/week3_document_refinery_extractions/<ts>.yaml` — immutable snapshot

---

## Script 2 — Validate Data (`contracts/runner.py`)

Consumer-side enforcement — validates a JSONL dataset against a Bitol contract.

```bash
# Validate clean data (expect 1 MEDIUM violation: non_zero_variance)
python contracts/runner.py \
  --contract generated_contracts/week3_document_refinery_extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/week3_final.json \
  --mode ENFORCE

# Validate violated data (confidence injected as 90.0 instead of 0.9)
python contracts/runner.py \
  --contract generated_contracts/week3_document_refinery_extractions.yaml \
  --data outputs/week3/extractions_violated.jsonl \
  --output validation_reports/week3_violated.json \
  --mode ENFORCE

# Week 5 events
python contracts/runner.py \
  --contract generated_contracts/week5_event_stream.yaml \
  --data outputs/week5/events.jsonl \
  --output validation_reports/week5_final.json \
  --mode ENFORCE
```

**Enforcement modes:** `AUDIT` (log only, exit 0) | `WARN` (warn, exit 0) | `ENFORCE` (block on CRITICAL/HIGH, default)

**Expected output on clean data:**
```
[runner] mode=ENFORCE | 11 checks | 7 passed | 1 failed | 0 warned | 3 errored
[runner][ENFORCE] ❌ VIOLATIONS DETECTED — pipeline BLOCKED
```
Exit code: `2`

**Expected output on violated data:**
```
[runner] mode=ENFORCE | 11 checks | 5 passed | 3 failed | 0 warned | 3 errored
[runner][ENFORCE] ❌ VIOLATIONS DETECTED — pipeline BLOCKED
```
- `CRITICAL` — confidence range: 50 records outside [0.0, 1.0] (actual: 90.0)
- `HIGH`     — z-score drift: z=99.0 (baseline_mean=0.9, current_mean=90.0)
- `MEDIUM`   — non-zero variance: all confidence values identical

Exit code: `2`

---

## Script 3 — Schema Evolution Analysis (`contracts/schema_analyzer.py`)

Diffs consecutive snapshots, classifies changes, computes registry-first blast radius.

```bash
# Week 3 — detects CRITICAL confidence scale break (0.9 → 90.0)
python contracts/schema_analyzer.py \
  --contract-id week3_document_refinery_extractions \
  --output validation_reports/schema_evolution_week3.json

# Week 5 — detects ENUM_ADDITION (LoanApproved/LoanRejected)
python contracts/schema_analyzer.py \
  --contract-id week5-events \
  --output validation_reports/schema_evolution_week5.json
```

**Expected output:**
```
[schema_analyzer] Snapshots found: 2
[schema_analyzer] Comparing: 20260401T184727Z → 20260402T090000Z
[schema_analyzer] Changes detected: 3 (2 breaking)
[schema_analyzer] Blast radius: 3 consumers affected
[schema_analyzer] Report written: validation_reports/schema_evolution_week3.json
```
**Exit codes:** `0` = compatible, `2` = breaking changes detected (CI-catchable)

**Artifacts produced:**
- `validation_reports/schema_evolution_{id}.json` — full diff + blast radius
- `validation_reports/migration_impact_{id}_{ts}.json` — migration impact artifact

---

## Script 4 — AI Contract Extensions (`contracts/ai_extensions.py`)

Three AI validation checks: embedding drift, prompt validation, LLM output schema.

```bash
# Run all three checks
python contracts/ai_extensions.py \
  --mode all \
  --extractions outputs/week3/extractions.jsonl \
  --verdicts    outputs/week2/verdicts.jsonl \
  --output      validation_reports/ai_extensions.json

# Run on violated data to trigger prompt validation quarantine
python contracts/ai_extensions.py \
  --mode prompt \
  --extractions outputs/week3/extractions_violated.jsonl \
  --verdicts    outputs/week2/verdicts.jsonl \
  --output      /tmp/ai_violated.json
```

**Expected output (clean data):**
```
[ai_extensions] Embedding drift  : status=PASS, drift=0.0000, method=lsa_fallback
[ai_extensions] Prompt validation: status=PASS, valid=100/100, quarantined=0
[ai_extensions] LLM output schema: status=PASS, violations=0/3
[ai_extensions] Report written: validation_reports/ai_extensions.json
```

**Expected output (violated data, --mode prompt):**
```
[ai_extensions] Prompt validation: status=WARN, valid=50/100, quarantined=50
[ai_extensions] Quarantine written: outputs/quarantine/quarantine.jsonl
```
- 50 records quarantined (confidence=90.0 fails JSON Schema range [0.0, 1.0])

---

## Script 5 — Enforcer Report (`contracts/report_generator.py`)

Aggregates all validation data into one stakeholder report with a health score.

```bash
python contracts/report_generator.py \
  --output enforcer_report/report_data.json
```

**Expected output:**
```
[report] Loading validation reports from validation_reports/ ...
[report] Loaded 3 runner reports, 2 schema evolution reports
[report] Loading violations from violation_log/violations.jsonl ...
[report] 4 violations loaded (1 real, 3 injected)
[report] Health score: 59 / 100
[report] Report written: enforcer_report/report_data.json
```

**Health score formula:**
```
base  = (passed / total_checks) × 100
score = max(0, base − 20 × critical_failures)
```

**Artifact produced:** `enforcer_report/report_data.json`

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
python contracts/runner.py \
  --contract generated_contracts/week3_document_refinery_extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/week3_final.json --mode ENFORCE

python contracts/runner.py \
  --contract generated_contracts/week3_document_refinery_extractions.yaml \
  --data outputs/week3/extractions_violated.jsonl \
  --output validation_reports/week3_violated.json --mode ENFORCE

# 4. Schema evolution
python contracts/schema_analyzer.py \
  --contract-id week3_document_refinery_extractions \
  --output validation_reports/schema_evolution_week3.json

python contracts/schema_analyzer.py \
  --contract-id week5-events \
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
  generator.py          Script 1 — contract + dbt + snapshot generator
  runner.py             Script 2 — consumer-side validation runner (--mode AUDIT|WARN|ENFORCE)
  schema_analyzer.py    Script 3 — schema evolution analyzer (registry-first blast radius)
  attributor.py         Violation attributor — blame chain + commit + blast radius
  registry.py           Contract registry — subscriptions.yaml primary, lineage enrichment
  ai_extensions.py      Script 4 — embedding drift, prompt validation, LLM output schema
  report_generator.py   Script 5 — enforcer report aggregator

contract_registry/
  subscriptions.yaml    PRIMARY consumer subscription catalog (11 consumers, 5 contracts)

generated_contracts/    Bitol YAML + dbt schema.yml per contract
schema_snapshots/       Immutable timestamped snapshots (≥2 per contract)
validation_reports/     Runner JSON reports + schema evolution + AI extension results
violation_log/
  violations.jsonl      4 violations (1 real, 3 injected; comment header documents injection)
enforcer_report/
  report_data.json      Machine-generated stakeholder report (health_score=59)
outputs/
  week3/extractions.jsonl           Clean extraction data (100 records)
  week3/extractions_violated.jsonl  Injected violation: confidence=90.0 (was 0.9) in 50/100 records
  quarantine/quarantine.jsonl       Records blocked by prompt validation
```

## Key Pre-Generated Artifacts

| Artifact | Location |
|----------|----------|
| Bitol contracts | `generated_contracts/*.yaml` |
| dbt schema files | `generated_contracts/*_dbt.yml` |
| Schema evolution (week3) | `validation_reports/schema_evolution_week3.json` |
| Schema evolution (week5) | `validation_reports/schema_evolution_week5.json` |
| AI extensions | `validation_reports/ai_extensions.json` |
| Enforcer report | `enforcer_report/report_data.json` |
| Violations (4 records) | `violation_log/violations.jsonl` |
| Registry index | `schema_snapshots/registry_index.json` |

## Development

```bash
ruff check .    # lint
black .         # format
pytest          # tests
```
