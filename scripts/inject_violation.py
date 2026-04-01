"""
scripts/inject_violation.py
Phase 2 — Mandatory violation injection for end-to-end testing.

Reads:   outputs/week3/extractions.jsonl
Mutates: confidence *= 100  (0.9 → 90.0, clearly outside [0.0, 1.0])
Writes:  outputs/week3/extractions_violated.jsonl

Run:
    python scripts/inject_violation.py

Then validate:
    python contracts/runner.py \\
        --contract generated_contracts/week3_extractions.yaml \\
        --data     outputs/week3/extractions_violated.jsonl \\
        --output   validation_reports/week3_violated.json

Then attribute:
    python contracts/attributor.py \\
        --report   validation_reports/week3_violated.json \\
        --contract generated_contracts/week3_extractions.yaml \\
        --lineage  outputs/week4/lineage_snapshots.jsonl \\
        --output   violation_log/violations.jsonl
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_SRC  = Path("outputs/week3/extractions.jsonl")
_DEST = Path("outputs/week3/extractions_violated.jsonl")

_VIOLATION_FACTOR = 100   # confidence 0.9 → 90.0


def inject(src: Path = _SRC, dest: Path = _DEST) -> None:
    if not src.exists():
        print(f"[inject] ERROR: source file not found: {src}", file=sys.stderr)
        sys.exit(1)

    records_in  = 0
    facts_mutated = 0
    mutated_records: list[str] = []

    with src.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            records_in += 1
            facts = rec.get("extracted_facts", [])
            for fact in facts:
                if "confidence" in fact and fact["confidence"] is not None:
                    fact["confidence"] = round(float(fact["confidence"]) * _VIOLATION_FACTOR, 4)
                    facts_mutated += 1

            mutated_records.append(json.dumps(rec))

    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text("\n".join(mutated_records) + "\n")

    print(f"[inject] {records_in} records read from  {src}")
    print(f"[inject] {facts_mutated} facts mutated  (confidence *= {_VIOLATION_FACTOR})")
    print(f"[inject] Violated data written to {dest}")
    print()
    print("Next steps:")
    print(f"  python contracts/runner.py \\")
    print(f"      --contract generated_contracts/week3_extractions.yaml \\")
    print(f"      --data     {dest} \\")
    print(f"      --output   validation_reports/week3_violated.json")
    print()
    print(f"  python contracts/attributor.py \\")
    print(f"      --report   validation_reports/week3_violated.json \\")
    print(f"      --contract generated_contracts/week3_extractions.yaml \\")
    print(f"      --lineage  outputs/week4/lineage_snapshots.jsonl \\")
    print(f"      --output   violation_log/violations.jsonl")


if __name__ == "__main__":
    inject()
