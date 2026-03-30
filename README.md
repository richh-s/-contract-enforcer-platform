# Data Contract Enforcer

Data Contract Enforcer is a data engineering project focused on JSONL ingestion, validation, and contract enforcement.

## Project Structure

- `contracts/`: CLI Python scripts for contract management.
- `generated_contracts/`: Storage for auto-generated data contracts.
- `validation_reports/`: Detailed validation results.
- `violation_log/`: Log of contract violations.
- `schema_snapshots/`: Historical snapshots of data schemas.
- `enforcer_report/`: Summary reports of enforcer activities.
- `outputs/`: General project outputs.
- `tests/`: Project test suite.

## Setup

1. Create a virtual environment:
   ```bash
   python3 -m venv .venv
   ```
2. Activate the virtual environment:
   ```bash
   source .venv/bin/activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Development

- Linting: `ruff check .`
- Formatting: `black .`
- Type checking: `mypy .`
- Testing: `pytest`
