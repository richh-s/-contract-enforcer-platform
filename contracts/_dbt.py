"""
contracts/_dbt.py
dbt YAML counterpart generator for Phase 1.

Maps Bitol contract clauses to dbt schema.yml test structure.

Mapping rules:
    structural / not_null          → "not_null" test
    pattern (uuid/hex)             → "not_null" test + description comment
    accepted_values                → accepted_values test
    range                          → dbt_utils.expression_is_true (emitted as stub)
    cross_field / statistical      → comment-only (no native dbt equivalent)
"""

from __future__ import annotations

from collections import defaultdict


def generate_dbt_yaml(
    contract_id: str,
    model_name: str,
    clauses: list[dict],
) -> dict:
    """
    Convert contract clauses to a dbt schema.yml dict.

    Returns a dict ready for yaml.dump():
    {
        "version": 2,
        "models": [
            {
                "name": model_name,
                "description": "...",
                "columns": [...]
            }
        ]
    }
    """
    # Group clauses by field
    by_field: dict[str, list[dict]] = defaultdict(list)
    for clause in clauses:
        field = clause.get("field", "")
        if field:
            by_field[field].append(clause)

    columns = []
    for field, field_clauses in sorted(by_field.items()):
        tests = []
        col_description = ""

        for clause in field_clauses:
            # Skip aspirational clauses
            if clause.get("status") == "aspirational":
                continue

            clause_type = clause.get("type", "")
            check       = clause.get("check", "")

            # not_null
            if clause_type == "structural" and check == "not_null":
                tests.append("not_null")
                if not col_description:
                    col_description = clause.get("description", "")

            # UUID / hex64 pattern → not_null + unique where applicable
            elif clause_type == "pattern":
                if "not_null" not in tests:
                    tests.append("not_null")
                # For ID-like fields add unique test
                if field.endswith("_id") or field in ("doc_id", "event_id"):
                    if "unique" not in tests:
                        tests.append("unique")
                col_description = col_description or clause.get("description", "")

            # accepted_values
            elif clause_type == "accepted_values":
                values = clause.get("accepted_values", [])
                if isinstance(values, list) and values:
                    tests.append({
                        "accepted_values": {"values": values}
                    })
                col_description = col_description or clause.get("description", "")

            # range → dbt_utils expression stub
            elif clause_type == "range":
                mn = clause.get("minimum")
                mx = clause.get("maximum")
                if mn is not None and mx is not None:
                    expr = f"{field} >= {mn} and {field} <= {mx}"
                    tests.append({
                        "dbt_utils.expression_is_true": {
                            "expression": expr,
                            "# note": "requires dbt-utils package",
                        }
                    })

            # cross_field / statistical → skip (emit as model description note)

        # Only include column if it has at least one test
        if tests:
            col_entry: dict = {"name": field, "tests": tests}
            if col_description:
                col_entry["description"] = col_description
            columns.append(col_entry)

    return {
        "version": 2,
        "models": [
            {
                "name":        model_name,
                "description": (
                    f"dbt model generated from Bitol contract '{contract_id}'. "
                    f"Cross-field and statistical checks are enforced by contracts/runner.py, "
                    f"not by dbt tests."
                ),
                "columns": columns,
            }
        ],
    }
