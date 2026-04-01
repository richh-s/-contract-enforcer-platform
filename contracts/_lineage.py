"""
contracts/_lineage.py
Lineage loading and Git enrichment for Phase 1 contract generation.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional


def load_latest_lineage(lineage_path: str) -> Optional[dict]:
    """
    Read all JSONL records from lineage_path and return the record
    with the latest 'timestamp' value (ISO string sort is safe for
    ISO-8601 timestamps).

    Returns a normalised dict:
    {
        "snapshot_id"   : str,
        "timestamp"     : str,
        "node_count"    : int,
        "edge_count"    : int,
        "node_types"    : list[str],    # unique node types present
        "edge_relations": list[str],    # unique relation types present
        "source_file"   : str,
        "upstream"      : [],           # populated by caller if needed
        "downstream"    : [             # week4 cartographer as default consumer
            {
                "id": "week4-cartographer",
                "description": "Cartographer ingests doc_id and extracted_facts as node metadata",
                "fields_consumed": ["doc_id", "extracted_facts", "extraction_model"],
                "breaking_if_changed": ["extracted_facts.confidence", "doc_id"],
            }
        ],
    }

    Returns None if file is missing or empty.
    """
    path = Path(lineage_path)
    if not path.exists():
        return None

    records: list[dict] = []
    try:
        with path.open() as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    except (json.JSONDecodeError, OSError):
        return None

    if not records:
        return None

    # Pick the record with the latest timestamp (lexicographic ISO sort)
    latest = max(records, key=lambda r: r.get("timestamp", ""))

    nodes = latest.get("nodes", [])
    edges = latest.get("edges", [])

    node_types    = sorted({n.get("type", "unknown") for n in nodes})
    edge_relations = sorted({e.get("relation", "unknown") for e in edges})

    return {
        "snapshot_id":    latest.get("snapshot_id", "unknown"),
        "timestamp":      latest.get("timestamp", ""),
        "node_count":     len(nodes),
        "edge_count":     len(edges),
        "node_types":     node_types,
        "edge_relations": edge_relations,
        "source_file":    str(path),
        "upstream":       [],
        "downstream": [
            {
                "id":                  "week4-cartographer",
                "description":         "Cartographer ingests doc_id and extracted_facts as node metadata",
                "fields_consumed":     ["doc_id", "extracted_facts", "extraction_model"],
                "breaking_if_changed": ["extracted_facts.confidence", "doc_id"],
            }
        ],
    }


def enrich_with_git(lineage: dict, repo_path: str = ".") -> dict:
    """
    Add git context to the lineage dict using gitpython.
    Gracefully returns lineage unchanged if repo is missing or git fails.

    Adds:
        git_commit       : first 12 chars of HEAD hexsha
        git_author       : author name of HEAD commit
        git_committed_at : ISO-8601 committed date
        git_message      : first line of commit message
    """
    try:
        import git as _git  # type: ignore
        repo = _git.Repo(repo_path, search_parent_directories=True)
        head = repo.head.commit
        lineage = dict(lineage)
        lineage["git_commit"]       = head.hexsha[:12]
        lineage["git_author"]       = str(head.author)
        lineage["git_committed_at"] = head.committed_datetime.isoformat()
        lineage["git_message"]      = head.message.strip().splitlines()[0]
    except Exception:
        pass  # Never crash because git context is missing
    return lineage
