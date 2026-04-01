"""
contracts/attributor.py
Phase 2 — ViolationAttributor

Traces validation failures back to source commits via:
  1. Loading a validation report JSON
  2. BFS traversal of the lineage graph upstream from the failing system
  3. git log (recent commits per candidate file)
  4. git blame -L (line-level attribution on candidate files)
  5. Confidence scoring: score = 1.0 - (days_old * 0.1) - (lineage_depth * 0.2)
  6. Ranked blame chain (1–5 candidates, never fewer than 1)
  7. Blast radius from contract lineage.downstream
  8. Append to violation_log/violations.jsonl

CLI usage:
    python contracts/attributor.py \\
        --report   validation_reports/week3_violated.json \\
        --contract generated_contracts/week3_extractions.yaml \\
        --lineage  outputs/week4/lineage_snapshots.jsonl \\
        --output   violation_log/violations.jsonl
"""

from __future__ import annotations

import argparse
import datetime
import json
import subprocess
import sys
import uuid
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Optional

import yaml

# ── sys.path guard ─────────────────────────────────────────────────────────────
_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

_DAYS_LOOKBACK = 14
_MAX_BLAME     = 5


# ═══════════════════════════════════════════════════════════════════════════════
# LINEAGE — BFS upstream traversal
# ═══════════════════════════════════════════════════════════════════════════════

def load_lineage_snapshot(lineage_path: str) -> Optional[dict]:
    """Load the latest lineage snapshot from a JSONL file."""
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
    return max(records, key=lambda r: r.get("timestamp", ""))


def bfs_upstream(
    lineage_record: dict,
    start_system:   str,
    max_depth:      int = 5,
) -> list[dict]:
    """
    BFS upstream from nodes whose ID contains `start_system`.
    Returns list of dicts: {node_id, node, depth}.

    Traversal follows reverse edges (target → source), i.e. upstream.
    Falls back to all transformation nodes if no seed is found.
    """
    nodes     = {n["id"]: n for n in lineage_record.get("nodes", [])}
    edges     = lineage_record.get("edges", [])

    # Reverse adjacency: target → [source, ...]
    reverse: dict[str, list[str]] = defaultdict(list)
    for edge in edges:
        src = edge.get("source")
        tgt = edge.get("target")
        if src and tgt and src != tgt:   # skip self-loops
            reverse[tgt].append(src)

    # Seed: nodes whose ID mentions start_system
    seeds = [
        nid for nid in nodes
        if start_system.lower() in nid.lower()
    ]
    # If no direct match, try to seed from transformation nodes
    if not seeds:
        seeds = [
            nid for nid, n in nodes.items()
            if n.get("type") == "transformation"
        ]
    # Last resort: all nodes
    if not seeds:
        seeds = list(nodes.keys())

    visited: set[str] = set()
    queue            = deque((nid, 0) for nid in seeds)
    result:  list[dict] = []

    while queue:
        node_id, depth = queue.popleft()
        if node_id in visited or depth > max_depth:
            continue
        visited.add(node_id)
        node = nodes.get(node_id, {"id": node_id, "type": "unknown"})
        result.append({"node_id": node_id, "node": node, "depth": depth})
        for src in reverse.get(node_id, []):
            if src not in visited:
                queue.append((src, depth + 1))

    return result


def extract_file_candidates(
    bfs_result:  list[dict],
    repo_root:   str = ".",
    start_system: str = "",
) -> list[dict]:
    """
    Turn BFS nodes into candidate file paths.

    Priority:
      1. Transformation nodes whose ID looks like a real file path (contains /).
      2. Any node ID that is a file that exists on disk.
      3. Heuristic: scan git log for Python/SQL files containing start_system.
    """
    repo = Path(repo_root).resolve()
    seen: set[str] = set()
    candidates: list[dict] = []

    for item in bfs_result:
        nid   = item["node_id"]
        depth = item["depth"]
        node  = item["node"]

        if "/" not in nid and node.get("type") != "transformation":
            continue

        # Try as repo-relative path first, then absolute
        for candidate_path in [repo / nid, Path(nid)]:
            if candidate_path.exists() and str(candidate_path) not in seen:
                seen.add(str(candidate_path))
                candidates.append({
                    "path":   str(candidate_path),
                    "depth":  depth,
                    "source": "lineage",
                })
                break
        else:
            # Store the ID as a path hint (git may still know it)
            if nid not in seen:
                seen.add(nid)
                candidates.append({
                    "path":   nid,
                    "depth":  depth,
                    "source": "lineage_hint",
                })

    # Supplement with git-tracked files related to start_system
    if start_system:
        for git_path in _git_files_for_system(start_system, repo_root):
            if git_path not in seen:
                seen.add(git_path)
                candidates.append({
                    "path":   git_path,
                    "depth":  3,   # heuristic distance
                    "source": "git_heuristic",
                })

    return candidates


def _git_files_for_system(system: str, repo_root: str) -> list[str]:
    """
    Find tracked files in the repo that relate to `system`
    (by filename keyword match) — used when lineage is sparse.
    """
    try:
        result = subprocess.run(
            ["git", "ls-files", "--", f"*{system}*", "*.py", "*.sql"],
            capture_output=True, text=True, cwd=repo_root, timeout=10,
        )
        return [
            line.strip() for line in result.stdout.splitlines()
            if line.strip() and system.lower() in line.lower()
        ]
    except Exception:
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# GIT — log + blame
# ═══════════════════════════════════════════════════════════════════════════════

def git_log_for_file(
    file_path: str,
    days:      int = _DAYS_LOOKBACK,
    repo_root: str = ".",
) -> list[dict]:
    """Run git log --since for a specific file. Returns list of commit dicts."""
    try:
        result = subprocess.run(
            [
                "git", "log",
                "--follow",
                f"--since={days} days ago",
                "--format=%H|%ae|%aI|%s",
                "--", file_path,
            ],
            capture_output=True, text=True, cwd=repo_root, timeout=10,
        )
        commits = []
        for line in result.stdout.strip().splitlines():
            if not line:
                continue
            parts = line.split("|", 3)
            if len(parts) >= 4:
                commits.append({
                    "commit_hash":      parts[0].strip(),
                    "author":           parts[1].strip(),
                    "commit_timestamp": parts[2].strip(),
                    "commit_message":   parts[3].strip(),
                    "file_path":        file_path,
                })
        return commits
    except Exception:
        return []


def git_log_recent_all(
    days:      int = _DAYS_LOOKBACK,
    repo_root: str = ".",
) -> list[dict]:
    """
    Broad git log (all files, last N days).
    Returns commits with a 'changed_files' list.
    Used as fallback when per-file log yields nothing.
    """
    try:
        result = subprocess.run(
            [
                "git", "log",
                f"--since={days} days ago",
                "--name-only",
                "--format=COMMIT|%H|%ae|%aI|%s",
            ],
            capture_output=True, text=True, cwd=repo_root, timeout=15,
        )
        commits: list[dict] = []
        current: dict       = {}
        for line in result.stdout.splitlines():
            if line.startswith("COMMIT|"):
                if current:
                    commits.append(current)
                parts = line.split("|", 4)
                current = {
                    "commit_hash":      parts[1].strip() if len(parts) > 1 else "",
                    "author":           parts[2].strip() if len(parts) > 2 else "",
                    "commit_timestamp": parts[3].strip() if len(parts) > 3 else "",
                    "commit_message":   parts[4].strip() if len(parts) > 4 else "",
                    "changed_files":    [],
                }
            elif line.strip() and current:
                current["changed_files"].append(line.strip())
        if current:
            commits.append(current)
        return commits
    except Exception:
        return []


def git_blame_lines(
    file_path:  str,
    start_line: int = 1,
    end_line:   int = 20,
    repo_root:  str = ".",
) -> list[dict]:
    """
    Run git blame -L start,end --porcelain on a file.
    Returns list of {commit_hash, author, commit_timestamp}.
    """
    try:
        result = subprocess.run(
            [
                "git", "blame",
                f"-L{start_line},{end_line}",
                "--porcelain",
                "--", file_path,
            ],
            capture_output=True, text=True, cwd=repo_root, timeout=10,
        )
        blame: dict[str, Any] = {}
        results: list[dict]   = []
        for line in result.stdout.splitlines():
            if len(line) >= 40 and all(c in "0123456789abcdef" for c in line[:8]):
                if blame:
                    results.append(dict(blame))
                blame = {"commit_hash": line[:40]}
            elif line.startswith("author "):
                blame["author"] = line[7:].strip()
            elif line.startswith("author-time "):
                try:
                    ts = int(line[12:].strip())
                    blame["commit_timestamp"] = datetime.datetime.fromtimestamp(
                        ts, tz=datetime.timezone.utc
                    ).isoformat()
                except (ValueError, OSError):
                    blame["commit_timestamp"] = ""
        if blame:
            results.append(blame)
        return results
    except Exception:
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIDENCE SCORING
# ═══════════════════════════════════════════════════════════════════════════════

def compute_confidence(commit_timestamp: str, lineage_depth: int) -> float:
    """
    score = 1.0 - (days_old * 0.1) - (lineage_depth * 0.2)
    Clamped to [0.05, 1.0].
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    try:
        ts = datetime.datetime.fromisoformat(commit_timestamp)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=datetime.timezone.utc)
        days_old = max(0, (now - ts).days)
    except Exception:
        days_old = 7   # assume week-old if unparseable

    score = 1.0 - (days_old * 0.1) - (lineage_depth * 0.2)
    return round(max(0.05, min(1.0, score)), 4)


# ═══════════════════════════════════════════════════════════════════════════════
# BLAST RADIUS
# ═══════════════════════════════════════════════════════════════════════════════

def compute_blast_radius(contract: dict, records_failing: int) -> dict:
    """
    Extract blast radius from contract lineage.downstream.

    Returns:
        {
          "affected_nodes":     [...],
          "affected_pipelines": [...],
          "estimated_records":  N,
        }
    """
    lineage    = contract.get("lineage", {})
    downstream = lineage.get("downstream", [])

    affected_nodes:     list[str] = []
    affected_pipelines: list[str] = []

    for item in downstream:
        if isinstance(item, dict):
            node_id = item.get("id", "")
            desc    = item.get("description", "")
            if node_id:
                affected_nodes.append(node_id)
                affected_pipelines.append(node_id)
            elif desc:
                affected_pipelines.append(desc[:80])
        elif isinstance(item, str):
            affected_nodes.append(item)
            affected_pipelines.append(item)

    return {
        "affected_nodes":     affected_nodes,
        "affected_pipelines": affected_pipelines,
        "estimated_records":  records_failing,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# BLAME CHAIN BUILDER
# ═══════════════════════════════════════════════════════════════════════════════

def build_blame_chain(
    candidates: list[dict],
    repo_root:  str = ".",
) -> list[dict]:
    """
    For each candidate file, collect git log commits and git blame attribution.
    Score each commit. Return top 1–5 ranked by confidence score.

    Each entry:
        {
          rank, file_path, commit_hash, author,
          commit_timestamp, commit_message, confidence_score
        }
    """
    scored: list[dict] = []

    for cand in candidates:
        file_path = cand["path"]
        depth     = cand.get("depth", 1)

        commits = git_log_for_file(file_path, repo_root=repo_root)

        # Enrich with blame on first 20 lines if file exists locally
        blame_info = {}
        p = Path(repo_root) / file_path if not Path(file_path).is_absolute() else Path(file_path)
        if p.exists() and commits:
            blame_rows = git_blame_lines(file_path, 1, 20, repo_root=repo_root)
            if blame_rows:
                blame_info = blame_rows[0]  # use first non-trivial line's blame

        for commit in commits:
            ts    = commit.get("commit_timestamp", "")
            score = compute_confidence(ts, lineage_depth=depth)
            entry = {
                "file_path":        file_path,
                "commit_hash":      commit.get("commit_hash", ""),
                "author":           commit.get("author", blame_info.get("author", "unknown")),
                "commit_timestamp": ts or blame_info.get("commit_timestamp", ""),
                "commit_message":   commit.get("commit_message", ""),
                "confidence_score": score,
                "_depth":           depth,
            }
            scored.append(entry)

    # Deduplicate by commit_hash, keep highest score
    seen_hashes: dict[str, dict] = {}
    for entry in scored:
        h = entry["commit_hash"]
        if not h:
            continue
        if h not in seen_hashes or entry["confidence_score"] > seen_hashes[h]["confidence_score"]:
            seen_hashes[h] = entry

    ranked = sorted(seen_hashes.values(), key=lambda x: x["confidence_score"], reverse=True)

    # Enforce 1–5 entries
    ranked = ranked[:_MAX_BLAME]

    # Fallback: if still empty, synthesise one entry from HEAD
    if not ranked:
        ranked = [_fallback_blame_entry(repo_root)]

    # Add rank, strip internal field
    result = []
    for i, entry in enumerate(ranked, start=1):
        result.append({
            "rank":             i,
            "file_path":        entry["file_path"],
            "commit_hash":      entry["commit_hash"],
            "author":           entry["author"],
            "commit_timestamp": entry["commit_timestamp"],
            "commit_message":   entry["commit_message"],
            "confidence_score": entry["confidence_score"],
        })

    return result


def _fallback_blame_entry(repo_root: str) -> dict:
    """Return HEAD commit as a last-resort blame entry."""
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%H|%ae|%aI|%s"],
            capture_output=True, text=True, cwd=repo_root, timeout=5,
        )
        parts = result.stdout.strip().split("|", 3)
        if len(parts) >= 4:
            return {
                "file_path":        "unknown",
                "commit_hash":      parts[0],
                "author":           parts[1],
                "commit_timestamp": parts[2],
                "commit_message":   parts[3],
                "confidence_score": 0.10,
            }
    except Exception:
        pass
    return {
        "file_path":        "unknown",
        "commit_hash":      "unknown",
        "author":           "unknown",
        "commit_timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "commit_message":   "unknown",
        "confidence_score": 0.05,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ATTRIBUTOR
# ═══════════════════════════════════════════════════════════════════════════════

def attribute_report(
    report_path:   str,
    contract_path: str,
    lineage_path:  str,
    output_path:   str,
    repo_root:     str = ".",
) -> list[dict]:
    """
    Full Phase 2 attribution pipeline.

    Returns list of violation dicts written to output_path (JSONL).
    """
    # ── Load inputs ────────────────────────────────────────────────────────────
    print(f"[attributor] Loading report   : {report_path}")
    with open(report_path) as f:
        report = json.load(f)

    print(f"[attributor] Loading contract : {contract_path}")
    with open(contract_path) as f:
        contract = yaml.safe_load(f)

    source_type = contract.get("info", {}).get("sourceType", "")

    print(f"[attributor] Loading lineage  : {lineage_path}")
    lineage_record = load_lineage_snapshot(lineage_path)

    # ── Filter failing checks ─────────────────────────────────────────────────
    results   = report.get("results", [])
    failures  = [r for r in results if r.get("status") == "FAIL"]
    print(f"[attributor] {len(failures)} failing check(s) found")

    if not failures:
        print("[attributor] No violations to attribute.")
        return []

    # ── BFS lineage traversal ─────────────────────────────────────────────────
    bfs_result: list[dict] = []
    if lineage_record:
        bfs_result = bfs_upstream(lineage_record, start_system=source_type, max_depth=5)
        print(f"[attributor] BFS found {len(bfs_result)} lineage nodes")

    candidates = extract_file_candidates(bfs_result, repo_root=repo_root, start_system=source_type)

    # Also check recent git log broadly to find commits for the system
    broad_commits = git_log_recent_all(repo_root=repo_root)
    for commit in broad_commits:
        for changed_file in commit.get("changed_files", []):
            if source_type.lower() in changed_file.lower() or "inject" in changed_file.lower():
                if not any(c["path"] == changed_file for c in candidates):
                    candidates.append({
                        "path":   changed_file,
                        "depth":  2,
                        "source": "git_broad_search",
                    })

    print(f"[attributor] {len(candidates)} candidate file(s) for attribution")

    # ── Build blame chain ─────────────────────────────────────────────────────
    blame_chain = build_blame_chain(candidates, repo_root=repo_root)
    print(f"[attributor] Blame chain: {len(blame_chain)} candidate(s)")

    # ── Blast radius ──────────────────────────────────────────────────────────
    total_records_failing = sum(r.get("records_failing", 0) for r in failures)
    blast_radius = compute_blast_radius(contract, records_failing=total_records_failing)

    # ── Build violation entries ───────────────────────────────────────────────
    detected_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    violations: list[dict] = []

    for check_result in failures:
        violation: dict[str, Any] = {
            "violation_id":  str(uuid.uuid4()),
            "check_id":      check_result.get("check_id", ""),
            "column_name":   check_result.get("column_name", ""),
            "check_type":    check_result.get("check_type", ""),
            "detected_at":   detected_at,
            "contract_id":   report.get("contract_id", ""),
            "snapshot_id":   report.get("snapshot_id", ""),
            "severity":      check_result.get("severity", ""),
            "actual_value":  check_result.get("actual_value", ""),
            "expected":      check_result.get("expected", ""),
            "records_failing": check_result.get("records_failing", 0),
            "sample_failing":  check_result.get("sample_failing", []),
            "message":       check_result.get("message", ""),
            "blame_chain":   blame_chain,
            "blast_radius":  blast_radius,
        }
        # Include z_score if present
        if "z_score" in check_result:
            violation["z_score"] = check_result["z_score"]
        violations.append(violation)

    # ── Write JSONL ───────────────────────────────────────────────────────────
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    with out.open("a") as f:
        for v in violations:
            f.write(json.dumps(v) + "\n")

    print(f"[attributor] {len(violations)} violation(s) written to {output_path}")
    return violations


# ═══════════════════════════════════════════════════════════════════════════════
# BACKWARD-COMPAT: simple helper used by Phase 1 code (generator.py etc.)
# ═══════════════════════════════════════════════════════════════════════════════

def attribute_violations(
    violations:   list[dict],
    lineage_info: dict | None,
) -> list[dict]:
    """
    Phase 1 lightweight helper: enrich violation dicts with attribution
    metadata from the contract lineage block.

    Kept for backward compatibility with any code that imports this function.
    """
    attribution: dict[str, Any] = {}
    if lineage_info:
        upstream = lineage_info.get("upstream", [])
        source_pipeline: str | None = None
        if upstream:
            first = upstream[0]
            source_pipeline = (
                first.get("id") or first.get("description")
                if isinstance(first, dict) else str(first)
            )
        else:
            source_pipeline = lineage_info.get("source_file")

        attribution = {
            "snapshot_id":     lineage_info.get("snapshot_id"),
            "git_commit":      lineage_info.get("git_commit"),
            "git_author":      lineage_info.get("git_author"),
            "git_committed_at": lineage_info.get("git_committed_at"),
            "source_pipeline": source_pipeline,
        }

    return [{**v, "attribution": attribution} for v in violations]


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="attributor",
        description="Phase 2 — Attribute validation violations to source commits",
    )
    p.add_argument("--report",   required=True, help="Path to validation report JSON")
    p.add_argument("--contract", required=True, help="Path to Bitol YAML contract")
    p.add_argument("--lineage",  required=True, help="Path to lineage JSONL snapshots")
    p.add_argument("--output",   default="violation_log/violations.jsonl",
                   help="Path to append JSONL violation log (default: violation_log/violations.jsonl)")
    p.add_argument("--repo",     default=".",
                   help="Repo root for git commands (default: current directory)")
    return p


def main() -> None:
    args = build_parser().parse_args()
    violations = attribute_report(
        report_path   = args.report,
        contract_path = args.contract,
        lineage_path  = args.lineage,
        output_path   = args.output,
        repo_root     = args.repo,
    )
    if violations:
        print(f"[attributor] ❌ {len(violations)} violation(s) attributed")
        for v in violations:
            top = v["blame_chain"][0] if v["blame_chain"] else {}
            print(
                f"  • {v['check_id']}"
                f" → {top.get('file_path','?')}"
                f" by {top.get('author','?')}"
                f" (score={top.get('confidence_score','?')})"
            )
    else:
        print("[attributor] ✓  No violations to attribute")


if __name__ == "__main__":
    main()
