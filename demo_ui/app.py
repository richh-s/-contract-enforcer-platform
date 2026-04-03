"""
Data Contract Enforcer — Demo UI Backend
Run from project root: uvicorn demo_ui.app:app --reload --port 3000
"""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import AsyncGenerator

import yaml
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
STATIC_DIR = Path(__file__).parent / "static"

# ---------------------------------------------------------------------------
# Step configuration
# ---------------------------------------------------------------------------
STEPS: dict[int, dict] = {
    1: {
        "name": "Contract Generation",
        "cmd": [
            sys.executable, "contracts/generator.py",
            "--source", "outputs/week3/extractions.jsonl",
            "--contract-id", "week3-document-refinery-extractions",
            "--lineage", "outputs/week4/lineage_snapshots.jsonl",
            "--output", "generated_contracts/",
        ],
        "output_file": "generated_contracts/week3_document_refinery_extractions.yaml",
        "output_format": "yaml",
    },
    2: {
        "name": "Violation Detection",
        "cmd": [
            sys.executable, "contracts/runner.py",
            "--contract", "generated_contracts/week3_document_refinery_extractions.yaml",
            "--data", "outputs/week3/extractions_violated.jsonl",
            "--output", "validation_reports/week3_violated.json",
            "--mode", "ENFORCE",
        ],
        "output_file": "validation_reports/week3_violated.json",
        "output_format": "json",
    },
    3: {
        "name": "Blame Chain",
        "cmd": [
            sys.executable, "contracts/attributor.py",
            "--report", "validation_reports/week3_violated.json",
            "--contract", "generated_contracts/week3_document_refinery_extractions.yaml",
            "--lineage", "outputs/week4/lineage_snapshots.jsonl",
            "--output", "validation_reports/attribution_demo.jsonl",
        ],
        "output_file": "validation_reports/attribution_demo.jsonl",
        "output_format": "jsonl",
    },
    4: {
        "name": "Schema Evolution",
        "cmd": [
            sys.executable, "contracts/schema_analyzer.py",
            "--contract-id", "week3-extractions",
            "--output", "validation_reports/schema_evolution_demo.json",
        ],
        "output_file": "validation_reports/schema_evolution_demo.json",
        "output_format": "json",
    },
    5: {
        "name": "AI Extensions",
        "cmd": [
            sys.executable, "contracts/ai_extensions.py",
            "--mode", "all",
            "--extractions", "outputs/week3/extractions.jsonl",
            "--verdicts", "outputs/week2/verdicts.jsonl",
            "--output", "validation_reports/ai_extensions_demo.json",
        ],
        "output_file": "validation_reports/ai_extensions_demo.json",
        "output_format": "json",
    },
    6: {
        "name": "Enforcer Report",
        "cmd": [
            sys.executable, "contracts/report_generator.py",
            "--output", "enforcer_report/report_data.json",
        ],
        "output_file": "enforcer_report/report_data.json",
        "output_format": "json",
    },
}

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title="Data Contract Enforcer Demo")


# ---------------------------------------------------------------------------
# SSE streaming runner
# ---------------------------------------------------------------------------
async def _stream_command(cmd: list[str]) -> AsyncGenerator[str, None]:
    """Run a subprocess and yield SSE-formatted lines."""
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd=str(PROJECT_ROOT),
    )

    assert process.stdout is not None
    while True:
        line = await process.stdout.readline()
        if not line:
            break
        text = line.decode("utf-8", errors="replace").rstrip("\n")
        yield f"data: {text}\n\n"

    await process.wait()
    exit_code = process.returncode
    yield f"data: __DONE__:{exit_code}\n\n"


@app.get("/run/{step}")
async def run_step(step: int):
    if step not in STEPS:
        raise HTTPException(status_code=404, detail=f"Step {step} not found")
    cfg = STEPS[step]
    return StreamingResponse(
        _stream_command(cfg["cmd"]),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ---------------------------------------------------------------------------
# Result parser
# ---------------------------------------------------------------------------
def _parse_output(step: int) -> dict:
    cfg = STEPS[step]
    output_path = PROJECT_ROOT / cfg["output_file"]
    fmt = cfg["output_format"]

    if not output_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Output file not found: {cfg['output_file']}",
        )

    raw = output_path.read_text(encoding="utf-8")

    if fmt == "yaml":
        data = yaml.safe_load(raw)
        return {"raw": raw, "parsed": data}

    if fmt == "json":
        data = json.loads(raw)
        return data

    if fmt == "jsonl":
        records = []
        for line in raw.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                pass
        return {"records": records}

    raise HTTPException(status_code=500, detail=f"Unknown format: {fmt}")


@app.get("/result/{step}")
async def get_result(step: int):
    if step not in STEPS:
        raise HTTPException(status_code=404, detail=f"Step {step} not found")
    return JSONResponse(_parse_output(step))


# ---------------------------------------------------------------------------
# Static files — mount last so API routes take priority
# ---------------------------------------------------------------------------
app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
