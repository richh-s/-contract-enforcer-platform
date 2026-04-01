"""
contracts/ai_extensions.py
Phase 1 — AI-powered contract extensions.

Provides higher-level wrappers around the LLM annotation layer,
including clause explanation, anomaly narrative generation, and
natural-language contract summaries for human review.

Usage:
    from contracts.ai_extensions import explain_violations, summarize_contract

    narrative = explain_violations(violations, contract)
    summary   = summarize_contract(contract)
"""

from __future__ import annotations

import os


# ── Public API ─────────────────────────────────────────────────────────────────

def explain_violations(
    violations: list[dict],
    contract: dict,
) -> str:
    """
    Generate a human-readable narrative explaining why each validation
    violation matters in the context of the contract.

    Requires ANTHROPIC_API_KEY or OPENAI_API_KEY to be set.
    Returns a plain-text explanation, or a stub message when no key is present.

    Parameters
    ----------
    violations : list[dict]
        CheckResult dicts (status != PASS) from contracts/runner.py.
    contract : dict
        The full Bitol contract dict (from generator.py).

    Returns
    -------
    str
    """
    if not _has_api_key():
        return _stub_explanation(violations)

    # Full LLM path: build prompt and call API.
    # Deferred to runtime to avoid hard dependency on anthropic/openai SDK.
    return _llm_explain(violations, contract)


def summarize_contract(contract: dict) -> str:
    """
    Return a 2–4 sentence natural-language summary of the contract suitable
    for inclusion in a PR description or Slack notification.

    Parameters
    ----------
    contract : dict
        The full Bitol contract dict.

    Returns
    -------
    str
    """
    if not _has_api_key():
        return _stub_summary(contract)

    return _llm_summarize(contract)


# ── Internals ──────────────────────────────────────────────────────────────────

def _has_api_key() -> bool:
    return bool(os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("OPENAI_API_KEY"))


def _stub_explanation(violations: list[dict]) -> str:
    if not violations:
        return "No violations to explain."
    lines = [
        "AI explanation unavailable (no API key). Violation summary:",
        "",
    ]
    for v in violations:
        severity = v.get("severity", "UNKNOWN")
        rule     = v.get("rule", v.get("check_id", "unknown"))
        field    = v.get("field", "—")
        msg      = v.get("message", "")
        lines.append(f"  [{severity}] {rule} on field '{field}': {msg}")
    return "\n".join(lines)


def _stub_summary(contract: dict) -> str:
    info       = contract.get("info", {})
    title      = info.get("title", "Unnamed contract")
    source     = info.get("sourceFile", "unknown source")
    desc       = info.get("description", "")
    quality    = contract.get("quality", {})
    n_struct   = len(quality.get("structural", []))
    n_stat     = len(quality.get("statistical", []))
    n_cross    = len(quality.get("crossField", []))
    return (
        f"{title} governs data from {source}. "
        f"{desc[:120] + '...' if len(desc) > 120 else desc} "
        f"It enforces {n_struct} structural, {n_stat} statistical, "
        f"and {n_cross} cross-field quality checks."
    ).strip()


def _llm_explain(violations: list[dict], contract: dict) -> str:
    """Call LLM to explain violations. Lazy import to avoid hard SDK dep."""
    try:
        import anthropic  # type: ignore
        client = anthropic.Anthropic()
        prompt = _build_explain_prompt(violations, contract)
        message = client.messages.create(
            model="claude-3-5-haiku-20241022",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text
    except Exception as exc:
        return f"LLM explanation failed: {exc}\n\n{_stub_explanation(violations)}"


def _llm_summarize(contract: dict) -> str:
    """Call LLM to summarize the contract. Lazy import to avoid hard SDK dep."""
    try:
        import anthropic  # type: ignore
        client = anthropic.Anthropic()
        prompt = _build_summary_prompt(contract)
        message = client.messages.create(
            model="claude-3-5-haiku-20241022",
            max_tokens=256,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text
    except Exception as exc:
        return f"LLM summary failed: {exc}\n\n{_stub_summary(contract)}"


def _build_explain_prompt(violations: list[dict], contract: dict) -> str:
    info  = contract.get("info", {})
    title = info.get("title", "this data contract")
    vlist = "\n".join(
        f"- [{v.get('severity')}] rule={v.get('rule', v.get('check_id'))} "
        f"field={v.get('field')} msg={v.get('message')}"
        for v in violations[:10]
    )
    return (
        f"You are a data quality engineer. The following violations were found "
        f"when validating data against '{title}':\n\n{vlist}\n\n"
        f"Explain in plain English why each violation matters and what the likely "
        f"root cause is. Be concise (2–3 sentences per violation)."
    )


def _build_summary_prompt(contract: dict) -> str:
    info  = contract.get("info", {})
    title = info.get("title", "this contract")
    desc  = info.get("description", "")
    return (
        f"Summarize the following data contract in 2–4 sentences for a "
        f"non-technical stakeholder. Contract title: '{title}'. "
        f"Description: {desc}"
    )
