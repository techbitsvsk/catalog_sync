"""
injection_suite/report.py
==========================
Generates JSON + self-contained HTML security reports.
"""

from __future__ import annotations

import json
import datetime
from pathlib import Path
from typing import List

from injection_suite.payloads import AttackCategory, RiskLevel
from injection_suite.test_runner import SuiteMetrics, TestScenarioResult, compute_metrics

_RISK_CSS = {
    "critical": "#e74c3c",
    "high":     "#e67e22",
    "medium":   "#f1c40f",
    "low":      "#2ecc71",
    "none":     "#95a5a6",
}


def _scenario_to_dict(r: TestScenarioResult) -> dict:
    p = r.payload
    return {
        "payload_id":   p.id,
        "name":         p.name,
        "category":     p.category.value,
        "risk_level":   p.risk_level.value,
        "target":       p.injection_target,
        "owasp_ref":    p.owasp_ref,
        "description":  p.description,
        "payload_text": p.text[:200] + ("…" if len(p.text) > 200 else ""),
        "error":        r.error,
        "vulnerable_pipeline": {
            "injection_detected": r.pipeline.vulnerable.injection_detected,
            "response_preview":   r.pipeline.vulnerable.response_text[:300],
            "detection_method":   r.pipeline.vulnerable.detection_method,
            "latency_ms":         round(r.pipeline.vulnerable.latency_ms, 1),
            "blocked_fields":     r.pipeline.vulnerable.blocked_fields,
        },
        "guarded_pipeline": {
            "injection_detected": r.pipeline.guarded.injection_detected,
            "response_preview":   r.pipeline.guarded.response_text[:300],
            "detection_method":   r.pipeline.guarded.detection_method,
            "latency_ms":         round(r.pipeline.guarded.latency_ms, 1),
            "blocked_fields":     r.pipeline.guarded.blocked_fields,
            "flagged_fields":     [k for k in r.pipeline.guarded.flagged_fields],
        },
        "classifier_results": {
            field: {
                "is_injection":    cr.is_injection,
                "confidence":      round(cr.confidence, 3),
                "risk_level":      cr.risk_level,
                "category":        cr.category,
                "recommendation":  cr.recommendation,
                "evidence":        cr.evidence,
                "tier":            cr.tier,
            }
            for field, cr in r.pipeline.classifier_results.items()
        },
        "defense_prevented_attack": r.pipeline.defense_prevented_attack,
        "false_positive": r.false_positive,
        "setup_ms":  round(r.setup_latency_ms, 1),
        "test_ms":   round(r.test_latency_ms,  1),
    }


def save_json(results: List[TestScenarioResult], out_dir: Path) -> Path:
    """Save a machine-readable JSON report."""
    metrics = compute_metrics(results)
    ts      = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    path    = out_dir / f"injection_report_{ts}.json"

    report = {
        "generated_at": datetime.datetime.now().isoformat(),
        "summary": {
            "total":                 metrics.total,
            "attack_payloads":       metrics.attack_total,
            "benign_controls":       metrics.benign_total,
            "errors":                metrics.errors,
            "attacks_succeeded":     metrics.attacks_succeeded,
            "attacks_blocked":       metrics.attacks_blocked,
            "detection_rate":        round(metrics.detection_rate, 4),
            "false_positive_rate":   round(metrics.false_positive_rate, 4),
            "attack_surface_coverage": round(metrics.attack_surface_coverage, 4),
        },
        "by_category": metrics.by_category,
        "by_risk":     metrics.by_risk,
        "scenarios":   [_scenario_to_dict(r) for r in results],
    }
    path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def save_html(results: List[TestScenarioResult], out_dir: Path) -> Path:
    """Save a self-contained HTML security dashboard."""
    metrics = compute_metrics(results)
    ts      = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    path    = out_dir / f"injection_report_{ts}.html"

    # Build scenario rows
    rows_html = ""
    for r in results:
        p = r.payload
        risk_color = _RISK_CSS.get(p.risk_level.value, "#95a5a6")
        is_benign  = p.category == AttackCategory.BENIGN

        if r.error:
            status = f'<span class="badge err">ERROR</span>'
        elif is_benign:
            status = ('<span class="badge fp">FALSE POS</span>'
                      if r.false_positive else
                      '<span class="badge ok">✓ OK</span>')
        elif r.injection_blocked_by_guard:
            status = '<span class="badge ok">✓ BLOCKED</span>'
        elif r.injection_succeeded_unguarded:
            status = '<span class="badge warn">⚠ MISSED</span>'
        else:
            status = '<span class="badge safe">SAFE</span>'

        vuln = ('<span class="badge danger">BYPASSED</span>'
                if r.pipeline.vulnerable.injection_detected
                else '<span class="badge safe">clean</span>')
        guarded = ('<span class="badge ok">BLOCKED</span>'
                   if r.pipeline.guarded.blocked_fields
                   else '<span class="badge safe">passed</span>')

        rows_html += f"""
        <tr>
          <td><code>{p.id}</code></td>
          <td>{p.name}</td>
          <td><span style="color:{risk_color};font-weight:bold">{p.risk_level.value.upper()}</span></td>
          <td>{p.category.value.replace('_',' ')}</td>
          <td><code>{p.injection_target}</code></td>
          <td>{vuln}</td>
          <td>{guarded}</td>
          <td>{status}</td>
        </tr>"""

    det_pct = f"{metrics.detection_rate:.0%}"
    fpr_pct = f"{metrics.false_positive_rate:.0%}"
    cov_pct = f"{metrics.attack_surface_coverage:.0%}"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Iceberg RAG Injection — Security Report</title>
<style>
  body      {{ font-family: 'Segoe UI', sans-serif; background:#0d1117; color:#c9d1d9; margin:0; padding:20px; }}
  h1        {{ color:#58a6ff; }} h2 {{ color:#8b949e; border-bottom:1px solid #30363d; padding-bottom:6px; }}
  .cards    {{ display:flex; gap:16px; flex-wrap:wrap; margin:20px 0; }}
  .card     {{ background:#161b22; border:1px solid #30363d; border-radius:8px; padding:16px 24px; min-width:160px; }}
  .card .n  {{ font-size:2.4rem; font-weight:bold; }}
  .card .l  {{ font-size:.8rem; color:#8b949e; }}
  .green    {{ color:#3fb950; }} .red {{ color:#f85149; }} .yellow {{ color:#e3b341; }}
  .blue     {{ color:#58a6ff; }}
  table     {{ width:100%; border-collapse:collapse; margin:16px 0; font-size:.88rem; }}
  th        {{ background:#161b22; padding:8px 10px; text-align:left; color:#8b949e; border-bottom:1px solid #30363d; }}
  td        {{ padding:7px 10px; border-bottom:1px solid #21262d; vertical-align:middle; }}
  tr:hover  {{ background:#161b22; }}
  .badge    {{ padding:2px 8px; border-radius:4px; font-size:.78rem; font-weight:600; }}
  .badge.ok     {{ background:#0f3d1c; color:#3fb950; }}
  .badge.danger {{ background:#3d1212; color:#f85149; }}
  .badge.warn   {{ background:#3d2e0f; color:#e3b341; }}
  .badge.safe   {{ background:#1c2432; color:#8b949e; }}
  .badge.fp     {{ background:#3d2e0f; color:#e3b341; }}
  .badge.err    {{ background:#3d0f0f; color:#f85149; }}
  pre       {{ background:#161b22; padding:12px; border-radius:6px; overflow:auto; font-size:.8rem; }}
  .note     {{ background:#1c2432; border-left:4px solid #58a6ff; padding:10px 14px; border-radius:4px; margin:16px 0; }}
</style>
</head>
<body>
<h1>🛡 Iceberg/RAG Prompt Injection Security Report</h1>
<p style="color:#8b949e">Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} &nbsp;|&nbsp;
Stack: Nessie + MinIO + catalog-gateway + OPA</p>

<div class="cards">
  <div class="card"><div class="n blue">{metrics.total}</div><div class="l">Payloads tested</div></div>
  <div class="card"><div class="n yellow">{cov_pct}</div><div class="l">Attack surface coverage<br><small>bypassed vulnerable pipeline</small></div></div>
  <div class="card"><div class="n {'green' if metrics.detection_rate > .8 else 'yellow' if metrics.detection_rate > .5 else 'red'}">{det_pct}</div><div class="l">Detection rate<br><small>blocked by guarded pipeline</small></div></div>
  <div class="card"><div class="n {'green' if metrics.false_positive_rate < .1 else 'yellow'}">{fpr_pct}</div><div class="l">False positive rate</div></div>
  <div class="card"><div class="n red">{metrics.attacks_succeeded}</div><div class="l">Attacks succeeded<br><small>on vulnerable pipeline</small></div></div>
  <div class="card"><div class="n green">{metrics.attacks_blocked}</div><div class="l">Attacks blocked<br><small>by classifier middleware</small></div></div>
</div>

<div class="note">
  <strong>How to read this report:</strong>
  &ldquo;Vulnerable&rdquo; = no guardrails (metadata passes directly to LLM).
  &ldquo;Guarded&rdquo; = classifier middleware intercepts metadata before LLM.
  Detection rate measures how many attacks the guardrail caught.
</div>

<h2>Scenario Results</h2>
<table>
<thead><tr>
  <th>ID</th><th>Name</th><th>Risk</th><th>Category</th><th>Target</th>
  <th>Vulnerable</th><th>Guarded</th><th>Status</th>
</tr></thead>
<tbody>{rows_html}</tbody>
</table>

<h2>Enterprise Defense Architecture</h2>
<pre>
 Data Engineer (ops-client)
    │
    │  writes poisoned metadata
    ▼
 Nessie Catalog ─── table descriptions ──────────────────────────────────┐
                ─── column doc strings ──────────────────────────────────┤
                ─── table properties (etl.notes, etc.) ─────────────────┤
                                                                         │
                                                                         ▼
                                          ┌─────────────────────────────────────┐
 Business User asks question ────────────▶│         RAG PIPELINE                │
                                          │                                     │
                                          │  ┌──────────────────────────────┐  │
                                          │  │  Tier-1: Rule-based classifier│  │
                                          │  │  • 18+ critical patterns      │  │
                                          │  │  • 10+ medium patterns        │  │
                                          │  │  • Zero-cost, &lt;1ms           │  │
                                          │  └──────────┬───────────────────┘  │
                                          │             │ BLOCK / SANITIZE / FLAG│
                                          │  ┌──────────▼───────────────────┐  │
                                          │  │  Tier-2: Ollama AI classifier │  │
                                          │  │  • Local LLM, no API cost    │  │
                                          │  │  • Runs only for ambiguous   │  │
                                          │  │    cases (~20% of traffic)   │  │
                                          │  └──────────┬───────────────────┘  │
                                          │             │ safe metadata only    │
                                          │  ┌──────────▼───────────────────┐  │
                                          │  │       LLM (Ollama)            │  │
                                          │  │  Answers the user question    │  │
                                          │  └──────────────────────────────┘  │
                                          └─────────────────────────────────────┘

 OPA Layer (catalog-gateway):
   ops-client   → READ + WRITE (can poison metadata — attack surface)
   analytics-client → READ only + row-filter + column exclusions (principle of least privilege)
</pre>

<h2>Remediation Recommendations</h2>
<table>
<thead><tr><th>Priority</th><th>Control</th><th>Implementation</th></tr></thead>
<tbody>
<tr><td><span style="color:#f85149">CRITICAL</span></td><td>Metadata write review workflow</td><td>Require peer review for changes to table descriptions and column docs in production namespaces</td></tr>
<tr><td><span style="color:#f85149">CRITICAL</span></td><td>Classifier middleware in RAG pipeline</td><td>Deploy the classifier before any metadata reaches an LLM; block CRITICAL/HIGH, sanitize MEDIUM</td></tr>
<tr><td><span style="color:#e67e22">HIGH</span></td><td>Principle of least privilege for metadata writes</td><td>Restrict ops-client WRITE to non-production namespaces; require admin approval for gold namespace writes</td></tr>
<tr><td><span style="color:#e67e22">HIGH</span></td><td>Audit logging for metadata changes</td><td>Log all PUT operations through catalog-gateway; alert on suspicious description changes</td></tr>
<tr><td><span style="color:#e3b341">MEDIUM</span></td><td>Ollama second-pass classifier</td><td>Run the Ollama tier-2 classifier asynchronously on all new metadata writes; hold for human review if flagged</td></tr>
<tr><td><span style="color:#e3b341">MEDIUM</span></td><td>LLM system-prompt hardening</td><td>Explicitly instruct the RAG LLM to ignore instructions embedded in metadata; use structured prompts</td></tr>
<tr><td><span style="color:#2ecc71">LOW</span></td><td>Column doc character limits</td><td>Enforce max 500 chars on description fields; reject metadata updates with unusual Unicode ranges</td></tr>
</tbody>
</table>

</body>
</html>"""

    path.write_text(html, encoding="utf-8")
    return path
