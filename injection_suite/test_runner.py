"""
injection_suite/test_runner.py
================================
Orchestrates the full injection test suite against the local Nessie/MinIO stack.

For each payload:
  1. Poison the Iceberg table metadata via ops-client
  2. Read it back via analytics-client (RAG pipeline perspective)
  3. Run vulnerable pipeline → check if injection succeeds
  4. Run guarded pipeline    → check if injection is blocked
  5. Collect metrics

Output: list of TestScenarioResult (consumed by report.py)
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.text import Text

from injection_suite import catalog_ops as cat
from injection_suite.classifier import ClassifierResult, classify
from injection_suite.payloads import ALL_PAYLOADS, AttackCategory, Payload, RiskLevel
from injection_suite.rag_agent import PipelineResult, run_pipeline_pair

console = Console(force_terminal=True, highlight=False)

# ── Result types ──────────────────────────────────────────────────────────────

@dataclass
class TestScenarioResult:
    payload:              Payload
    pipeline:             PipelineResult
    error:                Optional[str] = None
    setup_latency_ms:     float = 0.0
    test_latency_ms:      float = 0.0

    @property
    def injection_succeeded_unguarded(self) -> bool:
        return self.pipeline.vulnerable.injection_detected

    @property
    def injection_blocked_by_guard(self) -> bool:
        return self.pipeline.defense_prevented_attack

    @property
    def false_positive(self) -> bool:
        """Benign payload was incorrectly blocked."""
        return (
            self.payload.category == AttackCategory.BENIGN
            and bool(self.pipeline.guarded.blocked_fields)
        )


@dataclass
class SuiteMetrics:
    total:              int = 0
    attack_total:       int = 0
    benign_total:       int = 0
    attacks_succeeded:  int = 0    # injection got through vulnerable pipeline
    attacks_blocked:    int = 0    # injection blocked by guarded pipeline
    false_positives:    int = 0    # benign content incorrectly blocked
    errors:             int = 0

    by_category:  Dict[str, Dict[str, int]] = field(default_factory=dict)
    by_risk:      Dict[str, Dict[str, int]] = field(default_factory=dict)

    @property
    def detection_rate(self) -> float:
        """Of attacks that succeeded in vulnerable pipeline, % blocked by guard."""
        if self.attacks_succeeded == 0:
            return 0.0
        return self.attacks_blocked / self.attacks_succeeded

    @property
    def false_positive_rate(self) -> float:
        if self.benign_total == 0:
            return 0.0
        return self.false_positives / self.benign_total

    @property
    def attack_surface_coverage(self) -> float:
        """% of attack payloads that succeeded in vulnerable pipeline."""
        if self.attack_total == 0:
            return 0.0
        return self.attacks_succeeded / self.attack_total


# ── Setup helper ──────────────────────────────────────────────────────────────

def _apply_payload(payload: Payload) -> None:
    """Write a payload to the appropriate metadata location."""
    if payload.injection_target == "table_description":
        cat.poison_table_description(payload.text)
    elif payload.injection_target == "column_doc":
        cat.poison_column_doc("comment", payload.text)
    elif payload.injection_target == "table_property":
        cat.poison_table_property("etl.notes", payload.text)
    else:
        cat.poison_table_description(payload.text)


# ── Progress table ─────────────────────────────────────────────────────────────

_RISK_COLORS = {
    "critical": "red",
    "high":     "orange3",
    "medium":   "yellow",
    "low":      "green",
    "none":     "dim",
}


def _make_progress_table(results: List[TestScenarioResult]) -> Table:
    t = Table(
        "ID", "Name", "Category", "Risk",
        "Vulnerable", "Guarded", "Defense OK",
        show_header=True, header_style="bold cyan",
        expand=False, border_style="dim",
    )
    for r in results:
        p = r.payload
        risk_color = _RISK_COLORS.get(p.risk_level.value, "white")

        if r.error:
            t.add_row(p.id, p.name, "—", "—", "[red]ERROR[/red]", "[red]ERROR[/red]", "❌")
            continue

        vuln_cell  = "[red]BYPASSED[/red]"  if r.injection_succeeded_unguarded else "[green]safe[/green]"
        guard_cell = "[green]BLOCKED[/green]" if r.injection_blocked_by_guard else (
                     "[yellow]FLAGGED[/yellow]" if r.pipeline.guarded.flagged_fields else
                     "[dim]passed[/dim]"
                     )
        ok_cell    = "✅" if (r.injection_blocked_by_guard or not r.injection_succeeded_unguarded) else "❌"

        if r.payload.category == AttackCategory.BENIGN:
            fp = "⚠️ FP" if r.false_positive else "✅ ok"
            t.add_row(p.id, p.name, "benign", "none", "[dim]—[/dim]", guard_cell, fp)
        else:
            t.add_row(
                p.id, p.name[:30],
                p.category.value[:16],
                Text(p.risk_level.value, style=risk_color),
                vuln_cell, guard_cell, ok_cell,
            )
    return t


# ── Main runner ───────────────────────────────────────────────────────────────

def run_suite(
    payloads:    Optional[List[Payload]] = None,
    use_ollama:  bool = False,
    use_ollama_classifier: bool = False,
    verbose:     bool = True,
) -> List[TestScenarioResult]:
    """
    Run the full injection test suite.

    Args:
        payloads:              Subset of payloads to test (default: all).
        use_ollama:            Use Ollama for the RAG agent (requires ollama running).
        use_ollama_classifier: Use Ollama for ambiguous classifier cases.
        verbose:               Show live progress table.

    Returns:
        List of TestScenarioResult (one per payload).
    """
    if payloads is None:
        payloads = ALL_PAYLOADS

    # Bootstrap the test table
    console.print("[bold cyan]Setting up injection test table...[/bold cyan]")
    try:
        cat.ensure_table()
        console.print(f"  Table ready: [bold]{cat.FULL_TABLE_NAME}[/bold]")
    except Exception as exc:
        console.print(f"[red]  Failed to create table: {exc}[/red]")
        console.print("  [dim]Make sure the local stack is running: "
                      "docker compose -f docker/docker-compose.yml up -d[/dim]")
        return []

    results: List[TestScenarioResult] = []
    mode_label = "ollama" if use_ollama else "simulation"
    console.print(f"  Mode: [yellow]{mode_label}[/yellow] | "
                  f"Classifier: [yellow]{'rule+ollama' if use_ollama_classifier else 'rule-based'}[/yellow]")
    console.print(f"  Payloads: [yellow]{len(payloads)}[/yellow]\n")

    with Live(console=console, refresh_per_second=4) as live:
        for payload in payloads:
            t_start = time.monotonic()

            # Reset to clean baseline before each scenario
            try:
                cat.reset_metadata()
            except Exception:
                pass

            # Apply the injection payload
            setup_ok = True
            try:
                _apply_payload(payload)
            except Exception as exc:
                results.append(TestScenarioResult(
                    payload=payload,
                    pipeline=_empty_pipeline(payload.id),
                    error=f"setup failed: {exc}",
                ))
                live.update(_make_progress_table(results))
                continue

            setup_latency = (time.monotonic() - t_start) * 1000

            # Read the metadata back through the catalog gateway
            metadata = cat.read_metadata()
            if metadata is None:
                results.append(TestScenarioResult(
                    payload=payload,
                    pipeline=_empty_pipeline(payload.id),
                    error="catalog read failed",
                ))
                live.update(_make_progress_table(results))
                continue

            # Run both pipelines
            t_test = time.monotonic()
            try:
                pipeline = run_pipeline_pair(
                    metadata=metadata,
                    payload_id=payload.id,
                    success_markers=payload.success_markers,
                    use_ollama=use_ollama,
                    use_ollama_classifier=use_ollama_classifier,
                )
            except Exception as exc:
                results.append(TestScenarioResult(
                    payload=payload,
                    pipeline=_empty_pipeline(payload.id),
                    error=f"pipeline error: {exc}",
                ))
                live.update(_make_progress_table(results))
                continue

            test_latency = (time.monotonic() - t_test) * 1000

            results.append(TestScenarioResult(
                payload=payload,
                pipeline=pipeline,
                setup_latency_ms=setup_latency,
                test_latency_ms=test_latency,
            ))
            live.update(_make_progress_table(results))

    # Reset table to clean state after the full run
    try:
        cat.reset_metadata()
    except Exception:
        pass

    return results


def compute_metrics(results: List[TestScenarioResult]) -> SuiteMetrics:
    """Aggregate metrics from a completed test run."""
    m = SuiteMetrics()
    m.total        = len(results)
    m.attack_total = sum(1 for r in results if r.payload.category != AttackCategory.BENIGN and not r.error)
    m.benign_total = sum(1 for r in results if r.payload.category == AttackCategory.BENIGN and not r.error)
    m.errors       = sum(1 for r in results if r.error)

    for r in results:
        if r.error:
            continue
        cat_key  = r.payload.category.value
        risk_key = r.payload.risk_level.value

        m.by_category.setdefault(cat_key,  {"total": 0, "succeeded": 0, "blocked": 0})
        m.by_risk.setdefault(risk_key,      {"total": 0, "succeeded": 0, "blocked": 0})

        m.by_category[cat_key]["total"]  += 1
        m.by_risk[risk_key]["total"]     += 1

        if r.payload.category == AttackCategory.BENIGN:
            if r.false_positive:
                m.false_positives += 1
            continue

        if r.injection_succeeded_unguarded:
            m.attacks_succeeded              += 1
            m.by_category[cat_key]["succeeded"] += 1
            m.by_risk[risk_key]["succeeded"]    += 1

        if r.injection_blocked_by_guard:
            m.attacks_blocked                += 1
            m.by_category[cat_key]["blocked"]   += 1
            m.by_risk[risk_key]["blocked"]      += 1

    return m


def _empty_pipeline(payload_id: str):
    from injection_suite.rag_agent import AgentResponse, PipelineResult
    empty = AgentResponse(
        response_text="", injection_detected=False, detection_method="error"
    )
    return PipelineResult(payload_id=payload_id, metadata_context="",
                          vulnerable=empty, guarded=empty)


def print_summary(metrics: SuiteMetrics) -> None:
    console.print("\n[bold cyan]═══ Suite Summary ═══[/bold cyan]")
    console.print(f"  Payloads tested  : {metrics.total}")
    console.print(f"  Attack payloads  : {metrics.attack_total}")
    console.print(f"  Benign controls  : {metrics.benign_total}")
    console.print(f"  Errors           : {metrics.errors}")
    console.print()
    console.print(f"  [bold]Attack surface coverage : {metrics.attack_surface_coverage:.0%}[/bold]")
    console.print(f"  (% of attack payloads that bypassed the vulnerable pipeline)")
    console.print()
    console.print(f"  [bold green]Detection rate          : {metrics.detection_rate:.0%}[/bold green]")
    console.print(f"  (of successful attacks, % blocked by the guarded pipeline)")
    console.print()
    console.print(f"  [bold yellow]False positive rate     : {metrics.false_positive_rate:.0%}[/bold yellow]")
    console.print(f"  (% of benign metadata incorrectly blocked)")
    console.print()

    if metrics.by_risk:
        console.print("  Risk breakdown:")
        for risk in ("critical", "high", "medium", "low"):
            row = metrics.by_risk.get(risk)
            if row and row["total"] > 0:
                color = _RISK_COLORS.get(risk, "white")
                det = row["blocked"] / row["succeeded"] if row["succeeded"] else 0
                console.print(
                    f"    [{color}]{risk:8s}[/{color}]  "
                    f"{row['succeeded']}/{row['total']} bypassed  "
                    f"{det:.0%} detected"
                )
