#!/usr/bin/env python3
"""
run_injection_tests.py
========================
CLI entry point for the Iceberg/RAG Prompt Injection Test Suite.

Usage examples:
  # Offline mode — no LLM, pure rule-based classifier (fastest, free)
  python run_injection_tests.py

  # With Ollama — real LLM calls for the RAG agent
  python run_injection_tests.py --use-ollama

  # With Ollama for both RAG and classifier (full AI mode)
  python run_injection_tests.py --use-ollama --ollama-classifier

  # Run only a specific risk level
  python run_injection_tests.py --risk critical high

  # Run only specific categories
  python run_injection_tests.py --category direct_override jailbreak

  # Custom Ollama model
  OLLAMA_MODEL=mistral python run_injection_tests.py --use-ollama

Ollama setup (one-time):
  1. Download: https://ollama.com/download
  2. Pull a model: ollama pull llama3.1
  3. Run: ollama serve  (or it starts automatically on Windows)
"""

import sys
import os

# Make sure the project root is on the path
sys.path.insert(0, os.path.dirname(__file__))

import click
from pathlib import Path
from rich.console import Console
from rich.panel import Panel

from injection_suite.classifier import is_ollama_available, list_ollama_models
from injection_suite.payloads import (
    ALL_PAYLOADS, AttackCategory, RiskLevel,
    ATTACK_PAYLOADS, BENIGN_PAYLOADS, summary as payload_summary,
)
from injection_suite.report import save_html, save_json
from injection_suite.test_runner import compute_metrics, print_summary, run_suite

console = Console(force_terminal=True, highlight=False)

REPORTS_DIR = Path(__file__).parent / "reports"


def _check_ollama(use_ollama: bool) -> bool:
    """Returns True if Ollama is available when requested."""
    if not use_ollama:
        return False
    if is_ollama_available():
        models = list_ollama_models()
        console.print(f"  [green]✓ Ollama available[/green] — models: {models or ['(none pulled yet)']}")
        if not models:
            console.print("  [yellow]  Run: ollama pull llama3.1[/yellow]")
            return False
        return True
    else:
        console.print(Panel(
            "[yellow]Ollama is not running.[/yellow]\n\n"
            "To install and start:\n"
            "  1. Download from [bold]https://ollama.com/download[/bold]\n"
            "  2. Run:  [bold]ollama pull llama3.1[/bold]\n"
            "  3. Ollama starts automatically on Windows after install.\n\n"
            "[dim]Falling back to offline simulation mode.[/dim]",
            title="Ollama Setup Required",
            border_style="yellow",
        ))
        return False


@click.command()
@click.option("--use-ollama", is_flag=True, default=True,
              help="Use Ollama for the RAG agent LLM (requires ollama running).")
@click.option("--ollama-classifier", is_flag=True, default=True,
              help="Also use Ollama for AI-enhanced classification (ambiguous cases only).")
@click.option("--risk", multiple=True,
              type=click.Choice(["critical", "high", "medium", "low", "none"]),
              help="Filter payloads by risk level (repeatable).")
@click.option("--category", multiple=True,
              type=click.Choice([c.value for c in AttackCategory]),
              help="Filter payloads by attack category (repeatable).")
@click.option("--no-benign", is_flag=True, default=False,
              help="Skip benign control payloads.")
@click.option("--no-report", is_flag=True, default=False,
              help="Skip generating HTML/JSON reports.")
@click.option("--verbose", is_flag=True, default=False,
              help="Show detailed output per scenario.")
def main(use_ollama, ollama_classifier, risk, category, no_benign, no_report, verbose):
    """
    Iceberg/RAG Prompt Injection Test Suite

    Tests your Iceberg catalog + RAG pipeline for prompt injection vulnerabilities
    embedded in table descriptions, column documentation, and table properties.

    Requires the local stack to be running:
      docker compose -f docker/docker-compose.yml up -d
    """
    console.print(Panel(
        "[bold cyan]Iceberg / RAG  —  Prompt Injection Test Suite[/bold cyan]\n"
        "[dim]Enterprise security testing for catalog metadata pipelines[/dim]",
        border_style="cyan",
    ))

    # Payload selection
    payloads = list(ALL_PAYLOADS)
    if risk:
        payloads = [p for p in payloads if p.risk_level.value in risk]
    if category:
        payloads = [p for p in payloads if p.category.value in category]
    if no_benign:
        payloads = [p for p in payloads if p.category != AttackCategory.BENIGN]

    if not payloads:
        console.print("[red]No payloads match the selected filters.[/red]")
        raise SystemExit(1)

    console.print(f"\n[bold]Payload library:[/bold]\n{payload_summary()}\n")
    console.print(f"[bold]Selected:[/bold] {len(payloads)} payloads")

    # Ollama check
    ollama_ok = _check_ollama(use_ollama)
    use_ollama_final     = use_ollama and ollama_ok
    use_ollama_classifier_final = ollama_classifier and ollama_ok

    if use_ollama and not ollama_ok:
        console.print("[dim]Falling back to simulation mode.[/dim]\n")

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # Run the suite
    results = run_suite(
        payloads=payloads,
        use_ollama=use_ollama_final,
        use_ollama_classifier=use_ollama_classifier_final,
        verbose=verbose,
    )

    if not results:
        console.print("[red]No results — check that the local stack is running.[/red]")
        raise SystemExit(1)

    metrics = compute_metrics(results)
    print_summary(metrics)

    if not no_report:
        json_path = save_json(results, REPORTS_DIR)
        html_path = save_html(results, REPORTS_DIR)
        console.print(f"\n[bold]Reports saved:[/bold]")
        console.print(f"  JSON → [cyan]{json_path}[/cyan]")
        console.print(f"  HTML → [cyan]{html_path}[/cyan]")
        console.print(f"\n  Open the HTML report in your browser to see the full dashboard.")

    # Exit with non-zero code if any unblocked critical/high attacks
    unblocked_critical = sum(
        1 for r in results
        if not r.error
        and r.payload.risk_level.value in ("critical", "high")
        and r.payload.category != AttackCategory.BENIGN
        and r.injection_succeeded_unguarded
        and not r.injection_blocked_by_guard
    )
    if unblocked_critical:
        console.print(
            f"\n[red]⚠  {unblocked_critical} critical/high attacks not blocked by the guarded pipeline.[/red]"
        )
        sys.exit(1)

    console.print("\n[green]✓ Suite complete.[/green]")


if __name__ == "__main__":
    main()
