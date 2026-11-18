"""CLI do mktpulse.

Codigos de saida:
    0 = sucesso
    1 = sucesso parcial (algum ticker falhou)
    2 = falha (nada gravado, configuracao invalida ou erro fatal)

Esses codigos sao o contrato com o agendador (cron, Airflow, EventBridge).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any

from .config import Settings, get_settings
from .observability import setup_logging
from .pipeline import IngestionPipeline, RunSummary, read_snapshot
from .sources import create_quote_source
from .storage import create_storage

logger = logging.getLogger("mktpulse.cli")

EXIT_CODES = {"success": 0, "partial": 1, "failed": 2}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mktpulse",
        description="Pipeline de dados OHLCV da B3 (Semana 1: ingestao e armazenamento).",
    )
    parser.add_argument("--log-level", default=None, help="DEBUG, INFO, WARNING...")
    parser.add_argument("--json", action="store_true", help="imprime o resumo em JSON")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest = subparsers.add_parser("ingest", help="busca dados da brapi e grava raw + normalized")
    ingest.add_argument("--symbols", default=None, help="ex.: PETR4,VALE3 (padrao: MKT_SYMBOLS)")
    ingest.add_argument(
        "--range", dest="range_", default="1y", help="janela historica (padrao: 1y)"
    )
    ingest.add_argument(
        "--interval", dest="interval_", default="1d", help="granularidade (padrao: 1d)"
    )
    ingest.add_argument(
        "--run-id", default=None, help="reutilize um run_id para demonstrar idempotencia"
    )

    rebuild = subparsers.add_parser("rebuild", help="reconstroi o normalized a partir do raw")
    rebuild.add_argument(
        "--symbols", default=None, help="padrao: descobre todos os simbolos no raw"
    )
    rebuild.add_argument("--run-id", default=None)

    inspect = subparsers.add_parser("inspect", help="mostra o snapshot normalized de um ticker")
    inspect.add_argument("--symbol", required=True)
    return parser


def _print_summary(summary: RunSummary, as_json: bool) -> None:
    if as_json:
        print(json.dumps(summary.to_dict(), indent=2, ensure_ascii=False, default=str))
        return

    print(f"run_id:    {summary.run_id}")
    print(f"operacao:  {summary.operation}")
    print(f"status:    {summary.status}")
    print(f"duracao:   {summary.duration_seconds:.2f}s")
    print(
        "simbolos:  "
        f"{summary.totals['symbols_ok']} ok / "
        f"{summary.totals['symbols_failed']} falha(s)"
    )
    print(
        "linhas:    "
        f"recebidas={summary.totals['rows_received']} "
        f"ignoradas={summary.totals['rows_skipped']} "
        f"gravadas={summary.totals['rows_written']} "
        f"armazenadas={summary.totals['rows_stored']}"
    )
    print(
        "raw:       "
        f"escritos={summary.totals['raw_objects_written']} "
        f"ignorados={summary.totals['raw_objects_skipped']} "
        f"bytes={summary.totals['raw_bytes_written']}"
    )
    print(f"manifest:  {summary.manifest_key}")
    for symbol, outcome in summary.symbols.items():
        if outcome.status != "ok":
            print(f"  ERRO {symbol}: {outcome.error}")


def _print_snapshot(source: str, symbol: str, frame: Any) -> None:
    columns = [
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "adjusted_close",
        "volume",
        "run_id",
    ]
    print(f"source:  {source}")
    print(f"symbol:  {symbol.upper()}")
    print(f"rows:    {len(frame)}")
    print(f"periodo: {frame['trade_date'].min()} -> {frame['trade_date'].max()}")
    print("ultimas linhas:")
    print(frame[columns].tail(5).to_string(index=False))


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    setup_logging(args.log_level or "INFO")

    try:
        settings = _load_settings(args.log_level)
        storage = create_storage(settings)

        if args.command == "ingest":
            client = create_quote_source(settings)
            pipeline = IngestionPipeline(settings, client, storage)
            summary = pipeline.ingest(
                args.symbols,
                range_=args.range_,
                interval=args.interval_,
                run_id=args.run_id,
            )
            _print_summary(summary, args.json)
            return EXIT_CODES.get(summary.status, 2)

        if args.command == "rebuild":
            pipeline = IngestionPipeline(settings, None, storage)
            summary = pipeline.rebuild(args.symbols, run_id=args.run_id)
            _print_summary(summary, args.json)
            return EXIT_CODES.get(summary.status, 2)

        if args.command == "inspect":
            frame = read_snapshot(storage, settings.source, args.symbol)
            _print_snapshot(settings.source, args.symbol, frame)
            return 0

        logger.error("comando desconhecido", extra={"command": args.command})
        return 2
    except Exception as exc:  # noqa: BLE001 - fronteira da CLI
        logger.exception("comando falhou")
        print(f"erro: {exc}", file=sys.stderr)
        return 2


def _load_settings(log_level: str | None) -> Settings:
    settings = get_settings()
    if log_level is None:
        setup_logging(settings.log_level)
    return settings
