"""cli do mktpulse.

codigos de saida:
    0 = sucesso
    1 = sucesso parcial (algum ticker falhou)
    2 = falha (nada gravado, configuracao invalida ou erro fatal)

esses codigos sao o contrato com o agendador (cron, airflow, eventbridge).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any

from .anomaly import AnomalyJob, AnomalySummary
from .benchmark import run_benchmark
from .config import Settings, get_settings
from .lake import benchmark_key, new_run_id, utc_now
from .observability import setup_logging
from .pipeline import IngestionPipeline, RunSummary, read_snapshot
from .silver import SilverJob, SilverSummary
from .sources import create_quote_source
from .storage import create_storage

logger = logging.getLogger("mktpulse.cli")

EXIT_CODES = {"success": 0, "partial": 1, "failed": 2}

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mktpulse",
        description="Pipeline de dados OHLCV da B3 (ingestao, silver, qualidade e anomalias).",
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

    transform = subparsers.add_parser("transform", help="constroi a camada silver com PySpark")
    transform.add_argument("--symbols", default=None, help="padrao: todos no normalized")
    transform.add_argument("--run-id", default=None)
    transform.add_argument(
        "--full-refresh",
        action="store_true",
        help="apaga o silver antes de escrever (rebuild completo)",
    )
    transform.add_argument("--master", default=None, help="ex.: local[2]")
    transform.add_argument("--shuffle-partitions", type=int, default=None)

    benchmark = subparsers.add_parser(
        "benchmark", help="compara Pandas e Spark com dados sinteticos"
    )
    benchmark.add_argument("--rows", type=int, default=250_000)
    benchmark.add_argument("--symbol-count", type=int, default=8)
    benchmark.add_argument("--shuffle-partitions", type=int, default=None)
    benchmark.add_argument("--run-id", default=None)

    detect = subparsers.add_parser("detect", help="detecta anomalias com Isolation Forest")
    detect.add_argument("--symbols", default=None, help="padrao: todos no silver")
    detect.add_argument("--run-id", default=None)
    detect.add_argument("--contamination", type=float, default=None)
    detect.add_argument("--n-estimators", type=int, default=None)
    detect.add_argument("--train-fraction", type=float, default=None)

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

def _print_transform_summary(summary: SilverSummary, as_json: bool) -> None:
    if as_json:
        print(json.dumps(summary.to_dict(), indent=2, ensure_ascii=False, default=str))
        return

    print(f"run_id:    {summary.run_id}")
    print(f"operacao:  {summary.operation}")
    print(f"status:    {summary.status}")
    print(f"duracao:   {summary.duration_seconds:.2f}s")
    print(
        f"linhas:    lidas={summary.totals['rows_read']} escritas={summary.totals['rows_written']}"
    )
    print(
        "silver:    "
        f"simbolos={summary.totals['symbols']} "
        f"particoes={summary.totals['partitions_written']} "
        f"bytes={summary.totals['bytes_written']}"
    )
    print(
        "qualidade: "
        f"criticas={summary.totals['critical_failures']} "
        f"warnings={summary.totals['warnings']}"
    )
    print(f"manifest:  {summary.manifest_key}")
    print(f"quality:   {summary.quality_report_key}")
    for rule in summary.quality.get("rules", []):
        if rule["failed_rows"]:
            print(f"  {rule['severity']} {rule['name']}: {rule['failed_rows']} linha(s)")
    if summary.error:
        print(f"erro:      {summary.error}")

def _print_benchmark(result: dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        return

    print(f"linhas:    {result['rows']} ({result['symbols']} simbolos)")
    print(
        "pandas:    "
        f"{result['pandas']['seconds']}s "
        f"({result['pandas']['rows_per_second']} linhas/s)"
    )
    print(
        f"spark:     {result['spark']['seconds']}s ({result['spark']['rows_per_second']} linhas/s)"
    )
    print(f"speedup:   pandas={result['pandas_speedup']}x spark")
    print(f"artefato:  {result['artifact']}")

def _print_anomaly_summary(summary: AnomalySummary, as_json: bool) -> None:
    if as_json:
        print(json.dumps(summary.to_dict(), indent=2, ensure_ascii=False, default=str))
        return

    print(f"run_id:    {summary.run_id}")
    print(f"operacao:  {summary.operation}")
    print(f"status:    {summary.status}")
    print(f"duracao:   {summary.duration_seconds:.2f}s")
    print(
        "linhas:    "
        f"entrada={summary.totals['rows_input']} "
        f"com_features={summary.totals['rows_scored']}"
    )
    print(
        "anomalias: "
        f"{summary.totals['anomalies']} "
        f"({summary.totals['anomaly_rate']:.2%}) "
        f"simbolos={summary.totals['symbols']}"
    )
    print(f"treino:    {summary.totals['training_rows']} linha(s)")
    print(f"gold:      {summary.gold_prefix}")
    print(f"modelo:    {summary.model_key}")
    print(f"manifest:  {summary.manifest_key}")
    print(f"metrics:   {summary.metrics_key}")
    for anomaly in summary.metrics.get("top_anomalies", [])[:5]:
        print(
            f"  {anomaly['trade_date']} {anomaly['symbol']}: "
            f"score={anomaly['anomaly_score']:.4f} "
            f"retorno={anomaly['daily_return']}"
        )
    if summary.error:
        print(f"erro:      {summary.error}")

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

        if args.command == "transform":
            job = SilverJob(
                settings,
                storage,
                master=args.master,
                shuffle_partitions=args.shuffle_partitions,
            )
            summary = job.run(
                args.symbols,
                run_id=args.run_id,
                full_refresh=args.full_refresh,
            )
            _print_transform_summary(summary, args.json)
            return EXIT_CODES.get(summary.status, 2)

        if args.command == "benchmark":
            run_id = args.run_id or new_run_id(utc_now())
            result = run_benchmark(
                rows=args.rows,
                symbol_count=args.symbol_count,
                shuffle_partitions=(args.shuffle_partitions or settings.spark_shuffle_partitions),
            )
            artifact_key = benchmark_key(run_id)
            payload = {"run_id": run_id, **result}
            storage.write_bytes(
                artifact_key,
                json.dumps(payload, indent=2, ensure_ascii=False, default=str).encode("utf-8"),
            )
            result["artifact"] = artifact_key
            _print_benchmark(result, args.json)
            return 0

        if args.command == "detect":
            job = AnomalyJob(settings, storage)
            summary = job.run(
                args.symbols,
                run_id=args.run_id,
                contamination=args.contamination,
                n_estimators=args.n_estimators,
                train_fraction=args.train_fraction,
            )
            _print_anomaly_summary(summary, args.json)
            return EXIT_CODES.get(summary.status, 2)

        if args.command == "inspect":
            frame = read_snapshot(storage, settings.source, args.symbol)
            _print_snapshot(settings.source, args.symbol, frame)
            return 0

        logger.error("comando desconhecido", extra={"command": args.command})
        return 2
    except Exception as exc:  # noqa: ble001 - fronteira da cli
        logger.exception("comando falhou")
        print(f"erro: {exc}", file=sys.stderr)
        return 2

def _load_settings(log_level: str | None) -> Settings:
    settings = get_settings()
    if log_level is None:
        setup_logging(settings.log_level)
    return settings
