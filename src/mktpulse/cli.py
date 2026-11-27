"""cli do mktpulse.

códigos de saída (contrato com o agendador, ex.: cron, airflow, eventbridge):
    0 = sucesso
    1 = sucesso parcial
    2 = falha
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any

from mktpulse.comum import benchmark as benchmark_module
from mktpulse.comum import config as config_module
from mktpulse.comum import observability as observability_module
from mktpulse.ingestao import pipeline as pipeline_module
from mktpulse.ingestao.sources import create_quote_source
from mktpulse.inferencia import anomaly as anomaly_module
from mktpulse.processamento import artifacts as artifacts_module
from mktpulse.processamento import lake as lake_module
from mktpulse.processamento import silver as silver_module
from mktpulse.processamento import storage as storage_module

logger = logging.getLogger("mktpulse.cli")

EXIT_CODES = {"sucesso": 0, "parcial": 1, "falha": 2}

Handler = Callable[[argparse.Namespace, config_module.Settings, storage_module.Storage], int]

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mktpulse",
        description="Pipeline de dados OHLCV da B3 (ingestao, silver, qualidade e anomalias).",
    )
    parser.add_argument("--log-level", default=None, help="DEBUG, INFO, WARNING...")
    parser.add_argument("--json", action="store_true", help="imprime o resumo em JSON")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest = subparsers.add_parser("ingest", help="busca dados da brapi e grava raw + normalized")
    _add_symbols(ingest, "ex.: PETR4,VALE3 (padrao: MKT_SYMBOLS)")
    ingest.add_argument(
        "--range", dest="range_", default="1y", help="janela historica (padrao: 1y)"
    )
    ingest.add_argument(
        "--interval", dest="interval_", default="1d", help="granularidade (padrao: 1d)"
    )
    _add_run_id(ingest, "reutilize um run_id para demonstrar idempotencia")

    rebuild = subparsers.add_parser("rebuild", help="reconstroi o normalized a partir do raw")
    _add_symbols(rebuild, "padrao: descobre todos os simbolos no raw")
    _add_run_id(rebuild)

    transform = subparsers.add_parser("transform", help="constroi a camada silver com PySpark")
    _add_symbols(transform, "padrao: todos no normalized")
    _add_run_id(transform)
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
    _add_run_id(benchmark)

    detect = subparsers.add_parser("detect", help="detecta anomalias com Isolation Forest")
    _add_symbols(detect, "padrao: todos no silver")
    _add_run_id(detect)
    detect.add_argument("--contamination", type=float, default=None)
    detect.add_argument("--n-estimators", type=int, default=None)
    detect.add_argument("--train-fraction", type=float, default=None)

    inspect = subparsers.add_parser("inspect", help="mostra o snapshot normalized de um ticker")
    inspect.add_argument("--symbol", required=True)
    return parser

def _add_symbols(parser: argparse.ArgumentParser, help_text: str) -> None:
    parser.add_argument("--symbols", default=None, help=help_text)

def _add_run_id(parser: argparse.ArgumentParser, help_text: str | None = None) -> None:
    parser.add_argument("--run-id", default=None, help=help_text)

def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    observability_module.setup_logging(args.log_level or "INFO")

    try:
        settings = _load_settings(args.log_level)
        storage = storage_module.create_storage(settings)
        handler = _HANDLERS.get(args.command)
        if handler is None:
            logger.error("comando desconhecido", extra={"command": args.command})
            return 2
        return handler(args, settings, storage)
    except Exception as exc:  # noqa: BLE001 - fronteira da CLI
        logger.exception("comando falhou")
        print(f"erro: {exc}", file=sys.stderr)
        return 2

def _load_settings(log_level: str | None) -> config_module.Settings:
    settings = config_module.get_settings()
    if log_level is None:
        observability_module.setup_logging(settings.log_level)
    return settings

def _run_ingest(args: argparse.Namespace, settings: config_module.Settings, storage: storage_module.Storage) -> int:
    pipeline = pipeline_module.IngestionPipeline(settings, create_quote_source(settings), storage)
    summary = pipeline.ingest(
        args.symbols,
        range_=args.range_,
        interval=args.interval_,
        run_id=args.run_id,
    )
    _print_ingest_summary(summary, args.json)
    return EXIT_CODES.get(summary.status, 2)

def _run_rebuild(args: argparse.Namespace, settings: config_module.Settings, storage: storage_module.Storage) -> int:
    pipeline = pipeline_module.IngestionPipeline(settings, None, storage)
    summary = pipeline.rebuild(args.symbols, run_id=args.run_id)
    _print_ingest_summary(summary, args.json)
    return EXIT_CODES.get(summary.status, 2)

def _run_transform(args: argparse.Namespace, settings: config_module.Settings, storage: storage_module.Storage) -> int:
    job = silver_module.SilverJob(
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

def _run_benchmark(args: argparse.Namespace, settings: config_module.Settings, storage: storage_module.Storage) -> int:
    run_id = args.run_id or lake_module.new_run_id(observability_module.utc_now())
    result = benchmark_module.run_benchmark(
        rows=args.rows,
        symbol_count=args.symbol_count,
        shuffle_partitions=args.shuffle_partitions or settings.spark_shuffle_partitions,
    )
    artifact_key = lake_module.benchmark_key(run_id)
    artifacts_module.write_json(storage, artifact_key, {"run_id": run_id, **result})
    result["artifact"] = artifact_key
    _print_benchmark(result, args.json)
    return 0

def _run_detect(args: argparse.Namespace, settings: config_module.Settings, storage: storage_module.Storage) -> int:
    job = anomaly_module.AnomalyJob(settings, storage)
    summary = job.run(
        args.symbols,
        run_id=args.run_id,
        contamination=args.contamination,
        n_estimators=args.n_estimators,
        train_fraction=args.train_fraction,
    )
    _print_anomaly_summary(summary, args.json)
    return EXIT_CODES.get(summary.status, 2)

def _run_inspect(args: argparse.Namespace, settings: config_module.Settings, storage: storage_module.Storage) -> int:
    frame = pipeline_module.read_snapshot(storage, settings.source, args.symbol)
    _print_snapshot(settings.source, args.symbol, frame)
    return 0


_HANDLERS: dict[str, Handler] = {
    "ingest": _run_ingest,
    "rebuild": _run_rebuild,
    "transform": _run_transform,
    "benchmark": _run_benchmark,
    "detect": _run_detect,
    "inspect": _run_inspect,
}

def _print_header(summary: Any) -> None:
    print(f"run_id:    {summary.run_id}")
    print(f"operacao:  {summary.operation}")
    print(f"status:    {summary.status}")
    print(f"duracao:   {summary.duration_seconds:.2f}s")

def _print_json(payload: Any, as_json: bool) -> bool:
    if not as_json:
        return False
    print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    return True

def _print_ingest_summary(summary: pipeline_module.RunSummary, as_json: bool) -> None:
    if _print_json(summary.to_dict(), as_json):
        return
    _print_header(summary)
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

def _print_transform_summary(summary: silver_module.SilverSummary, as_json: bool) -> None:
    if _print_json(summary.to_dict(), as_json):
        return
    _print_header(summary)
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
    if _print_json(result, as_json):
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

def _print_anomaly_summary(summary: anomaly_module.AnomalySummary, as_json: bool) -> None:
    if _print_json(summary.to_dict(), as_json):
        return
    _print_header(summary)
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
