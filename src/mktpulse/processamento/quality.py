"""
regras de qualidade avaliadas em um dataframe spark.

cada regra devolve uma condicao que, quando verdadeira, representa uma linha
violando o contrato. regras `critical` bloqueiam a escrita do silver; regras
`warning` sao registradas no relatorio sem interromper o job.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any, Literal

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

Severity = Literal["critical", "warning"]

@dataclass(frozen=True)
class QualityContext:
    as_of: date
    max_daily_return: float = 0.25
    max_gap_days: int = 5
    max_staleness_days: int = 5

@dataclass(frozen=True)
class QualityRule:
    name: str
    description: str
    severity: Severity
    condition: Callable[[DataFrame, QualityContext], Column]

@dataclass
class QualityRuleResult:
    name: str
    description: str
    severity: Severity
    failed_rows: int
    total_rows: int
    sample: list[dict[str, Any]] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return self.failed_rows == 0

    @property
    def failure_rate(self) -> float:
        return round(self.failed_rows / self.total_rows, 6) if self.total_rows else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "severity": self.severity,
            "failed_rows": self.failed_rows,
            "total_rows": self.total_rows,
            "failure_rate": self.failure_rate,
            "passed": self.passed,
            "sample": self.sample,
        }

@dataclass
class QualityReport:
    as_of: str
    total_rows: int
    rules: list[QualityRuleResult]

    @property
    def critical_failures(self) -> int:
        return sum(rule.failed_rows for rule in self.rules if rule.severity == "critical")

    @property
    def warning_failures(self) -> int:
        return sum(rule.failed_rows for rule in self.rules if rule.severity == "warning")

    @property
    def status(self) -> str:
        return "failed" if self.critical_failures else "success"

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "total_rows": self.total_rows,
            "status": self.status,
            "critical_failures": self.critical_failures,
            "warning_failures": self.warning_failures,
            "rules": [rule.to_dict() for rule in self.rules],
        }

def _null_business_key(_frame: DataFrame, _context: QualityContext) -> Column:
    return F.col("symbol").isNull() | F.col("trade_date").isNull()

def _invalid_price(_frame: DataFrame, _context: QualityContext) -> Column:
    condition = F.lit(False)
    for column in ("open", "high", "low", "close", "adjusted_close"):
        condition = condition | F.col(column).isNull() | (F.col(column) <= 0)
    return condition

def _invalid_ohlc(_frame: DataFrame, _context: QualityContext) -> Column:
    return (
        (F.col("high") < F.col("low"))
        | (F.col("high") < F.greatest("open", "close"))
        | (F.col("low") > F.least("open", "close"))
    )

def _negative_volume(_frame: DataFrame, _context: QualityContext) -> Column:
    return F.col("volume").isNull() | (F.col("volume") < 0)

def _future_trade_date(_frame: DataFrame, context: QualityContext) -> Column:
    return F.col("trade_date") > F.lit(context.as_of)

def _duplicate_business_key(_frame: DataFrame, _context: QualityContext) -> Column:
    return F.col("_business_key_count") > 1

def _extreme_daily_return(_frame: DataFrame, context: QualityContext) -> Column:
    return F.abs(F.col("daily_return")) > F.lit(context.max_daily_return)

def _calendar_gap(_frame: DataFrame, context: QualityContext) -> Column:
    return F.datediff("trade_date", F.col("_previous_trade_date")) > F.lit(context.max_gap_days)

def _stale_symbol(_frame: DataFrame, context: QualityContext) -> Column:
    return F.datediff(F.lit(context.as_of), F.col("_latest_trade_date")) > F.lit(
        context.max_staleness_days
    )

DEFAULT_RULES = (
    QualityRule(
        "null_business_key",
        "symbol e trade_date nao podem ser nulos",
        "critical",
        _null_business_key,
    ),
    QualityRule(
        "invalid_price",
        "OHLC e adjusted_close devem ser nao nulos e positivos",
        "critical",
        _invalid_price,
    ),
    QualityRule(
        "invalid_ohlc",
        "high deve cobrir open/close e low deve ser o piso",
        "critical",
        _invalid_ohlc,
    ),
    QualityRule(
        "negative_volume",
        "volume deve ser maior ou igual a zero",
        "critical",
        _negative_volume,
    ),
    QualityRule(
        "future_trade_date",
        "trade_date nao pode estar no futuro em relacao a execucao",
        "critical",
        _future_trade_date,
    ),
    QualityRule(
        "duplicate_business_key",
        "symbol + trade_date deve ser unico",
        "critical",
        _duplicate_business_key,
    ),
    QualityRule(
        "extreme_daily_return",
        "retorno diario absoluto acima do limite operacional",
        "warning",
        _extreme_daily_return,
    ),
    QualityRule(
        "calendar_gap",
        "intervalo entre pregoes acima do limite de calendario",
        "warning",
        _calendar_gap,
    ),
    QualityRule(
        "stale_symbol",
        "ultimo pregao do simbolo esta mais antigo que o limite",
        "warning",
        _stale_symbol,
    ),
)

def evaluate_quality(
    frame: DataFrame,
    *,
    as_of: date | None = None,
    max_daily_return: float = 0.25,
    max_gap_days: int = 5,
    max_staleness_days: int = 5,
    rules: tuple[QualityRule, ...] = DEFAULT_RULES,
) -> QualityReport:

    context = QualityContext(
        as_of=as_of or datetime.now(UTC).date(),
        max_daily_return=max_daily_return,
        max_gap_days=max_gap_days,
        max_staleness_days=max_staleness_days,
    )

    prepared = _prepare_frame(frame)
    total_rows = prepared.count()
    sample_columns = frame.columns
    results: list[QualityRuleResult] = []

    for rule in rules:
        failed_frame = prepared.filter(rule.condition(prepared, context))
        failed_rows = failed_frame.count()
        sample = (
            [
                row.asDict(recursive=True)
                for row in failed_frame.select(*sample_columns).limit(5).collect()
            ]
            if failed_rows
            else []
        )
        results.append(
            QualityRuleResult(
                name=rule.name,
                description=rule.description,
                severity=rule.severity,
                failed_rows=failed_rows,
                total_rows=total_rows,
                sample=sample,
            )
        )

    return QualityReport(
        as_of=context.as_of.isoformat(),
        total_rows=total_rows,
        rules=results,
    )

def _prepare_frame(frame: DataFrame) -> DataFrame:
    symbol_window = Window.partitionBy("symbol").orderBy("trade_date")
    symbol_partition = Window.partitionBy("symbol")
    business_key = Window.partitionBy("symbol", "trade_date")
    return (
        frame.withColumn(
            "_business_key_count",
            F.count(F.lit(1)).over(business_key),
        )
        .withColumn(
            "_previous_trade_date",
            F.lag("trade_date").over(symbol_window),
        )
        .withColumn(
            "_latest_trade_date",
            F.max("trade_date").over(symbol_partition),
        )
    )
