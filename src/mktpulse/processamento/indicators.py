"""
indicadores financeiros calculados com janelas do spark.

as janelas sao por simbolo e ordenadas por data do pregao. o `min_periods`
explicito evita que uma media movel de 20 dias apareca preenchida no primeiro
dia do historico, o que mascararia dados incompletos.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

INDICATOR_COLUMNS = [
    "daily_return",
    "log_return",
    "return_5d",
    "return_20d",
    "volatility_20d",
    "sma_20",
    "sma_50",
    "avg_volume_20d",
    "drawdown_60d",
]

def add_indicators(frame: DataFrame) -> DataFrame:
    by_symbol = Window.partitionBy("symbol").orderBy("trade_date")
    window_20 = by_symbol.rowsBetween(-19, 0)
    window_50 = by_symbol.rowsBetween(-49, 0)
    window_60 = by_symbol.rowsBetween(-59, 0)

    previous = F.lag("adjusted_close", 1).over(by_symbol)
    previous_5 = F.lag("adjusted_close", 5).over(by_symbol)
    previous_20 = F.lag("adjusted_close", 20).over(by_symbol)

    with_previous = frame.withColumn("_previous_close", previous)
    return (
        with_previous.withColumn(
            "daily_return",
            F.when(
                F.col("_previous_close") > 0,
                F.col("adjusted_close") / F.col("_previous_close") - 1.0,
            ),
        )
        .withColumn(
            "log_return",
            F.when(
                F.col("_previous_close") > 0,
                F.log(F.col("adjusted_close") / F.col("_previous_close")),
            ),
        )
        .withColumn(
            "return_5d",
            F.when(
                previous_5 > 0,
                F.col("adjusted_close") / previous_5 - 1.0,
            ),
        )
        .withColumn(
            "return_20d",
            F.when(
                previous_20 > 0,
                F.col("adjusted_close") / previous_20 - 1.0,
            ),
        )
        .withColumn(
            "volatility_20d",
            F.when(
                F.count("daily_return").over(window_20) >= 20,
                F.stddev_samp("daily_return").over(window_20) * F.sqrt(F.lit(252.0)),
            ),
        )
        .withColumn(
            "sma_20",
            F.when(
                F.count("adjusted_close").over(window_20) >= 20,
                F.avg("adjusted_close").over(window_20),
            ),
        )
        .withColumn(
            "sma_50",
            F.when(
                F.count("adjusted_close").over(window_50) >= 50,
                F.avg("adjusted_close").over(window_50),
            ),
        )
        .withColumn(
            "avg_volume_20d",
            F.when(
                F.count("volume").over(window_20) >= 20,
                F.avg("volume").over(window_20),
            ),
        )
        .withColumn(
            "drawdown_60d",
            F.when(
                F.count("adjusted_close").over(window_60) >= 60,
                F.col("adjusted_close") / F.max("adjusted_close").over(window_60) - 1.0,
            ),
        )
        .drop("_previous_close")
    )
