"""Sessao Spark local usada pelos jobs da Semana 2."""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession


def build_spark_session(
    *,
    app_name: str = "mktpulse",
    master: str = "local[2]",
    shuffle_partitions: int = 8,
    warehouse_dir: str | Path | None = None,
) -> SparkSession:

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    builder = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
    )

    if warehouse_dir is not None:
        builder = builder.config("spark.sql.warehouse.dir", str(warehouse_dir))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
