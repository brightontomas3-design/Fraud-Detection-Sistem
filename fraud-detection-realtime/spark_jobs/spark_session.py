"""spark_session.py - membuat SparkSession (lokal di laptop atau di container Docker)."""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402


def _ensure_java():
    """Spark 3.5 butuh Java 17. Di Docker sudah terpasang; di laptop bisa memakai paket pip jdk4py."""
    if os.environ.get("JAVA_HOME") and Path(os.environ["JAVA_HOME"]).exists():
        return
    try:
        import jdk4py
        os.environ["JAVA_HOME"] = str(jdk4py.JAVA_HOME)
    except ImportError:
        pass


def get_spark(app: str, kafka: bool = False, shuffle_partitions: int = 4):
    _ensure_java()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    from pyspark.sql import SparkSession
    b = (SparkSession.builder.appName(app).master(config.SPARK_MASTER)
         .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
         .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "3g"))
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.showConsoleProgress", "false"))
    if kafka:
        b = b.config("spark.jars.packages", config.SPARK_KAFKA_PACKAGE)
        if os.getenv("IVY_HOME"):
            b = b.config("spark.jars.ivy", os.environ["IVY_HOME"])
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
