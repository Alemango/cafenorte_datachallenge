"""Construcción de la SparkSession y utilidades de escritura del lake."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from pyspark.sql import DataFrame, SparkSession

from .config import Settings

log = logging.getLogger(__name__)


def construir_sesion(settings: Settings) -> SparkSession:
    """SparkSession local con configuración conservadora.

    El volumen real (~330k filas) no justifica un clúster; se usa `local[*]`
    con pocas particiones de shuffle para evitar el overhead de tareas vacías.
    """
    cfg = settings.spark
    return (
        SparkSession.builder.appName(cfg["app_name"])
        .master(cfg["master"])
        .config("spark.sql.shuffle.partitions", cfg["shuffle_partitions"])
        .config("spark.driver.memory", cfg["driver_memory"])
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


@contextmanager
def sesion(settings: Settings) -> Iterator[SparkSession]:
    spark = construir_sesion(settings)
    spark.sparkContext.setLogLevel("ERROR")
    try:
        yield spark
    finally:
        spark.stop()


def escribir(df: DataFrame, destino: Path, particiones: list[str] | None = None) -> None:
    """Persiste una tabla del lake en Parquet, sobrescribiendo la anterior."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    writer = df.write.mode("overwrite")
    if particiones:
        writer = writer.partitionBy(*particiones)
    writer.parquet(str(destino))
    log.info("escrito %s", destino)


def leer(spark: SparkSession, origen: Path) -> DataFrame:
    return spark.read.parquet(str(origen))


def exportar_csv(df: DataFrame, destino: Path) -> None:
    """Exporta un mart pequeño a un único CSV legible para el README/entrevista."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    df.toPandas().to_csv(destino, index=False)
    log.info("exportado %s", destino)
