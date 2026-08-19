"""Fixtures compartidas.

Se levanta una única SparkSession local para toda la sesión de tests: crearla
por test dispara ~5 s de arranque de JVM cada vez.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from cafenorte.config import cargar_settings

RAIZ = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    sesion = (
        SparkSession.builder.appName("cafenorte-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    sesion.sparkContext.setLogLevel("ERROR")
    yield sesion
    sesion.stop()


@pytest.fixture(scope="session")
def settings():
    return cargar_settings(RAIZ / "conf" / "settings.yaml", raiz=RAIZ)


@pytest.fixture(scope="session")
def corte() -> date:
    """Fecha de corte fija para los tests de marts."""
    return date(2026, 3, 31)


@pytest.fixture(scope="session")
def lake_existe(settings) -> bool:
    return (settings.lake / "gold" / "p1_rotacion_top10").exists()


# --- helpers ---------------------------------------------------------------
def filas(df) -> list[dict]:
    return [f.asDict() for f in df.collect()]
