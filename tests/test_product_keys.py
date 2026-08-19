"""Tests de la conciliación de identificadores de producto."""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from cafenorte.transform.product_keys import (
    key_desde_handle,
    key_desde_sku_erp,
    key_desde_sku_pos,
    validar_mapeo_explicito,
)


@pytest.mark.parametrize(
    "sku, esperado",
    [
        ("CN-00015", 15),
        ("CN-00001", 1),
        ("CN-00070", 70),
        ("  CN-00007  ", 7),
        ("CN-7", 7),
        ("SKU-00015", None),
        ("CN-ABCDE", None),
        (None, None),
    ],
)
def test_key_desde_sku_pos(spark, sku, esperado):
    df = spark.createDataFrame([(sku,)], "sku string")
    assert df.select(key_desde_sku_pos(F.col("sku"))).first()[0] == esperado


@pytest.mark.parametrize(
    "sku, esperado",
    [
        ("ERP-PROV-MX-018-A", 18),
        ("ERP-PROV-MX-001-A", 1),
        ("ERP-PROV-MX-070-D", 70),
        ("ERP-PROV-018", None),
        (None, None),
    ],
)
def test_key_desde_sku_erp(spark, sku, esperado):
    df = spark.createDataFrame([(sku,)], "sku string")
    assert df.select(key_desde_sku_erp(F.col("sku"))).first()[0] == esperado


@pytest.mark.parametrize(
    "handle, esperado",
    [
        ("tradicional-cafe-molido-018", 18),
        ("selección-cafe-molido-013", 13),
        ("taza-mercancia-039", 39),
        ("sin-numero", None),
        (None, None),
    ],
)
def test_key_desde_handle(spark, handle, esperado):
    df = spark.createDataFrame([(handle,)], "h string")
    assert df.select(key_desde_handle(F.col("h"))).first()[0] == esperado


def test_validar_mapeo_acepta_mapeo_consistente(spark):
    mapeos = spark.createDataFrame(
        [
            ("CN-00003", "ERP-PROV-MX-003-D", "filtros-mercancia-003"),
            ("CN-00006", None, "estándar-cafe-grano-006"),  # sku_erp faltante
            ("CN-00008", "ERP-PROV-MX-008-C", None),  # handle faltante
        ],
        "sku_pos string, sku_erp string, handle string",
    )
    assert validar_mapeo_explicito(mapeos) == []


def test_validar_mapeo_detecta_contradiccion(spark):
    """Si el ERP mapeara CN-00003 a otro consecutivo, el pipeline debe frenar."""
    mapeos = spark.createDataFrame(
        [("CN-00003", "ERP-PROV-MX-009-D", "filtros-mercancia-003")],
        "sku_pos string, sku_erp string, handle string",
    )
    discrepancias = validar_mapeo_explicito(mapeos)
    assert len(discrepancias) == 1
    assert discrepancias[0]["key_pos"] == 3
    assert discrepancias[0]["key_erp"] == 9
