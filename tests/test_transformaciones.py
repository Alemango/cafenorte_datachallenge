"""Tests de las transformaciones silver sobre datos sintéticos mínimos.

Cada test aísla una regla de negocio que, de romperse, produciría un pipeline
que "corre sin error" con números equivocados.
"""

from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import functions as F

from cafenorte.transform.dimensions import dim_costo_vigencia, dim_tipo_cambio
from cafenorte.transform.facts import (
    fct_inventario_diario,
    fct_ventas_ecommerce,
    fct_ventas_pos,
    ventas_costeadas,
)

ESQUEMA_POS = (
    "venta_id string, fecha_hora string, tienda_id string, sku string, "
    "cantidad int, monto double, moneda string, tipo_comprobante string"
)
ESQUEMA_ECOM = (
    "order_id string, fecha string, product_handle string, cantidad int, "
    "amount double, currency string"
)
ESQUEMA_FX = "fecha string, currency string, rate_to_mxn double"
ESQUEMA_SNAP = "fecha string, tienda_id string, sku_erp string, cantidad_en_stock string"


@pytest.fixture
def fx(spark):
    return dim_tipo_cambio(
        spark.createDataFrame(
            [
                ("2025-04-01", "USD", 17.5),
                ("2025-04-01", "EUR", 20.0),
                ("2025-04-02", "USD", 18.0),
                ("2025-04-02", "EUR", 21.0),
            ],
            ESQUEMA_FX,
        )
    )


# --- CFDI ------------------------------------------------------------------
def test_solo_ingresos_y_devoluciones_afectan_el_importe(spark, fx, settings):
    """P, T y N deben quedar fuera del ingreso; E debe restar."""
    ventas = spark.createDataFrame(
        [
            ("V1", "2025-04-01 10:00:00", "T001", "CN-00001", 2, 100.0, "MXN", "I"),
            ("V2", "2025-04-01 11:00:00", "T001", "CN-00001", 1, 40.0, "MXN", "E"),
            ("V3", "2025-04-01 12:00:00", "T001", "CN-00001", 1, 500.0, "MXN", "P"),
            ("V4", "2025-04-01 13:00:00", "T001", "CN-00001", 1, 700.0, "MXN", "T"),
            ("V5", "2025-04-01 14:00:00", "T001", "CN-00001", 1, 900.0, "MXN", "N"),
        ],
        ESQUEMA_POS,
    )
    df = fct_ventas_pos(ventas, fx, settings)

    assert df.agg(F.sum("importe_mxn")).first()[0] == pytest.approx(60.0)
    assert df.agg(F.sum("unidades")).first()[0] == 1
    assert df.where(F.col("es_ingreso")).count() == 2
    # Los documentos no-venta se conservan para auditoría, en cero.
    no_venta = df.where(~F.col("es_ingreso"))
    assert no_venta.count() == 3
    assert no_venta.agg(F.sum("importe_mxn")).first()[0] == 0.0


def test_devolucion_tiene_signo_negativo(spark, fx, settings):
    ventas = spark.createDataFrame(
        [("V2", "2025-04-01 11:00:00", "T001", "CN-00009", 3, 150.0, "MXN", "E")],
        ESQUEMA_POS,
    )
    fila = fct_ventas_pos(ventas, fx, settings).first()
    assert fila["unidades"] == -3
    assert fila["importe_mxn"] == pytest.approx(-150.0)


def test_ticket_duplicado_se_ignora(spark, fx, settings):
    ventas = spark.createDataFrame(
        [
            ("V1", "2025-04-01 10:00:00", "T001", "CN-00001", 1, 100.0, "MXN", "I"),
            ("V1", "2025-04-01 10:00:00", "T001", "CN-00001", 1, 100.0, "MXN", "I"),
        ],
        ESQUEMA_POS,
    )
    assert fct_ventas_pos(ventas, fx, settings).count() == 1


# --- moneda ----------------------------------------------------------------
def test_ecommerce_convierte_con_el_tipo_de_cambio_del_dia(spark, fx, settings):
    """Dos órdenes iguales en días distintos deben dar importes distintos."""
    ordenes = spark.createDataFrame(
        [
            ("S1", "2025-04-01 09:00:00", "premium-cafe-grano-057", 1, 100.0, "USD"),
            ("S2", "2025-04-02 09:00:00", "premium-cafe-grano-057", 1, 100.0, "USD"),
            ("S3", "2025-04-01 09:00:00", "premium-cafe-grano-057", 1, 100.0, "MXN"),
        ],
        ESQUEMA_ECOM,
    )
    df = fct_ventas_ecommerce(ordenes, fx, settings)
    importes = {f["documento_id"]: f["importe_mxn"] for f in df.collect()}

    assert importes["S1"] == pytest.approx(1750.0)
    assert importes["S2"] == pytest.approx(1800.0)
    assert importes["S3"] == pytest.approx(100.0)  # MXN pasa por la identidad


def test_moneda_desconocida_deja_el_importe_nulo(spark, fx, settings):
    """Una moneda sin tipo de cambio no debe pasar como si fuera MXN.

    El chequeo `ventas_convertidas_a_mxn` convierte este NULL en falla del
    pipeline; lo peligroso sería un `coalesce(..., 1.0)` silencioso.
    """
    ordenes = spark.createDataFrame(
        [("S9", "2025-04-01 09:00:00", "premium-cafe-grano-057", 1, 100.0, "GBP")],
        ESQUEMA_ECOM,
    )
    assert fct_ventas_ecommerce(ordenes, fx, settings).first()["importe_mxn"] is None


def test_ecommerce_va_a_la_pseudo_tienda(spark, fx, settings):
    ordenes = spark.createDataFrame(
        [("S1", "2025-04-01 09:00:00", "premium-cafe-grano-057", 1, 100.0, "MXN")],
        ESQUEMA_ECOM,
    )
    fila = fct_ventas_ecommerce(ordenes, fx, settings).first()
    assert fila["tienda_id"] == settings.negocio.tienda_ecommerce
    assert fila["canal"] == "ecommerce"
    assert fila["producto_key"] == 57


# --- inventario ------------------------------------------------------------
def test_na_no_se_convierte_en_cero(spark, settings):
    snaps = spark.createDataFrame(
        [
            ("2026-01-01", "T001", "ERP-PROV-MX-014-D", "5"),
            ("2026-01-02", "T001", "ERP-PROV-MX-014-D", "N/A"),
            ("2026-01-03", "T001", "ERP-PROV-MX-014-D", "0"),
        ],
        ESQUEMA_SNAP,
    )
    df = fct_inventario_diario(snaps, settings).orderBy("fecha")
    filas = df.collect()

    assert [f["unidades"] for f in filas] == [5, None, 0]
    assert [f["lectura_valida"] for f in filas] == [True, False, True]
    # El día sin lectura no debe contar como quiebre.
    assert df.where(F.col("lectura_valida") & (F.col("unidades") == 0)).count() == 1


def test_inventario_deduplica_por_grano(spark, settings):
    snaps = spark.createDataFrame(
        [
            ("2026-01-01", "T001", "ERP-PROV-MX-014-D", "5"),
            ("2026-01-01", "T001", "ERP-PROV-MX-014-D", "5"),
        ],
        ESQUEMA_SNAP,
    )
    assert fct_inventario_diario(snaps, settings).count() == 1


# --- costos SCD2 -----------------------------------------------------------
@pytest.fixture
def catalogo_con_historia(spark):
    esquema = (
        "sku_erp string, nombre string, categoria string, "
        "cost_history array<struct<fecha_vigencia:string,costo_mxn:double,proveedor:string>>"
    )
    return spark.createDataFrame(
        [
            (
                "ERP-PROV-MX-001-A",
                "Sándwich",
                "comida_caliente",
                [
                    ("2024-10-01", 80.0, "Proveedor A"),
                    ("2025-06-01", 100.0, "Proveedor B"),
                ],
            )
        ],
        esquema,
    )


def test_vigencias_de_costo_se_cierran_sin_huecos(catalogo_con_historia):
    filas = {f["valido_desde"]: f for f in dim_costo_vigencia(catalogo_con_historia).collect()}

    assert filas[date(2024, 10, 1)]["valido_hasta"] == date(2025, 5, 31)
    assert filas[date(2025, 6, 1)]["valido_hasta"] == date(9999, 12, 31)


def test_margen_usa_el_costo_vigente_no_el_ultimo(spark, catalogo_con_historia):
    """Una venta de mayo debe costearse a 80, no a 100."""
    costos = dim_costo_vigencia(catalogo_con_historia)
    ventas = spark.createDataFrame(
        [
            (1, date(2025, 5, 15), 1, 120.0, True),
            (1, date(2025, 7, 15), 1, 120.0, True),
        ],
        "producto_key int, fecha date, unidades int, importe_mxn double, es_ingreso boolean",
    )
    resultado = {
        f["fecha"]: f for f in ventas_costeadas(ventas, costos).collect()
    }

    assert resultado[date(2025, 5, 15)]["costo_total_mxn"] == pytest.approx(80.0)
    assert resultado[date(2025, 5, 15)]["margen_mxn"] == pytest.approx(40.0)
    assert resultado[date(2025, 7, 15)]["costo_total_mxn"] == pytest.approx(100.0)
    assert resultado[date(2025, 7, 15)]["margen_mxn"] == pytest.approx(20.0)


def test_cada_venta_recibe_exactamente_una_vigencia(spark, catalogo_con_historia):
    """Un solapamiento en las vigencias duplicaría líneas y doblaría el COGS."""
    costos = dim_costo_vigencia(catalogo_con_historia)
    ventas = spark.createDataFrame(
        [(1, date(2025, 6, 1), 1, 120.0, True)],
        "producto_key int, fecha date, unidades int, importe_mxn double, es_ingreso boolean",
    )
    assert ventas_costeadas(ventas, costos).count() == 1
