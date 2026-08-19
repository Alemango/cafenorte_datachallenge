"""Tests de los marts de las 4 preguntas, sobre escenarios construidos a mano.

La lógica se verifica contra números calculables a mano, no contra el output
del propio pipeline: un test que compara el pipeline consigo mismo sólo
detecta cambios, no errores.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from cafenorte.marts.business_questions import (
    _inicio_ventana,
    crecimiento_mom_por_canal,
    margen_negativo,
    quiebres_de_stock,
    rotacion_inventario,
)

CORTE = date(2026, 3, 31)

ESQ_VENTAS_COST = (
    "producto_key int, canal string, fecha date, tienda_id string, "
    "documento_id string, es_ingreso boolean, unidades int, "
    "importe_mxn double, costo_total_mxn double, margen_mxn double"
)
ESQ_INV_VAL = (
    "producto_key int, fecha date, tienda_id string, unidades int, "
    "lectura_valida boolean, costo_mxn double, valor_mxn double"
)
ESQ_INV = "producto_key int, fecha date, tienda_id string, unidades int, lectura_valida boolean"
ESQ_PROD = "producto_key int, sku_pos string, sku_erp string, nombre string, categoria string"
ESQ_TIENDA = "tienda_id string, ciudad string, region string"


@pytest.fixture
def dim_prod(spark):
    return spark.createDataFrame(
        [
            (1, "CN-00001", "ERP-PROV-MX-001-A", "Producto Uno", "bebidas"),
            (2, "CN-00002", "ERP-PROV-MX-002-B", "Producto Dos", "panaderia"),
        ],
        ESQ_PROD,
    )


@pytest.fixture
def dim_tie(spark):
    return spark.createDataFrame(
        [("T001", "CDMX", "centro"), ("ONLINE", "N/A", "digital")], ESQ_TIENDA
    )


# --- ventanas --------------------------------------------------------------
@pytest.mark.parametrize(
    "meses, esperado",
    [(6, date(2025, 10, 1)), (12, date(2025, 4, 1)), (1, date(2026, 3, 1))],
)
def test_inicio_ventana(meses, esperado):
    assert _inicio_ventana(CORTE, meses) == esperado


def test_inicio_ventana_cruza_anio():
    assert _inicio_ventana(date(2026, 1, 15), 3) == date(2025, 11, 1)


# --- P1 rotación -----------------------------------------------------------
def test_rotacion_es_cogs_sobre_inventario_promedio(spark, dim_prod, settings):
    """COGS 600 / inventario promedio 300 = 2.0 vueltas."""
    ventas = spark.createDataFrame(
        [
            (1, "fisico", date(2026, 1, 10), "T001", "V1", True, 6, 900.0, 600.0, 300.0),
            # Fuera de ventana: no debe contar.
            (1, "fisico", date(2025, 1, 10), "T001", "V0", True, 99, 999.0, 999.0, 0.0),
            # E-commerce: excluido por diseño (no hay inventario que lo respalde).
            (1, "ecommerce", date(2026, 1, 11), "ONLINE", "S1", True, 5, 800.0, 500.0, 300.0),
        ],
        ESQ_VENTAS_COST,
    )
    inventario = spark.createDataFrame(
        [
            (1, date(2026, 1, 10), "T001", 4, True, 100.0, 400.0),
            (1, date(2026, 1, 11), "T001", 2, True, 100.0, 200.0),
        ],
        ESQ_INV_VAL,
    )
    fila = rotacion_inventario(ventas, inventario, dim_prod, CORTE, settings).first()

    assert fila["cogs_mxn"] == pytest.approx(600.0)
    assert fila["inventario_promedio_mxn"] == pytest.approx(300.0)
    assert fila["rotacion_veces"] == pytest.approx(2.0)


def test_rotacion_ignora_lecturas_invalidas(spark, dim_prod, settings):
    """Un día con N/A no debe arrastrar el inventario promedio hacia abajo."""
    ventas = spark.createDataFrame(
        [(1, "fisico", date(2026, 1, 10), "T001", "V1", True, 6, 900.0, 600.0, 300.0)],
        ESQ_VENTAS_COST,
    )
    inventario = spark.createDataFrame(
        [
            (1, date(2026, 1, 10), "T001", 4, True, 100.0, 400.0),
            (1, date(2026, 1, 11), "T001", 2, True, 100.0, 200.0),
            (1, date(2026, 1, 12), "T001", None, False, 100.0, None),
        ],
        ESQ_INV_VAL,
    )
    fila = rotacion_inventario(ventas, inventario, dim_prod, CORTE, settings).first()
    assert fila["inventario_promedio_mxn"] == pytest.approx(300.0)
    assert fila["dias_con_lectura"] == 2


def test_rotacion_devuelve_top_n_ordenado(spark, dim_prod, settings):
    ventas = spark.createDataFrame(
        [
            (1, "fisico", date(2026, 1, 10), "T001", "V1", True, 6, 900.0, 600.0, 300.0),
            (2, "fisico", date(2026, 1, 10), "T001", "V2", True, 3, 500.0, 300.0, 200.0),
        ],
        ESQ_VENTAS_COST,
    )
    inventario = spark.createDataFrame(
        [
            (1, date(2026, 1, 10), "T001", 3, True, 100.0, 300.0),
            (2, date(2026, 1, 10), "T001", 6, True, 100.0, 600.0),
        ],
        ESQ_INV_VAL,
    )
    filas = rotacion_inventario(ventas, inventario, dim_prod, CORTE, settings, top_n=1).collect()
    assert len(filas) == 1
    assert filas[0]["producto_key"] == 1  # 2.0 vueltas contra 0.5


# --- P2 quiebres -----------------------------------------------------------
def _snapshots(spark, patron, tienda="T001", producto=1, inicio=date(2026, 1, 1)):
    """`patron` es una lista de int (unidades) o None (lectura ausente)."""
    filas = [
        (producto, inicio + timedelta(days=i), tienda, u, u is not None)
        for i, u in enumerate(patron)
    ]
    return spark.createDataFrame(filas, ESQ_INV)


def test_racha_de_cuatro_dias_se_reporta(spark, dim_prod, dim_tie, settings):
    inv = _snapshots(spark, [5, 0, 0, 0, 0, 3])
    detalle, resumen = quiebres_de_stock(inv, dim_prod, dim_tie, CORTE, settings)

    assert detalle.count() == 1
    fila = detalle.first()
    assert fila["dias_consecutivos"] == 4
    assert fila["inicio_quiebre"] == date(2026, 1, 2)
    assert fila["fin_quiebre"] == date(2026, 1, 5)
    assert resumen.first()["racha_mas_larga"] == 4


def test_racha_de_tres_dias_no_se_reporta(spark, dim_prod, dim_tie, settings):
    """"Más de 3 días" se lee estricto."""
    inv = _snapshots(spark, [5, 0, 0, 0, 3])
    detalle, _ = quiebres_de_stock(inv, dim_prod, dim_tie, CORTE, settings)
    assert detalle.count() == 0


def test_lectura_faltante_corta_la_racha(spark, dim_prod, dim_tie, settings):
    """0,0,N/A,0,0 son dos rachas de 2, no una de 5."""
    inv = _snapshots(spark, [0, 0, None, 0, 0])
    detalle, _ = quiebres_de_stock(inv, dim_prod, dim_tie, CORTE, settings)
    assert detalle.count() == 0


def test_lectura_faltante_no_cuenta_como_quiebre(spark, dim_prod, dim_tie, settings):
    """El error clásico: tratar N/A como cero inventa un quiebre de 5 días."""
    inv = _snapshots(spark, [None, None, None, None, None])
    detalle, _ = quiebres_de_stock(inv, dim_prod, dim_tie, CORTE, settings)
    assert detalle.count() == 0


def test_quiebres_fuera_del_trimestre_se_excluyen(spark, dim_prod, dim_tie, settings):
    inv = _snapshots(spark, [0, 0, 0, 0, 0], inicio=date(2025, 12, 1))
    detalle, _ = quiebres_de_stock(inv, dim_prod, dim_tie, CORTE, settings)
    assert detalle.count() == 0


def test_rachas_de_tiendas_distintas_no_se_mezclan(spark, dim_prod, dim_tie, settings):
    inv = _snapshots(spark, [0, 0], tienda="T001").unionByName(
        _snapshots(spark, [0, 0], tienda="ONLINE", inicio=date(2026, 1, 3))
    )
    detalle, _ = quiebres_de_stock(inv, dim_prod, dim_tie, CORTE, settings)
    assert detalle.count() == 0


# --- P3 MoM ----------------------------------------------------------------
def test_mom_calcula_variacion_por_canal(spark, settings):
    ventas = spark.createDataFrame(
        [
            ("fisico", date(2025, 4, 5), True, 1, 1000.0, "V1"),
            ("fisico", date(2025, 5, 5), True, 1, 1500.0, "V2"),
            ("ecommerce", date(2025, 4, 5), True, 1, 200.0, "S1"),
            ("ecommerce", date(2025, 5, 5), True, 1, 100.0, "S2"),
        ],
        "canal string, fecha date, es_ingreso boolean, unidades int, importe_mxn double, documento_id string",
    )
    filas = {
        (f["canal"], f["mes"]): f
        for f in crecimiento_mom_por_canal(ventas, CORTE, settings).collect()
    }

    assert filas[("fisico", "2025-04")]["crecimiento_mom_pct"] is None
    assert filas[("fisico", "2025-05")]["crecimiento_mom_pct"] == pytest.approx(50.0)
    assert filas[("ecommerce", "2025-05")]["crecimiento_mom_pct"] == pytest.approx(-50.0)


def test_mom_excluye_documentos_que_no_son_venta(spark, settings):
    ventas = spark.createDataFrame(
        [
            ("fisico", date(2025, 4, 5), True, 1, 1000.0, "V1"),
            ("fisico", date(2025, 4, 6), False, 0, 0.0, "V2"),
        ],
        "canal string, fecha date, es_ingreso boolean, unidades int, importe_mxn double, documento_id string",
    )
    fila = crecimiento_mom_por_canal(ventas, CORTE, settings).first()
    assert fila["ingreso_mxn"] == pytest.approx(1000.0)
    assert fila["documentos"] == 1


# --- P4 margen -------------------------------------------------------------
def test_margen_negativo_detecta_producto_y_tiendas(spark, dim_prod, dim_tie, settings):
    ventas = spark.createDataFrame(
        [
            # Producto 1: pierde en T001, gana en ONLINE, negativo en total.
            (1, "fisico", date(2026, 1, 10), "T001", "V1", True, 10, 800.0, 1000.0, -200.0),
            (1, "ecommerce", date(2026, 1, 10), "ONLINE", "S1", True, 1, 150.0, 100.0, 50.0),
            # Producto 2: rentable, no debe aparecer.
            (2, "fisico", date(2026, 1, 10), "T001", "V2", True, 5, 900.0, 400.0, 500.0),
        ],
        ESQ_VENTAS_COST,
    )
    por_producto, por_tienda = margen_negativo(ventas, dim_prod, dim_tie, CORTE, settings)

    assert [f["producto_key"] for f in por_producto.collect()] == [1]
    assert por_producto.first()["margen_mxn"] == pytest.approx(-150.0)

    detalle = {f["tienda_id"]: f for f in por_tienda.collect()}
    assert set(detalle) == {"T001", "ONLINE"}
    assert detalle["T001"]["margen_mxn"] == pytest.approx(-200.0)
    # La tienda rentable del producto problemático se conserva como contraste.
    assert detalle["ONLINE"]["margen_mxn"] == pytest.approx(50.0)


def test_margen_ignora_ventas_fuera_de_ventana(spark, dim_prod, dim_tie, settings):
    ventas = spark.createDataFrame(
        [(1, "fisico", date(2024, 11, 1), "T001", "V1", True, 10, 800.0, 1000.0, -200.0)],
        ESQ_VENTAS_COST,
    )
    por_producto, _ = margen_negativo(ventas, dim_prod, dim_tie, CORTE, settings)
    assert por_producto.count() == 0
