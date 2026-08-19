"""Verificación cruzada: el lake construido contra un cálculo independiente.

Los tests unitarios prueban la lógica sobre datos sintéticos. Estos prueban la
corrida real: recalculan cifras clave leyendo los archivos fuente con pandas
—un camino que no comparte una sola línea con el pipeline de Spark— y las
comparan contra la capa gold. Si ambas rutas coinciden, es poco probable que
un error de transformación haya sobrevivido a las dos.

Se saltan si el lake no está construido (`make run` primero).
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

RAIZ = Path(__file__).resolve().parents[1]
CRUDO = RAIZ / "data" / "raw"
REPORTES = RAIZ / "outputs" / "reportes"

VENTANA_MOM = ("2025-04-01", "2026-03-31")
VENTANA_TRIMESTRE = ("2026-01-01", "2026-03-31")

pytestmark = pytest.mark.skipif(
    not (REPORTES / "p3_ventas_mom_canal.csv").exists(),
    reason="el lake no está construido; corre `make run` antes",
)


# --- referencia calculada con pandas ---------------------------------------
@pytest.fixture(scope="module")
def ventas_pos_ref() -> pd.DataFrame:
    df = pd.read_csv(CRUDO / "sales.csv", parse_dates=["fecha_hora"])
    df["fecha"] = df.fecha_hora.dt.normalize()
    df["signo"] = df.tipo_comprobante.map({"I": 1, "E": -1}).fillna(0)
    df["producto_key"] = df.sku.str.extract(r"CN-0*(\d+)$").astype(int)
    df["importe"] = df.signo * df.monto
    df["unidades_netas"] = df.signo * df.cantidad
    return df


@pytest.fixture(scope="module")
def ecommerce_ref() -> pd.DataFrame:
    df = pd.read_parquet(CRUDO / "ecommerce_orders.parquet")
    fx = pd.read_csv(CRUDO / "exchange_rates.csv", parse_dates=["fecha"])
    df["fecha"] = pd.to_datetime(df.fecha).dt.normalize()
    df["producto_key"] = df.product_handle.str.extract(r"-0*(\d+)$").astype(int)
    df = df.merge(
        fx.rename(columns={"currency": "currency_fx"}),
        left_on=["fecha", "currency"],
        right_on=["fecha", "currency_fx"],
        how="left",
    )
    df["rate_to_mxn"] = df.rate_to_mxn.fillna(1.0).where(df.currency != "MXN", 1.0)
    df["importe"] = df.amount * df.rate_to_mxn
    return df


@pytest.fixture(scope="module")
def inventario_ref() -> pd.DataFrame:
    doc = json.loads((CRUDO / "inventory.json").read_text(encoding="utf-8"))
    df = pd.DataFrame(doc["snapshots"])
    df["fecha"] = pd.to_datetime(df.fecha)
    df["unidades"] = pd.to_numeric(df.cantidad_en_stock, errors="coerce")
    df["producto_key"] = df.sku_erp.str.extract(r"-0*(\d+)-[A-Z]$").astype(int)
    return df


# --- P3: ingreso mensual por canal -----------------------------------------
def test_ingreso_mensual_fisico_coincide_con_calculo_independiente(ventas_pos_ref):
    gold = pd.read_csv(REPORTES / "p3_ventas_mom_canal.csv")
    gold = gold[gold.canal == "fisico"].set_index("mes").ingreso_mxn

    ref = ventas_pos_ref[
        ventas_pos_ref.fecha.between(*VENTANA_MOM)
        & ventas_pos_ref.signo.ne(0)
    ]
    ref = ref.groupby(ref.fecha.dt.strftime("%Y-%m")).importe.sum()

    assert len(gold) == 12
    for mes, valor in ref.items():
        assert gold[mes] == pytest.approx(valor, rel=1e-9)


def test_ingreso_mensual_ecommerce_coincide_con_calculo_independiente(ecommerce_ref):
    gold = pd.read_csv(REPORTES / "p3_ventas_mom_canal.csv")
    gold = gold[gold.canal == "ecommerce"].set_index("mes").ingreso_mxn

    ref = ecommerce_ref[ecommerce_ref.fecha.between(*VENTANA_MOM)]
    ref = ref.groupby(ref.fecha.dt.strftime("%Y-%m")).importe.sum()

    for mes, valor in ref.items():
        assert gold[mes] == pytest.approx(valor, rel=1e-6)


def test_mom_es_consistente_con_las_columnas_de_ingreso():
    gold = pd.read_csv(REPORTES / "p3_ventas_mom_canal.csv").sort_values(["canal", "mes"])
    con_previo = gold.dropna(subset=["ingreso_mes_anterior"])
    esperado = 100 * (con_previo.ingreso_mxn - con_previo.ingreso_mes_anterior) / con_previo.ingreso_mes_anterior
    assert (con_previo.crecimiento_mom_pct - esperado).abs().max() < 0.01


def test_no_se_sumaron_comprobantes_que_no_son_venta(ventas_pos_ref):
    """La suma ingenua de `monto` infla el ingreso; el gold no debe parecérsele."""
    gold = pd.read_csv(REPORTES / "p3_ventas_mom_canal.csv")
    gold_fisico = gold[gold.canal == "fisico"].ingreso_mxn.sum()

    ventana = ventas_pos_ref[ventas_pos_ref.fecha.between(*VENTANA_MOM)]
    ingenuo = ventana.monto.sum()

    assert gold_fisico < ingenuo
    assert ingenuo / gold_fisico > 1.05  # la diferencia es material, no redondeo


# --- P2: quiebres de stock -------------------------------------------------
def test_quiebres_coinciden_con_gaps_and_islands_en_pandas(inventario_ref):
    gold = pd.read_csv(REPORTES / "p2_quiebres_detalle.csv")

    q = inventario_ref[inventario_ref.fecha.between(*VENTANA_TRIMESTRE)].copy()
    q["dia"] = (q.fecha - pd.Timestamp(VENTANA_TRIMESTRE[0])).dt.days
    ceros = q[q.unidades.eq(0)].sort_values(["tienda_id", "producto_key", "fecha"])
    ceros["isla"] = ceros.dia - ceros.groupby(["tienda_id", "producto_key"]).cumcount()
    rachas = ceros.groupby(["tienda_id", "producto_key", "isla"]).size()
    esperadas = rachas[rachas > 3]

    assert len(gold) == len(esperadas)
    assert set(zip(gold.tienda_id, gold.producto_key)) == {
        (t, p) for t, p, _ in esperadas.index
    }
    assert sorted(gold.dias_consecutivos) == sorted(esperadas.values)


def test_ninguna_racha_reportada_incluye_una_lectura_faltante(inventario_ref):
    gold = pd.read_csv(REPORTES / "p2_quiebres_detalle.csv", parse_dates=["inicio_quiebre", "fin_quiebre"])
    idx = inventario_ref.set_index(["tienda_id", "producto_key", "fecha"]).unidades

    for _, r in gold.iterrows():
        dias = pd.date_range(r.inicio_quiebre, r.fin_quiebre)
        valores = [idx.get((r.tienda_id, r.producto_key, d)) for d in dias]
        assert all(v == 0 for v in valores), (r.tienda_id, r.producto_key, valores)


def test_sensibilidad_muestra_que_tratar_na_como_cero_cambia_la_respuesta():
    sens = pd.read_csv(REPORTES / "p2_quiebres_sensibilidad.csv").set_index("escenario")
    assert sens.loc["na_como_cero", "num_quiebres"] > sens.loc["na_corta_racha", "num_quiebres"]


# --- P4: margen ------------------------------------------------------------
def test_margen_negativo_reproduce_los_mismos_productos(ventas_pos_ref, ecommerce_ref):
    gold = pd.read_csv(REPORTES / "p4_margen_negativo_producto.csv")

    doc = json.loads((CRUDO / "inventory.json").read_text(encoding="utf-8"))
    costos = pd.DataFrame(
        [
            (int(p["sku_erp"].split("-")[3]), c["fecha_vigencia"], c["costo_mxn"])
            for p in doc["catalogo"]["productos"]
            for c in p["cost_history"]
        ],
        columns=["producto_key", "fecha", "costo_mxn"],
    )
    costos["fecha"] = pd.to_datetime(costos.fecha)

    pos = ventas_pos_ref[
        ventas_pos_ref.fecha.between(*VENTANA_MOM) & ventas_pos_ref.signo.ne(0)
    ][["producto_key", "fecha", "unidades_netas", "importe"]]
    ecom = ecommerce_ref[ecommerce_ref.fecha.between(*VENTANA_MOM)].assign(
        unidades_netas=lambda d: d.cantidad
    )[["producto_key", "fecha", "unidades_netas", "importe"]]
    todo = pd.concat([pos, ecom]).sort_values("fecha")

    costeado = pd.merge_asof(
        todo,
        costos.sort_values("fecha"),
        on="fecha",
        by="producto_key",
        direction="backward",
    )
    costeado["margen"] = costeado.importe - costeado.costo_mxn * costeado.unidades_netas
    ref = costeado.groupby("producto_key").margen.sum()
    negativos = set(ref[ref < 0].index)

    assert set(gold.producto_key) == negativos
    for _, r in gold.iterrows():
        assert r.margen_mxn == pytest.approx(ref[r.producto_key], rel=1e-6)


def test_margen_por_tienda_suma_al_margen_por_producto():
    prod = pd.read_csv(REPORTES / "p4_margen_negativo_producto.csv").set_index("producto_key")
    tienda = pd.read_csv(REPORTES / "p4_margen_negativo_producto_tienda.csv")
    suma = tienda.groupby("producto_key").margen_mxn.sum()

    for pk, total in prod.margen_mxn.items():
        assert suma[pk] == pytest.approx(total, abs=1.0)


# --- P1: rotación ----------------------------------------------------------
def test_rotacion_es_coherente_con_sus_componentes():
    gold = pd.read_csv(REPORTES / "p1_rotacion_top10.csv")
    calculada = gold.cogs_mxn / gold.inventario_promedio_mxn
    assert (gold.rotacion_veces - calculada).abs().max() < 0.001
    assert gold.rotacion_veces.is_monotonic_decreasing
    assert len(gold) == 10


def test_rotacion_usa_solo_ventas_del_canal_fisico(ventas_pos_ref):
    """Las unidades del top deben venir del POS, sin el volumen de Shopify."""
    gold = pd.read_csv(REPORTES / "p1_rotacion_top10.csv")
    ref = ventas_pos_ref[
        ventas_pos_ref.fecha.between("2025-10-01", "2026-03-31")
        & ventas_pos_ref.signo.ne(0)
    ]
    esperado = ref.groupby("producto_key").unidades_netas.sum()

    for _, r in gold.iterrows():
        assert r.unidades_vendidas == esperado[r.producto_key]


def test_inventario_promedio_usa_todos_los_dias_del_periodo():
    gold = pd.read_csv(REPORTES / "p1_rotacion_top10.csv")
    assert (gold.dias_con_lectura > 175).all()  # 182 días de snapshots disponibles
