"""Orquestador del pipeline CaféNorte: bronze -> silver -> gold -> reportes.

Uso:
    python -m cafenorte.pipeline                 # corrida completa
    python -m cafenorte.pipeline --capa silver   # hasta silver
    python -m cafenorte.pipeline --fecha-corte 2026-03-31
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Settings, cargar_settings
from .ingest import bronze as ing
from .marts import business_questions as bq
from .quality import expectations as qc
from .spark import escribir, exportar_csv, leer, sesion
from .transform import dimensions as dim
from .transform import facts as fct

log = logging.getLogger("cafenorte")

CAPAS = ("bronze", "silver", "gold")


# --------------------------------------------------------------------------
def construir_bronze(spark: SparkSession, settings: Settings) -> dict[str, DataFrame]:
    tablas = {
        "bronze_ventas_pos": ing.leer_ventas_pos(spark, settings),
        "bronze_ecommerce": ing.leer_ecommerce(spark, settings),
        "bronze_tipos_cambio": ing.leer_tipos_de_cambio(spark, settings),
    }
    tablas.update(ing.leer_inventario_erp(spark, settings))
    for nombre, df in tablas.items():
        escribir(df, settings.ruta_capa("bronze", nombre))
    return tablas


def _cargar_bronze(spark: SparkSession, settings: Settings) -> dict[str, DataFrame]:
    nombres = [
        "bronze_ventas_pos",
        "bronze_ecommerce",
        "bronze_tipos_cambio",
        "bronze_tiendas",
        "bronze_sku_mappings",
        "bronze_catalogo",
        "bronze_snapshots",
    ]
    return {n: leer(spark, settings.ruta_capa("bronze", n)) for n in nombres}


def resolver_fecha_corte(bronze: dict[str, DataFrame], settings: Settings) -> date:
    """Fecha de corte = máxima fecha transaccional observada, salvo override.

    Anclar el análisis al dato y no a `current_date()` hace que las respuestas
    sean reproducibles: el mismo repo corrido en 2027 devuelve los mismos
    números.
    """
    if settings.negocio.fecha_corte:
        return settings.negocio.fecha_corte

    max_pos = bronze["bronze_ventas_pos"].select(
        F.max(F.to_date("fecha_hora")).alias("f")
    ).first()["f"]
    max_ecom = bronze["bronze_ecommerce"].select(
        F.max(F.to_date("fecha")).alias("f")
    ).first()["f"]
    return max(max_pos, max_ecom)


# --------------------------------------------------------------------------
def construir_silver(
    bronze: dict[str, DataFrame], settings: Settings
) -> dict[str, DataFrame]:
    dim_fx = dim.dim_tipo_cambio(bronze["bronze_tipos_cambio"])
    dim_prod = dim.dim_producto(bronze["bronze_catalogo"], bronze["bronze_sku_mappings"])
    dim_tie = dim.dim_tienda(bronze["bronze_tiendas"], settings)
    dim_costo = dim.dim_costo_vigencia(bronze["bronze_catalogo"])

    ventas_pos = fct.fct_ventas_pos(bronze["bronze_ventas_pos"], dim_fx, settings)
    ventas_ecom = fct.fct_ventas_ecommerce(bronze["bronze_ecommerce"], dim_fx, settings)
    ventas = fct.fct_ventas(ventas_pos, ventas_ecom)

    inventario = fct.fct_inventario_diario(bronze["bronze_snapshots"], settings)
    inv_valuado = fct.inventario_valuado(inventario, dim_costo)
    ventas_cost = fct.ventas_costeadas(ventas, dim_costo)

    tablas = {
        "dim_producto": dim_prod,
        "dim_tienda": dim_tie,
        "dim_costo_vigencia": dim_costo,
        "dim_tipo_cambio": dim_fx,
        "fct_ventas": ventas,
        "fct_ventas_costeadas": ventas_cost,
        "fct_inventario_diario": inventario,
        "fct_inventario_valuado": inv_valuado,
    }
    for nombre, df in tablas.items():
        escribir(df, settings.ruta_capa("silver", nombre))
    return tablas


def _cargar_silver(spark: SparkSession, settings: Settings) -> dict[str, DataFrame]:
    nombres = [
        "dim_producto",
        "dim_tienda",
        "dim_costo_vigencia",
        "dim_tipo_cambio",
        "fct_ventas",
        "fct_ventas_costeadas",
        "fct_inventario_diario",
        "fct_inventario_valuado",
    ]
    return {n: leer(spark, settings.ruta_capa("silver", n)) for n in nombres}


# --------------------------------------------------------------------------
def construir_gold(
    silver: dict[str, DataFrame], corte: date, settings: Settings
) -> dict[str, DataFrame]:
    rotacion = bq.rotacion_inventario(
        silver["fct_ventas_costeadas"],
        silver["fct_inventario_valuado"],
        silver["dim_producto"],
        corte,
        settings,
    )
    quiebres_detalle, quiebres_resumen = bq.quiebres_de_stock(
        silver["fct_inventario_diario"],
        silver["dim_producto"],
        silver["dim_tienda"],
        corte,
        settings,
    )
    sensibilidad = bq.quiebres_sensibilidad(
        silver["fct_inventario_diario"], corte, settings
    )
    dias_cero = bq.dias_en_cero_por_tienda(
        silver["fct_inventario_diario"], silver["dim_tienda"], corte
    )
    mom = bq.crecimiento_mom_por_canal(silver["fct_ventas"], corte, settings)
    margen_prod, margen_tienda = bq.margen_negativo(
        silver["fct_ventas_costeadas"],
        silver["dim_producto"],
        silver["dim_tienda"],
        corte,
        settings,
    )

    tablas = {
        "p1_rotacion_top10": rotacion,
        "p2_quiebres_detalle": quiebres_detalle,
        "p2_quiebres_por_tienda": quiebres_resumen,
        "p2_quiebres_sensibilidad": sensibilidad,
        "p2_dias_en_cero_por_tienda": dias_cero,
        "p3_ventas_mom_canal": mom,
        "p4_margen_negativo_producto": margen_prod,
        "p4_margen_negativo_producto_tienda": margen_tienda,
    }
    for nombre, df in tablas.items():
        df = df.cache()
        escribir(df, settings.ruta_capa("gold", nombre))
        exportar_csv(df, settings.reportes / f"{nombre}.csv")
        tablas[nombre] = df
    return tablas


# --------------------------------------------------------------------------
def _guardar_reporte_calidad(
    resultados: list[qc.Resultado], corte: date, settings: Settings
) -> None:
    destino = settings.reportes / "reporte_calidad.json"
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(
        json.dumps(
            {
                "ejecutado_en": datetime.now().isoformat(timespec="seconds"),
                "fecha_corte": corte.isoformat(),
                "chequeos": [asdict(r) for r in resultados],
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def ejecutar(settings: Settings, hasta: str = "gold") -> dict[str, DataFrame]:
    with sesion(settings) as spark:
        log.info("== bronze ==")
        bronze = construir_bronze(spark, settings)
        if hasta == "bronze":
            return bronze

        corte = resolver_fecha_corte(bronze, settings)
        log.info("fecha de corte del análisis: %s", corte)

        log.info("== silver ==")
        silver = construir_silver(bronze, settings)

        resultados = qc.chequeos_silver(
            silver["fct_ventas"],
            silver["fct_inventario_diario"],
            silver["dim_producto"],
            bronze["bronze_sku_mappings"],
            settings,
        )
        for r in resultados:
            log.info("[%s] %s -> %s", "OK " if r.ok else "FALLA", r.nombre, r.detalle)
        qc.exigir(resultados)
        if hasta == "silver":
            _guardar_reporte_calidad(resultados, corte, settings)
            return silver

        log.info("== gold ==")
        gold = construir_gold(silver, corte, settings)
        resultados += qc.chequeos_gold(gold["p3_ventas_mom_canal"], gold["p1_rotacion_top10"])
        qc.exigir(resultados)
        _guardar_reporte_calidad(resultados, corte, settings)

        log.info("reportes en %s", settings.reportes)
        return gold


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Pipeline analítico CaféNorte")
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--capa", choices=CAPAS, default="gold", help="capa final a construir")
    parser.add_argument("--fecha-corte", default=None, help="YYYY-MM-DD; omite la detección automática")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    settings = cargar_settings(args.config)
    if args.fecha_corte:
        settings = Settings(
            **{
                **settings.__dict__,
                "negocio": type(settings.negocio)(
                    **{
                        **settings.negocio.__dict__,
                        "fecha_corte": date.fromisoformat(args.fecha_corte),
                    }
                ),
            }
        )

    ejecutar(settings, hasta=args.capa)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
