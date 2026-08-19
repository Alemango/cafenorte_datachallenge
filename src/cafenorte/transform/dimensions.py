"""Capa silver: dimensiones conformes."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ..config import Settings
from .product_keys import key_desde_sku_erp, key_desde_sku_pos

# Fecha centinela de fin de vigencia (SCD2 abierto).
FIN_VIGENCIA_ABIERTA = "9999-12-31"


def dim_tienda(bronze_tiendas: DataFrame, settings: Settings) -> DataFrame:
    """40 tiendas físicas + la pseudo-tienda del canal e-commerce.

    Shopify no tiene tienda; para poder responder "en qué tiendas ocurre" con
    un solo modelo, el canal digital se representa como una tienda sintética.
    """
    fisicas = bronze_tiendas.select(
        F.col("tienda_id"),
        F.col("ciudad"),
        F.col("region"),
        F.col("timezone"),
        F.lit("fisico").alias("canal"),
    ).dropDuplicates(["tienda_id"])

    online = fisicas.sparkSession.createDataFrame(
        [
            (
                settings.negocio.tienda_ecommerce,
                "N/A",
                "digital",
                "America/Mexico_City",
                "ecommerce",
            )
        ],
        schema=fisicas.schema,
    )
    return fisicas.unionByName(online)


def dim_producto(bronze_catalogo: DataFrame, bronze_mapeos: DataFrame) -> DataFrame:
    """Maestro de producto con la llave conforme y los alias de cada sistema.

    El catálogo del ERP es la fuente de verdad del universo de productos
    (70 SKUs). Los alias de POS y Shopify se pegan por `producto_key`, no por
    el mapeo explícito, para no perder los SKUs que el ERP dejó sin mapear.
    """
    catalogo = (
        bronze_catalogo.select(
            key_desde_sku_erp(F.col("sku_erp")).alias("producto_key"),
            F.col("sku_erp"),
            F.col("nombre"),
            F.col("categoria"),
        )
        .where(F.col("producto_key").isNotNull())
        .dropDuplicates(["producto_key"])
    )

    alias = (
        bronze_mapeos.select(
            key_desde_sku_pos(F.col("sku_pos")).alias("producto_key"),
            F.col("sku_pos").alias("sku_pos_mapeado"),
            F.col("handle"),
        )
        .where(F.col("producto_key").isNotNull())
        .dropDuplicates(["producto_key"])
    )

    return (
        catalogo.join(alias, on="producto_key", how="left")
        .withColumn("tiene_mapeo_explicito", F.col("sku_pos_mapeado").isNotNull())
        .withColumn(
            "sku_pos",
            F.coalesce(
                F.col("sku_pos_mapeado"),
                F.concat(F.lit("CN-"), F.lpad(F.col("producto_key").cast("string"), 5, "0")),
            ),
        )
        .select(
            "producto_key",
            "sku_erp",
            "sku_pos",
            "handle",
            "nombre",
            "categoria",
            "tiene_mapeo_explicito",
        )
    )


def dim_costo_vigencia(bronze_catalogo: DataFrame) -> DataFrame:
    """SCD2 de costos a partir de `cost_history`.

    El ERP entrega una lista de (fecha_vigencia, costo). Se cierra cada
    intervalo con el día anterior al siguiente cambio para poder hacer un join
    "as-of" por fecha de venta.

    Usar el último costo conocido para todo el histórico —el atajo obvio—
    reescribiría el margen de los meses previos a cada cambio de proveedor.
    """
    explotado = bronze_catalogo.select(
        key_desde_sku_erp(F.col("sku_erp")).alias("producto_key"),
        F.explode("cost_history").alias("c"),
    ).select(
        "producto_key",
        F.to_date("c.fecha_vigencia").alias("valido_desde"),
        F.col("c.costo_mxn").cast("double").alias("costo_mxn"),
        F.col("c.proveedor").alias("proveedor"),
    )

    ventana = Window.partitionBy("producto_key").orderBy("valido_desde")
    return (
        explotado.withColumn(
            "siguiente_vigencia", F.lead("valido_desde").over(ventana)
        )
        .withColumn(
            "valido_hasta",
            F.coalesce(
                F.date_sub(F.col("siguiente_vigencia"), 1),
                F.to_date(F.lit(FIN_VIGENCIA_ABIERTA)),
            ),
        )
        .drop("siguiente_vigencia")
    )


def dim_tipo_cambio(bronze_fx: DataFrame) -> DataFrame:
    """Tipos de cambio diarios + la identidad MXN->MXN.

    Se agrega la fila MXN=1 para que el hecho de ventas pueda hacer un único
    join en vez de un `CASE` por moneda, y para que una moneda nueva en el
    origen se detecte como fila sin tipo de cambio en vez de pasar sin convertir.
    """
    fx = bronze_fx.select(
        F.to_date("fecha").alias("fecha"),
        F.upper(F.trim("currency")).alias("moneda"),
        F.col("rate_to_mxn").cast("double").alias("tipo_cambio_mxn"),
    ).dropDuplicates(["fecha", "moneda"])

    mxn = (
        fx.select("fecha")
        .distinct()
        .withColumn("moneda", F.lit("MXN"))
        .withColumn("tipo_cambio_mxn", F.lit(1.0))
    )
    return fx.unionByName(mxn)


def dim_calendario(fecha_min, fecha_max, spark) -> DataFrame:
    """Calendario diario continuo entre dos fechas (inclusive)."""
    return (
        spark.sql(
            f"SELECT explode(sequence(to_date('{fecha_min}'), to_date('{fecha_max}'), interval 1 day)) AS fecha"
        )
        .withColumn("anio", F.year("fecha"))
        .withColumn("mes", F.date_format("fecha", "yyyy-MM"))
        .withColumn("trimestre", F.concat(F.year("fecha"), F.lit("-Q"), F.quarter("fecha")))
    )
