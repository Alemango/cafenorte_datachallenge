"""Capa silver: hechos conformes de ventas e inventario."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..config import Settings
from .product_keys import key_desde_handle, key_desde_sku_pos

# Valores que el ERP legacy usa cuando no hubo lectura de inventario.
COLUMNAS_VENTA = [
    "documento_id",
    "canal",
    "fecha",
    "fecha_hora_local",
    "tienda_id",
    "producto_key",
    "tipo_documento",
    "es_ingreso",
    "unidades",
    "importe_mxn",
    "moneda_origen",
    "importe_origen",
]


def _fx_para_join(dim_fx: DataFrame) -> DataFrame:
    """Renombra la dimensión de tipo de cambio para evitar colisiones de nombre.

    `sales.csv` ya trae una columna `moneda`; sin el prefijo, el join produce
    una referencia ambigua.
    """
    return dim_fx.select(
        F.col("fecha").alias("fx_fecha"),
        F.col("moneda").alias("fx_moneda"),
        F.col("tipo_cambio_mxn"),
    )


def _clasificar_comprobante(col, settings: Settings):
    """CFDI 4.0: sólo I (ingreso) y E (egreso/nota de crédito) mueven ingresos.

    P (complemento de pago) es la liquidación de una factura ya registrada:
    sumarlo duplica ingreso. T (traslado) es movimiento de mercancía entre
    almacenes y N (nómina) es ruido que el POS dejó pasar. Sumar los cinco tipos
    —lo que hace `SUM(monto)` a secas— infla el ingreso 9.1% en este dataset.
    """
    neg = settings.negocio
    return (
        F.when(col.isin(list(neg.comprobantes_ingreso)), F.lit(1))
        .when(col.isin(list(neg.comprobantes_devolucion)), F.lit(-1))
        .otherwise(F.lit(0))
    )


def fct_ventas_pos(
    bronze_ventas: DataFrame, dim_fx: DataFrame, settings: Settings
) -> DataFrame:
    """Normaliza el POS: llave de producto, signo contable y moneda de reporte."""
    base = (
        bronze_ventas.withColumn("fecha_hora_local", F.to_timestamp("fecha_hora"))
        .withColumn("fecha", F.to_date("fecha_hora_local"))
        .withColumn("producto_key", key_desde_sku_pos(F.col("sku")))
        .withColumn("moneda_origen", F.upper(F.trim("moneda")))
        .withColumn("signo", _clasificar_comprobante(F.col("tipo_comprobante"), settings))
        # Deduplicación defensiva: el POS podría reenviar un ticket.
        .dropDuplicates(["venta_id"])
    )

    fx = _fx_para_join(dim_fx)
    return (
        base.join(
            fx,
            (base["fecha"] == fx["fx_fecha"])
            & (base["moneda_origen"] == fx["fx_moneda"]),
            "left",
        )
        .withColumn("tipo_cambio_mxn", F.coalesce(F.col("tipo_cambio_mxn"), F.lit(1.0)))
        .select(
            F.col("venta_id").alias("documento_id"),
            F.lit("fisico").alias("canal"),
            F.col("fecha"),
            F.col("fecha_hora_local"),
            F.col("tienda_id"),
            F.col("producto_key"),
            F.col("tipo_comprobante").alias("tipo_documento"),
            (F.col("signo") != 0).alias("es_ingreso"),
            (F.col("signo") * F.col("cantidad")).alias("unidades"),
            (F.col("signo") * F.col("monto") * F.col("tipo_cambio_mxn")).alias("importe_mxn"),
            F.col("moneda_origen"),
            (F.col("signo") * F.col("monto")).alias("importe_origen"),
        )
    )


def fct_ventas_ecommerce(
    bronze_ecommerce: DataFrame, dim_fx: DataFrame, settings: Settings
) -> DataFrame:
    """Normaliza Shopify: llave de producto y conversión a MXN por fecha.

    `amount` es el importe total de la línea en la moneda de la orden. Se
    convierte con el tipo de cambio **del día de la orden**, no con el último
    disponible: una tasa fija sesga el ingreso del canal digital y, con USD
    moviéndose entre 17.13 y 18.83 en el periodo, eso es hasta 9% sobre el 30%
    de órdenes en moneda extranjera.
    """
    base = (
        bronze_ecommerce.withColumn("fecha_hora_local", F.to_timestamp("fecha"))
        .withColumn("fecha_orden", F.to_date("fecha_hora_local"))
        .withColumn("producto_key", key_desde_handle(F.col("product_handle")))
        .withColumn("moneda_origen", F.upper(F.trim("currency")))
        .dropDuplicates(["order_id"])
    )

    fx = _fx_para_join(dim_fx)
    return (
        base.join(
            fx,
            (base["fecha_orden"] == fx["fx_fecha"])
            & (base["moneda_origen"] == fx["fx_moneda"]),
            "left",
        )
        .select(
            F.col("order_id").alias("documento_id"),
            F.lit("ecommerce").alias("canal"),
            F.col("fecha_orden").alias("fecha"),
            F.col("fecha_hora_local"),
            F.lit(settings.negocio.tienda_ecommerce).alias("tienda_id"),
            F.col("producto_key"),
            F.lit("SHOPIFY").alias("tipo_documento"),
            F.lit(True).alias("es_ingreso"),
            F.col("cantidad").cast("int").alias("unidades"),
            (F.col("amount") * F.col("tipo_cambio_mxn")).alias("importe_mxn"),
            F.col("moneda_origen"),
            F.col("amount").alias("importe_origen"),
        )
    )


def fct_ventas(pos: DataFrame, ecommerce: DataFrame) -> DataFrame:
    """Hecho unificado de ventas de los dos canales."""
    return pos.select(COLUMNAS_VENTA).unionByName(ecommerce.select(COLUMNAS_VENTA))


def fct_inventario_diario(
    bronze_snapshots: DataFrame, settings: Settings
) -> DataFrame:
    """Snapshots diarios de inventario con las lecturas faltantes marcadas.

    El ERP escribe la cadena "N/A" (1.9% de los renglones) cuando no hubo
    lectura. Castearlo a entero lo convierte en NULL, y `coalesce(..., 0)` —el
    reflejo automático— inventaría quiebres de stock que nunca ocurrieron.
    Aquí se conserva la distinción entre "hay cero piezas" y "no sabemos".
    """
    centinelas = [c.upper() for c in settings.negocio.centinela_inventario_nulo]
    crudo = F.upper(F.trim(F.col("cantidad_en_stock")))
    es_centinela = crudo.isin(centinelas) | crudo.isNull()

    return (
        bronze_snapshots.select(
            F.to_date("fecha").alias("fecha"),
            F.col("tienda_id"),
            F.col("sku_erp"),
            F.col("cantidad_en_stock").alias("_crudo"),
            es_centinela.alias("_es_centinela"),
        )
        .withColumn(
            "producto_key",
            F.regexp_extract(F.col("sku_erp"), r"^ERP-[A-Z]+-[A-Z]+-0*([0-9]+)-[A-Z]$", 1).cast("int"),
        )
        .withColumn(
            "unidades",
            F.when(F.col("_es_centinela"), F.lit(None).cast("int")).otherwise(
                F.col("_crudo").cast("int")
            ),
        )
        .withColumn("lectura_valida", F.col("unidades").isNotNull())
        .select("fecha", "tienda_id", "producto_key", "unidades", "lectura_valida")
        .dropDuplicates(["fecha", "tienda_id", "producto_key"])
    )


def inventario_valuado(
    inventario: DataFrame, dim_costo: DataFrame
) -> DataFrame:
    """Agrega el costo vigente del día a cada lectura de inventario."""
    return (
        inventario.join(dim_costo, on="producto_key", how="left")
        .where(
            F.col("valido_desde").isNull()
            | (
                (F.col("fecha") >= F.col("valido_desde"))
                & (F.col("fecha") <= F.col("valido_hasta"))
            )
        )
        .withColumn("valor_mxn", F.col("unidades") * F.col("costo_mxn"))
        .select(
            "fecha",
            "tienda_id",
            "producto_key",
            "unidades",
            "lectura_valida",
            "costo_mxn",
            "valor_mxn",
        )
    )


def ventas_costeadas(ventas: DataFrame, dim_costo: DataFrame) -> DataFrame:
    """Join "as-of" del costo vigente a la fecha de cada venta.

    Se usa un rango de vigencia (no el último costo) porque el ERP cambia de
    proveedor varias veces en el periodo y el margen histórico debe calcularse
    con el costo que estaba vigente ese día.
    """
    return (
        ventas.join(dim_costo, on="producto_key", how="left")
        .where(
            F.col("valido_desde").isNull()
            | (
                (F.col("fecha") >= F.col("valido_desde"))
                & (F.col("fecha") <= F.col("valido_hasta"))
            )
        )
        .withColumn("costo_total_mxn", F.col("unidades") * F.col("costo_mxn"))
        .withColumn("margen_mxn", F.col("importe_mxn") - F.col("costo_total_mxn"))
        .drop("valido_desde", "valido_hasta", "proveedor")
    )
