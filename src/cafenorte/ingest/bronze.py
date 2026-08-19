"""Capa bronze: lectura de las fuentes tal como llegan, con esquema explícito.

Reglas de la capa:

* esquema declarado a mano (nunca `inferSchema`) para que un cambio en el
  origen falle en la frontera y no a mitad del cálculo de negocio;
* sin reglas de negocio: no se filtra, no se deduplica, no se convierte moneda;
* se agrega `_ingestado_en` y `_fuente` para trazabilidad.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from ..config import Settings

ESQUEMA_VENTAS = StructType(
    [
        StructField("venta_id", StringType(), False),
        StructField("fecha_hora", StringType(), False),
        StructField("tienda_id", StringType(), False),
        StructField("sku", StringType(), False),
        StructField("cantidad", IntegerType(), False),
        StructField("monto", DoubleType(), False),
        StructField("moneda", StringType(), False),
        StructField("tipo_comprobante", StringType(), False),
    ]
)

ESQUEMA_FX = StructType(
    [
        StructField("fecha", StringType(), False),
        StructField("currency", StringType(), False),
        StructField("rate_to_mxn", DoubleType(), False),
    ]
)

# `cantidad_en_stock` se lee como string a propósito: el ERP legacy escribe
# "N/A" en las lecturas faltantes y un cast temprano las volvería NULL sin
# dejar rastro de cuántas eran.
ESQUEMA_INVENTARIO = StructType(
    [
        StructField(
            "metadata",
            StructType(
                [
                    StructField("generado", StringType()),
                    StructField("fuente", StringType()),
                    StructField("descripcion", StringType()),
                ]
            ),
        ),
        StructField(
            "tiendas_info",
            ArrayType(
                StructType(
                    [
                        StructField("tienda_id", StringType()),
                        StructField("ciudad", StringType()),
                        StructField("region", StringType()),
                        StructField("timezone", StringType()),
                    ]
                )
            ),
        ),
        StructField(
            "sku_mappings",
            ArrayType(
                StructType(
                    [
                        StructField("sku_pos", StringType()),
                        StructField("sku_erp", StringType()),
                        StructField("handle", StringType()),
                    ]
                )
            ),
        ),
        StructField(
            "catalogo",
            StructType(
                [
                    StructField(
                        "productos",
                        ArrayType(
                            StructType(
                                [
                                    StructField("sku_erp", StringType()),
                                    StructField("nombre", StringType()),
                                    StructField("categoria", StringType()),
                                    StructField(
                                        "cost_history",
                                        ArrayType(
                                            StructType(
                                                [
                                                    StructField(
                                                        "fecha_vigencia", StringType()
                                                    ),
                                                    StructField(
                                                        "costo_mxn", DoubleType()
                                                    ),
                                                    StructField(
                                                        "proveedor", StringType()
                                                    ),
                                                ]
                                            )
                                        ),
                                    ),
                                ]
                            )
                        ),
                    )
                ]
            ),
        ),
        StructField(
            "snapshots",
            ArrayType(
                StructType(
                    [
                        StructField("fecha", StringType()),
                        StructField("tienda_id", StringType()),
                        StructField("sku_erp", StringType()),
                        StructField("cantidad_en_stock", StringType()),
                    ]
                )
            ),
        ),
    ]
)


def _sellar(df: DataFrame, fuente: str) -> DataFrame:
    return df.withColumn("_fuente", F.lit(fuente)).withColumn(
        "_ingestado_en", F.current_timestamp()
    )


def leer_ventas_pos(spark: SparkSession, settings: Settings) -> DataFrame:
    ruta = settings.ruta_raw("ventas_pos")
    df = (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .schema(ESQUEMA_VENTAS)
        .csv(str(ruta))
    )
    return _sellar(df, "pos:sales.csv")


def leer_ecommerce(spark: SparkSession, settings: Settings) -> DataFrame:
    ruta = settings.ruta_raw("ordenes_ecommerce")
    # Se descartan aquí las columnas de PII (nombre, email, RFC, dirección):
    # no hacen falta para ninguna de las 4 preguntas y no deben viajar al lake.
    df = spark.read.parquet(str(ruta)).select(
        "order_id", "fecha", "product_handle", "cantidad", "amount", "currency"
    )
    return _sellar(df, "shopify:ecommerce_orders.parquet")


def leer_tipos_de_cambio(spark: SparkSession, settings: Settings) -> DataFrame:
    ruta = settings.ruta_raw("tipos_de_cambio")
    df = (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .schema(ESQUEMA_FX)
        .csv(str(ruta))
    )
    return _sellar(df, "fx:exchange_rates.csv")


def _documento_inventario(spark: SparkSession, ruta: Path) -> DataFrame:
    return (
        spark.read.option("multiLine", True)
        .schema(ESQUEMA_INVENTARIO)
        .json(str(ruta))
        .cache()
    )


def leer_inventario_erp(
    spark: SparkSession, settings: Settings
) -> dict[str, DataFrame]:
    """Aplana el JSON anidado del ERP en cuatro tablas bronze independientes."""
    doc = _documento_inventario(spark, settings.ruta_raw("inventario_erp"))

    tiendas = _sellar(doc.select(F.explode("tiendas_info").alias("t")).select("t.*"), "erp:tiendas_info")
    mapeos = _sellar(doc.select(F.explode("sku_mappings").alias("m")).select("m.*"), "erp:sku_mappings")
    catalogo = _sellar(
        doc.select(F.explode("catalogo.productos").alias("p")).select("p.*"),
        "erp:catalogo",
    )
    snapshots = _sellar(
        doc.select(F.explode("snapshots").alias("s")).select("s.*"), "erp:snapshots"
    )

    return {
        "bronze_tiendas": tiendas,
        "bronze_sku_mappings": mapeos,
        "bronze_catalogo": catalogo,
        "bronze_snapshots": snapshots,
    }
