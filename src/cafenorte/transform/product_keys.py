"""Conciliación de identificadores de producto entre POS, ERP y Shopify.

Contexto
--------
Los tres sistemas nombran al mismo producto de forma distinta:

    POS      CN-00018
    ERP      ERP-PROV-MX-018-A
    Shopify  tradicional-cafe-molido-018

El ERP publica una tabla `sku_mappings`, pero está incompleta:

* 5 SKUs del POS que sí tienen ventas no aparecen en la tabla,
* 5 renglones traen `sku_erp` nulo,
* 6 handles de Shopify con órdenes no aparecen en la tabla.

Los tres formatos comparten un consecutivo numérico. Se validó que en los 65
renglones del mapeo explícito el consecutivo coincide en los tres sistemas sin
una sola excepción, y que el catálogo del ERP tiene exactamente 70 productos con
consecutivos 1..70 sin duplicados.

Decisión
--------
`producto_key` (el consecutivo, entero) es la llave conforme. El mapeo explícito
del ERP se usa como **oráculo de validación**, no como fuente de la llave: si
alguna vez deja de coincidir, el pipeline falla en `validar_mapeo_explicito`
en vez de producir números silenciosamente incorrectos.

Alternativa descartada: `join` directo contra `sku_mappings`. Habría descartado
~6% de las líneas de venta —incluyendo CN-00001, uno de los tres productos con
margen negativo—, y el pipeline habría "corrido sin error" con la respuesta 4
equivocada.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# El consecutivo va al final en POS/Shopify y en el 4º segmento del ERP.
PATRON_POS = r"^CN-0*([0-9]+)$"
PATRON_ERP = r"^ERP-[A-Z]+-[A-Z]+-0*([0-9]+)-[A-Z]$"
PATRON_HANDLE = r"-0*([0-9]+)$"


def _extraer(col: Column, patron: str) -> Column:
    extraido = F.regexp_extract(F.trim(col), patron, 1)
    return F.when(extraido == "", F.lit(None)).otherwise(extraido.cast("int"))


def key_desde_sku_pos(col: Column) -> Column:
    """`CN-00018` -> 18."""
    return _extraer(col, PATRON_POS)


def key_desde_sku_erp(col: Column) -> Column:
    """`ERP-PROV-MX-018-A` -> 18."""
    return _extraer(col, PATRON_ERP)


def key_desde_handle(col: Column) -> Column:
    """`tradicional-cafe-molido-018` -> 18."""
    return _extraer(col, PATRON_HANDLE)


def validar_mapeo_explicito(mapeos: DataFrame) -> list[dict]:
    """Devuelve los renglones donde el mapeo del ERP contradice el consecutivo.

    Una lista vacía significa que la convención numérica es consistente con la
    tabla oficial y por lo tanto es seguro usarla como llave conforme.
    """
    discrepancias = (
        mapeos.withColumn("key_pos", key_desde_sku_pos(F.col("sku_pos")))
        .withColumn("key_erp", key_desde_sku_erp(F.col("sku_erp")))
        .withColumn("key_handle", key_desde_handle(F.col("handle")))
        .where(
            (F.col("key_pos").isNull())
            | (F.col("key_erp").isNotNull() & (F.col("key_erp") != F.col("key_pos")))
            | (
                F.col("key_handle").isNotNull()
                & (F.col("key_handle") != F.col("key_pos"))
            )
        )
        .select("sku_pos", "sku_erp", "handle", "key_pos", "key_erp", "key_handle")
    )
    return [fila.asDict() for fila in discrepancias.collect()]
