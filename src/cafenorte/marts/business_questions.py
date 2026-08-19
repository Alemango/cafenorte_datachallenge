"""Capa gold: las cuatro preguntas de negocio.

Cada función documenta la interpretación que se adoptó donde el enunciado era
ambiguo. Las ventanas de tiempo se anclan a la fecha de corte (última fecha
observada en las fuentes transaccionales), no a `current_date()`, para que los
resultados sean reproducibles dentro de un año.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ..config import Settings


def _inicio_ventana(corte: date, meses: int) -> date:
    """Primer día del mes que está `meses - 1` meses antes del mes de corte.

    Con corte 2026-03-31 y 6 meses: 2025-10-01 .. 2026-03-31.
    """
    total = (corte.year * 12 + corte.month - 1) - (meses - 1)
    return date(total // 12, total % 12 + 1, 1)


# --------------------------------------------------------------------------
# P1. Top 10 SKUs por rotación de inventario en los últimos 6 meses
# --------------------------------------------------------------------------
def rotacion_inventario(
    ventas_costeadas: DataFrame,
    inventario_valuado: DataFrame,
    dim_producto: DataFrame,
    corte: date,
    settings: Settings,
    top_n: int = 10,
) -> DataFrame:
    """Rotación = COGS del periodo / inventario promedio valuado.

    Interpretaciones documentadas:

    * **Definición.** Se usa la definición contable estándar (veces que el
      inventario se renueva), valuada a costo en ambos lados de la razón. Una
      rotación en unidades sería sensible al mix de precios; se reporta de todos
      modos como columna de contraste.
    * **Canal.** Sólo canal físico. Los snapshots del ERP son por tienda física
      y no incluyen el almacén de Shopify; mezclar el COGS de e-commerce con un
      inventario que no lo respalda infla la rotación.
    * **Ventana.** Últimos 6 meses respecto a la fecha de corte. Coincide con la
      cobertura completa de snapshots del ERP (2025-10-01 a 2026-03-31).
    * **Devoluciones.** Restan unidades y COGS (la mercancía regresa al piso).
    * **Denominador.** Promedio de los valores diarios de inventario de la
      cadena, contando sólo lecturas válidas. Se descarta el atajo
      (inicial+final)/2: con 182 snapshots diarios disponibles, usar dos puntos
      desperdicia la serie y es frágil ante un día atípico.
    """
    inicio = _inicio_ventana(corte, settings.negocio.ventanas.rotacion_meses)

    cogs = (
        ventas_costeadas.where(
            (F.col("canal") == "fisico")
            & F.col("es_ingreso")
            & (F.col("fecha") >= F.lit(inicio))
            & (F.col("fecha") <= F.lit(corte))
        )
        .groupBy("producto_key")
        .agg(
            F.sum("costo_total_mxn").alias("cogs_mxn"),
            F.sum("unidades").alias("unidades_vendidas"),
            F.sum("importe_mxn").alias("ingreso_mxn"),
        )
    )

    diario = (
        inventario_valuado.where(
            F.col("lectura_valida")
            & (F.col("fecha") >= F.lit(inicio))
            & (F.col("fecha") <= F.lit(corte))
        )
        .groupBy("producto_key", "fecha")
        .agg(
            F.sum("valor_mxn").alias("valor_dia"),
            F.sum("unidades").alias("unidades_dia"),
        )
    )
    promedio = diario.groupBy("producto_key").agg(
        F.avg("valor_dia").alias("inventario_promedio_mxn"),
        F.avg("unidades_dia").alias("inventario_promedio_unidades"),
        F.countDistinct("fecha").alias("dias_con_lectura"),
    )

    dias_periodo = (corte - inicio).days + 1

    return (
        cogs.join(promedio, on="producto_key", how="inner")
        .join(dim_producto.select("producto_key", "sku_pos", "sku_erp", "nombre", "categoria"), on="producto_key")
        .withColumn(
            "rotacion_veces",
            F.round(F.col("cogs_mxn") / F.col("inventario_promedio_mxn"), 3),
        )
        .withColumn(
            "rotacion_unidades",
            F.round(
                F.col("unidades_vendidas") / F.col("inventario_promedio_unidades"), 3
            ),
        )
        .withColumn(
            "dias_de_inventario",
            F.round(F.lit(dias_periodo) / F.col("rotacion_veces"), 1),
        )
        .withColumn("ventana_inicio", F.lit(inicio))
        .withColumn("ventana_fin", F.lit(corte))
        .orderBy(F.col("rotacion_veces").desc())
        .limit(top_n)
        .select(
            "producto_key",
            "sku_pos",
            "sku_erp",
            "nombre",
            "categoria",
            "unidades_vendidas",
            "cogs_mxn",
            "inventario_promedio_mxn",
            "inventario_promedio_unidades",
            "rotacion_veces",
            "rotacion_unidades",
            "dias_de_inventario",
            "dias_con_lectura",
            "ventana_inicio",
            "ventana_fin",
        )
    )


# --------------------------------------------------------------------------
# P2. Tiendas con quiebres de stock de más de 3 días en el último trimestre
# --------------------------------------------------------------------------
def quiebres_de_stock(
    inventario: DataFrame,
    dim_producto: DataFrame,
    dim_tienda: DataFrame,
    corte: date,
    settings: Settings,
) -> tuple[DataFrame, DataFrame]:
    """Rachas de días consecutivos con stock cero, por tienda y producto.

    Interpretaciones documentadas:

    * **Quiebre** = lectura válida con `unidades = 0`.
    * **"Más de 3 días"** se lee estricto: rachas de **4 días o más**.
    * **Último trimestre** = trimestre calendario de la fecha de corte
      (2026-01-01 a 2026-03-31), no "los últimos 90 días".
    * **Lecturas faltantes.** Un día con "N/A" no cuenta como quiebre y además
      **corta** la racha. Es la lectura conservadora: no se afirma un quiebre
      que no se observó. La alternativa (puentear el hueco) se calcula en
      `quiebres_sensibilidad` para dimensionar cuánto cambia la respuesta.

    Devuelve (detalle_por_racha, resumen_por_tienda).
    """
    trimestre = (corte.month - 1) // 3 + 1
    inicio = date(corte.year, 3 * (trimestre - 1) + 1, 1)

    periodo = inventario.where(
        (F.col("fecha") >= F.lit(inicio)) & (F.col("fecha") <= F.lit(corte))
    )

    ceros = periodo.where(F.col("lectura_valida") & (F.col("unidades") == 0)).withColumn(
        "dia", F.datediff(F.col("fecha"), F.lit(inicio))
    )

    ventana = Window.partitionBy("tienda_id", "producto_key").orderBy("fecha")
    # Gaps & islands: en una racha de días consecutivos, (día - consecutivo) es
    # constante. Como los días sin lectura válida no entran al conjunto, un
    # hueco rompe el grupo automáticamente.
    islas = ceros.withColumn(
        "isla", F.col("dia") - F.row_number().over(ventana)
    )

    detalle = (
        islas.groupBy("tienda_id", "producto_key", "isla")
        .agg(
            F.min("fecha").alias("inicio_quiebre"),
            F.max("fecha").alias("fin_quiebre"),
            F.count("*").alias("dias_consecutivos"),
        )
        .where(F.col("dias_consecutivos") > settings.negocio.quiebre_dias_minimos)
        .drop("isla")
        .join(dim_producto.select("producto_key", "sku_pos", "nombre", "categoria"), on="producto_key")
        .join(dim_tienda.select("tienda_id", "ciudad", "region"), on="tienda_id")
        .withColumn("trimestre", F.lit(f"{corte.year}-Q{trimestre}"))
        .select(
            "tienda_id",
            "ciudad",
            "region",
            "producto_key",
            "sku_pos",
            "nombre",
            "categoria",
            "inicio_quiebre",
            "fin_quiebre",
            "dias_consecutivos",
            "trimestre",
        )
        .orderBy(F.col("dias_consecutivos").desc(), "tienda_id")
    )

    resumen = (
        detalle.groupBy("tienda_id", "ciudad", "region", "trimestre")
        .agg(
            F.count("*").alias("num_quiebres"),
            F.countDistinct("producto_key").alias("skus_afectados"),
            F.sum("dias_consecutivos").alias("dias_quiebre_totales"),
            F.max("dias_consecutivos").alias("racha_mas_larga"),
        )
        .orderBy(F.col("dias_quiebre_totales").desc())
    )
    return detalle, resumen


def dias_en_cero_por_tienda(
    inventario: DataFrame, dim_tienda: DataFrame, corte: date
) -> DataFrame:
    """Vista complementaria: días-SKU con stock cero por tienda en el trimestre.

    La respuesta estricta a P2 (rachas de más de 3 días) devuelve muy pocos
    casos: en este trimestre los ceros son casi siempre de 1 o 2 días. Esa cifra
    sola le diría al dueño "casi no tienes quiebres", lo cual sería engañoso: hay
    5,769 días-SKU con anaquel vacío repartidos en las 40 tiendas. Se entrega
    junto a la respuesta estricta para que la conclusión no dependa de un umbral.
    """
    trimestre = (corte.month - 1) // 3 + 1
    inicio = date(corte.year, 3 * (trimestre - 1) + 1, 1)

    return (
        inventario.where(
            (F.col("fecha") >= F.lit(inicio)) & (F.col("fecha") <= F.lit(corte))
        )
        .groupBy("tienda_id")
        .agg(
            F.sum(F.when(F.col("lectura_valida") & (F.col("unidades") == 0), 1).otherwise(0)).alias("dias_sku_en_cero"),
            F.sum(F.when(F.col("lectura_valida"), 1).otherwise(0)).alias("dias_sku_observados"),
            F.countDistinct(
                F.when(F.col("lectura_valida") & (F.col("unidades") == 0), F.col("producto_key"))
            ).alias("skus_con_al_menos_un_cero"),
        )
        .withColumn(
            "pct_disponibilidad",
            F.round(
                100
                * (1 - F.col("dias_sku_en_cero") / F.col("dias_sku_observados")),
                2,
            ),
        )
        .join(dim_tienda.select("tienda_id", "ciudad", "region"), on="tienda_id")
        .withColumn("trimestre", F.lit(f"{corte.year}-Q{trimestre}"))
        .orderBy(F.col("dias_sku_en_cero").desc())
    )


def quiebres_sensibilidad(
    inventario: DataFrame, corte: date, settings: Settings
) -> DataFrame:
    """Contrasta el criterio conservador contra dos alternativas.

    Sirve para poder decir en la entrevista *cuánto* depende la respuesta del
    supuesto sobre las lecturas faltantes, en vez de sólo afirmar que se eligió
    el criterio conservador.
    """
    trimestre = (corte.month - 1) // 3 + 1
    inicio = date(corte.year, 3 * (trimestre - 1) + 1, 1)
    periodo = inventario.where(
        (F.col("fecha") >= F.lit(inicio)) & (F.col("fecha") <= F.lit(corte))
    ).withColumn("dia", F.datediff(F.col("fecha"), F.lit(inicio)))

    ventana = Window.partitionBy("tienda_id", "producto_key").orderBy("fecha")
    # `orden_valido` numera sólo los días con lectura; permite construir el
    # escenario "puentea" ignorando los huecos sin fusionar rachas separadas
    # por una lectura con stock positivo.
    con_orden = periodo.withColumn(
        "orden_valido",
        F.when(
            F.col("lectura_valida"),
            F.row_number().over(
                Window.partitionBy("tienda_id", "producto_key")
                .orderBy(F.col("lectura_valida").desc(), F.col("fecha"))
            ),
        ),
    )

    escenarios = {
        # Conservador (el que se reporta): N/A no es quiebre y corta la racha.
        "na_corta_racha": (F.col("lectura_valida") & (F.col("unidades") == 0), "dia"),
        # Optimista: N/A se ignora; la racha se puentea sobre el hueco.
        "na_puentea": (F.col("lectura_valida") & (F.col("unidades") == 0), "orden_valido"),
        # Pesimista (el error clásico): N/A se trata como cero.
        "na_como_cero": ((F.col("unidades") == 0) | ~F.col("lectura_valida"), "dia"),
    }

    resultados = []
    for nombre, (condicion, eje) in escenarios.items():
        islas = periodo.join(
            con_orden.select("fecha", "tienda_id", "producto_key", "orden_valido"),
            on=["fecha", "tienda_id", "producto_key"],
            how="left",
        ).where(condicion).withColumn(
            "isla", F.col(eje) - F.row_number().over(ventana)
        )

        agg = (
            islas.groupBy("tienda_id", "producto_key", "isla")
            .agg(F.count("*").alias("dias"))
            .where(F.col("dias") > settings.negocio.quiebre_dias_minimos)
            .agg(
                F.count("*").alias("num_quiebres"),
                F.countDistinct("tienda_id").alias("tiendas_afectadas"),
            )
            .withColumn("escenario", F.lit(nombre))
        )
        resultados.append(agg.select("escenario", "num_quiebres", "tiendas_afectadas"))

    salida = resultados[0]
    for r in resultados[1:]:
        salida = salida.unionByName(r)
    return salida


# --------------------------------------------------------------------------
# P3. Crecimiento MoM de ventas por canal en el último año
# --------------------------------------------------------------------------
def crecimiento_mom_por_canal(
    ventas: DataFrame, corte: date, settings: Settings
) -> DataFrame:
    """Ingreso neto mensual en MXN y variación mes contra mes, por canal.

    Interpretaciones documentadas:

    * **Ventas** = ingreso neto (facturas menos notas de crédito), en MXN.
    * **Último año** = los 12 meses calendario que terminan en el mes de corte
      (2025-04 a 2026-03). Coincide con la cobertura de Shopify; el POS tiene 18
      meses, así que los 6 meses previos existen pero se recortan para que la
      comparación entre canales sea sobre la misma ventana.
    * **Mes de la venta** = fecha local de la tienda. Los timestamps del POS ya
      vienen en hora local (verificado: todas las tiendas venden entre las 07 y
      las 21 h en sus nueve husos), así que no se reinterpretan.
    """
    inicio = _inicio_ventana(corte, settings.negocio.ventanas.mom_meses)

    mensual = (
        ventas.where(
            F.col("es_ingreso")
            & (F.col("fecha") >= F.lit(inicio))
            & (F.col("fecha") <= F.lit(corte))
        )
        .groupBy("canal", F.date_format("fecha", "yyyy-MM").alias("mes"))
        .agg(
            F.round(F.sum("importe_mxn"), 2).alias("ingreso_mxn"),
            F.sum("unidades").alias("unidades"),
            F.countDistinct("documento_id").alias("documentos"),
        )
    )

    ventana = Window.partitionBy("canal").orderBy("mes")
    return (
        mensual.withColumn("ingreso_mes_anterior", F.lag("ingreso_mxn").over(ventana))
        .withColumn(
            "crecimiento_mom_pct",
            F.round(
                100
                * (F.col("ingreso_mxn") - F.col("ingreso_mes_anterior"))
                / F.col("ingreso_mes_anterior"),
                2,
            ),
        )
        .withColumn(
            "ticket_promedio_mxn", F.round(F.col("ingreso_mxn") / F.col("documentos"), 2)
        )
        .orderBy("canal", "mes")
    )


# --------------------------------------------------------------------------
# P4. Productos con margen negativo y en qué tiendas ocurren
# --------------------------------------------------------------------------
def margen_negativo(
    ventas_costeadas: DataFrame,
    dim_producto: DataFrame,
    dim_tienda: DataFrame,
    corte: date,
    settings: Settings,
) -> tuple[DataFrame, DataFrame]:
    """Margen bruto por producto y por producto x tienda, filtrando lo negativo.

    Interpretaciones documentadas:

    * **Margen bruto** = ingreso neto MXN − (unidades netas x costo vigente ese
      día). No incluye gastos de operación ni flete: el ERP sólo publica costo
      de proveedor.
    * **Ventana** = últimos 12 meses (misma que P3), para que la respuesta sea
      accionable y no arrastre precios de hace año y medio.
    * **"En qué tiendas"** incluye la pseudo-tienda ONLINE: el canal digital
      vende los mismos SKUs a otro precio y su margen puede diferir del físico.
    * Un producto puede tener margen agregado positivo y aun así perder dinero
      en tiendas concretas, por eso se entregan los dos niveles.
    """
    inicio = _inicio_ventana(corte, settings.negocio.ventanas.margen_meses)

    base = ventas_costeadas.where(
        F.col("es_ingreso")
        & (F.col("fecha") >= F.lit(inicio))
        & (F.col("fecha") <= F.lit(corte))
    )

    agg_cols = [
        F.round(F.sum("importe_mxn"), 2).alias("ingreso_mxn"),
        F.round(F.sum("costo_total_mxn"), 2).alias("cogs_mxn"),
        F.round(F.sum("margen_mxn"), 2).alias("margen_mxn"),
        F.sum("unidades").alias("unidades"),
    ]

    por_producto = (
        base.groupBy("producto_key")
        .agg(*agg_cols)
        .withColumn(
            "margen_pct", F.round(100 * F.col("margen_mxn") / F.col("ingreso_mxn"), 2)
        )
        .join(dim_producto.select("producto_key", "sku_pos", "sku_erp", "nombre", "categoria"), on="producto_key")
        .where(F.col("margen_mxn") < 0)
        .orderBy("margen_mxn")
        .select(
            "producto_key", "sku_pos", "sku_erp", "nombre", "categoria",
            "unidades", "ingreso_mxn", "cogs_mxn", "margen_mxn", "margen_pct",
        )
    )

    productos_negativos = [f.producto_key for f in por_producto.select("producto_key").collect()]

    por_tienda = (
        base.groupBy("producto_key", "tienda_id", "canal")
        .agg(*agg_cols)
        .withColumn(
            "margen_pct", F.round(100 * F.col("margen_mxn") / F.col("ingreso_mxn"), 2)
        )
        .where(
            (F.col("margen_mxn") < 0)
            | F.col("producto_key").isin(productos_negativos)
        )
        .join(dim_producto.select("producto_key", "sku_pos", "nombre", "categoria"), on="producto_key")
        .join(dim_tienda.select("tienda_id", "ciudad", "region"), on="tienda_id")
        .orderBy("margen_mxn")
        .select(
            "producto_key", "sku_pos", "nombre", "categoria",
            "tienda_id", "ciudad", "region", "canal",
            "unidades", "ingreso_mxn", "cogs_mxn", "margen_mxn", "margen_pct",
        )
    )
    return por_producto, por_tienda
