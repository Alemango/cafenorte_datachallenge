"""Chequeos de calidad que corren dentro del pipeline.

Filosofía: el red flag del reto es "corre sin error pero produce números
incorrectos". Estos chequeos existen para convertir errores silenciosos en
fallas ruidosas. Cada uno devuelve un `Resultado` y el orquestador decide si
detiene el pipeline (severidad `error`) o sólo lo anota en el reporte
(`advertencia`).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..config import Settings
from ..transform.product_keys import validar_mapeo_explicito


@dataclass
class Resultado:
    nombre: str
    ok: bool
    severidad: str  # "error" | "advertencia"
    detalle: str
    metricas: dict[str, Any] = field(default_factory=dict)


class FallaDeCalidad(RuntimeError):
    pass


def _pct(parte: int, total: int) -> float:
    return 0.0 if total == 0 else round(100 * parte / total, 4)


def chequeos_silver(
    ventas: DataFrame,
    inventario: DataFrame,
    dim_prod: DataFrame,
    mapeos: DataFrame,
    settings: Settings,
) -> list[Resultado]:
    resultados: list[Resultado] = []
    keys_validas = {f.producto_key for f in dim_prod.select("producto_key").collect()}

    # 1. La convención numérica no contradice el mapeo oficial del ERP.
    discrepancias = validar_mapeo_explicito(mapeos)
    resultados.append(
        Resultado(
            "mapeo_sku_consistente",
            ok=not discrepancias,
            severidad="error",
            detalle=(
                "La llave numérica coincide con sku_mappings en todos los renglones"
                if not discrepancias
                else f"{len(discrepancias)} renglones donde el mapeo del ERP contradice el consecutivo"
            ),
            metricas={"discrepancias": len(discrepancias)},
        )
    )

    # 2. Toda línea de venta aterriza en el catálogo de producto.
    total_ventas = ventas.count()
    huerfanas = ventas.where(~F.col("producto_key").isin(list(keys_validas))).count()
    pct = _pct(huerfanas, total_ventas)
    resultados.append(
        Resultado(
            "ventas_con_producto",
            ok=pct <= settings.calidad.max_pct_ventas_sin_producto,
            severidad="error",
            detalle=f"{huerfanas} de {total_ventas} líneas de venta sin producto en catálogo ({pct}%)",
            metricas={"lineas_huerfanas": huerfanas, "pct": pct},
        )
    )

    # 3. Ninguna venta se quedó sin convertir a MXN.
    sin_fx = ventas.where(F.col("importe_mxn").isNull()).count()
    pct_fx = _pct(sin_fx, total_ventas)
    resultados.append(
        Resultado(
            "ventas_convertidas_a_mxn",
            ok=pct_fx <= settings.calidad.max_pct_ecommerce_sin_fx,
            severidad="error",
            detalle=f"{sin_fx} líneas sin tipo de cambio aplicable ({pct_fx}%)",
            metricas={"lineas_sin_fx": sin_fx, "pct": pct_fx},
        )
    )

    # 4. El importe conserva el signo del tipo de comprobante.
    signo_malo = ventas.where(
        (F.col("unidades") > 0) & (F.col("importe_mxn") < 0)
        | (F.col("unidades") < 0) & (F.col("importe_mxn") > 0)
    ).count()
    resultados.append(
        Resultado(
            "signo_coherente",
            ok=signo_malo == 0,
            severidad="error",
            detalle=f"{signo_malo} líneas donde unidades e importe tienen signos opuestos",
            metricas={"lineas": signo_malo},
        )
    )

    # 5. Volumen de lecturas de inventario perdidas (esperado ~1.9%).
    total_inv = inventario.count()
    invalidas = inventario.where(~F.col("lectura_valida")).count()
    pct_inv = _pct(invalidas, total_inv)
    resultados.append(
        Resultado(
            "lecturas_inventario_validas",
            ok=pct_inv <= settings.calidad.max_pct_lecturas_invalidas,
            severidad="advertencia",
            detalle=f"{invalidas} de {total_inv} snapshots sin lectura ({pct_inv}%)",
            metricas={"lecturas_invalidas": invalidas, "pct": pct_inv},
        )
    )

    # 6. Grano del snapshot: una lectura por fecha/tienda/producto.
    dups = (
        inventario.groupBy("fecha", "tienda_id", "producto_key")
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    resultados.append(
        Resultado(
            "grano_inventario_unico",
            ok=dups == 0,
            severidad="error",
            detalle=f"{dups} combinaciones fecha/tienda/producto duplicadas",
            metricas={"duplicados": dups},
        )
    )

    # 7. Inventario no negativo.
    negativos = inventario.where(F.col("unidades") < 0).count()
    resultados.append(
        Resultado(
            "inventario_no_negativo",
            ok=negativos == 0,
            severidad="error",
            detalle=f"{negativos} lecturas de inventario negativas",
            metricas={"negativos": negativos},
        )
    )

    return resultados


def chequeos_gold(mom: DataFrame, rotacion: DataFrame) -> list[Resultado]:
    resultados: list[Resultado] = []

    meses_incompletos = mom.where(F.col("ingreso_mxn") <= 0).count()
    resultados.append(
        Resultado(
            "ingreso_mensual_positivo",
            ok=meses_incompletos == 0,
            severidad="error",
            detalle=f"{meses_incompletos} meses-canal con ingreso no positivo",
            metricas={"meses": meses_incompletos},
        )
    )

    rot_invalida = rotacion.where(
        F.col("rotacion_veces").isNull() | (F.col("rotacion_veces") <= 0)
    ).count()
    resultados.append(
        Resultado(
            "rotacion_valida",
            ok=rot_invalida == 0,
            severidad="error",
            detalle=f"{rot_invalida} SKUs con rotación nula o no positiva en el top",
            metricas={"skus": rot_invalida},
        )
    )
    return resultados


def exigir(resultados: list[Resultado]) -> None:
    """Detiene el pipeline si algún chequeo de severidad `error` falló."""
    fallas = [r for r in resultados if not r.ok and r.severidad == "error"]
    if fallas:
        detalle = "\n".join(f"  - {r.nombre}: {r.detalle}" for r in fallas)
        raise FallaDeCalidad(f"Chequeos de calidad fallidos:\n{detalle}")
