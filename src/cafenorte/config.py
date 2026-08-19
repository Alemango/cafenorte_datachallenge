"""Carga y validación de la configuración del pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import yaml

RAIZ_PROYECTO = Path(__file__).resolve().parents[2]
CONFIG_POR_DEFECTO = RAIZ_PROYECTO / "conf" / "settings.yaml"


@dataclass(frozen=True)
class Ventanas:
    rotacion_meses: int
    quiebres_meses: int
    mom_meses: int
    margen_meses: int


@dataclass(frozen=True)
class Negocio:
    fecha_corte: date | None
    comprobantes_ingreso: tuple[str, ...]
    comprobantes_devolucion: tuple[str, ...]
    comprobantes_excluidos: tuple[str, ...]
    centinela_inventario_nulo: tuple[str, ...]
    ventanas: Ventanas
    quiebre_dias_minimos: int
    tienda_ecommerce: str
    moneda_reporte: str


@dataclass(frozen=True)
class Calidad:
    max_pct_ventas_sin_producto: float
    max_pct_lecturas_invalidas: float
    max_pct_ecommerce_sin_fx: float


@dataclass(frozen=True)
class Settings:
    raiz: Path
    raw: Path
    lake: Path
    reportes: Path
    archivos: dict[str, str]
    spark: dict[str, Any]
    negocio: Negocio
    calidad: Calidad
    _crudo: dict[str, Any] = field(repr=False, default_factory=dict)

    # --- helpers de rutas -------------------------------------------------
    def ruta_raw(self, clave: str) -> Path:
        return self.raw / self.archivos[clave]

    def ruta_capa(self, capa: str, tabla: str) -> Path:
        return self.lake / capa / tabla


def _resolver(raiz: Path, valor: str) -> Path:
    ruta = Path(valor)
    return ruta if ruta.is_absolute() else raiz / ruta


def cargar_settings(
    ruta_config: Path | str | None = None, raiz: Path | None = None
) -> Settings:
    """Lee `conf/settings.yaml` y lo convierte en objetos tipados.

    Se separa de la lógica de negocio para poder inyectar configuraciones
    alternativas en los tests sin tocar el YAML de producción.
    """
    ruta_config = Path(ruta_config) if ruta_config else CONFIG_POR_DEFECTO
    raiz = raiz or RAIZ_PROYECTO
    crudo = yaml.safe_load(ruta_config.read_text(encoding="utf-8"))

    neg = crudo["negocio"]
    corte = neg.get("fecha_corte", "auto")
    fecha_corte = None if corte in (None, "auto") else date.fromisoformat(str(corte))

    return Settings(
        raiz=raiz,
        raw=_resolver(raiz, crudo["rutas"]["raw"]),
        lake=_resolver(raiz, crudo["rutas"]["lake"]),
        reportes=_resolver(raiz, crudo["rutas"]["reportes"]),
        archivos=crudo["archivos"],
        spark=crudo["spark"],
        negocio=Negocio(
            fecha_corte=fecha_corte,
            comprobantes_ingreso=tuple(neg["comprobantes_ingreso"]),
            comprobantes_devolucion=tuple(neg["comprobantes_devolucion"]),
            comprobantes_excluidos=tuple(neg["comprobantes_excluidos"]),
            centinela_inventario_nulo=tuple(neg["centinela_inventario_nulo"]),
            ventanas=Ventanas(**neg["ventanas"]),
            quiebre_dias_minimos=int(neg["quiebre_dias_minimos"]),
            tienda_ecommerce=neg["tienda_ecommerce"],
            moneda_reporte=neg["moneda_reporte"],
        ),
        calidad=Calidad(**crudo["calidad"]),
        _crudo=crudo,
    )
