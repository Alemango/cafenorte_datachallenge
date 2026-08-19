#!/usr/bin/env python3
"""Genera `docs/propuesta_tecnica.pdf` a partir del Markdown y el diagrama.

Se mantiene en el repo para que la propuesta que recibe el cliente se pueda
regenerar desde la fuente en vez de mantener dos versiones a mano.

Requiere `markdown` (en requirements.txt) y un Chrome/Chromium headless.

    python scripts/build_propuesta_pdf.py [--chrome /ruta/a/chrome]
"""

from __future__ import annotations

import argparse
import base64
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import markdown

RAIZ = Path(__file__).resolve().parents[1]
FUENTE = RAIZ / "docs" / "propuesta_tecnica.md"
DIAGRAMA = RAIZ / "docs" / "arquitectura.png"
SALIDA = RAIZ / "docs" / "propuesta_tecnica.pdf"

CANDIDATOS_CHROME = [
    "google-chrome",
    "chromium",
    "chromium-browser",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
]

# Tamaño de fuente calibrado para que la propuesta quepa en 2 páginas carta,
# que es el límite que pide el reto.
BASE_PT = 8.6

CSS = f"""
@page {{ size: letter; margin: 12mm 13mm; }}
body {{ font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
        font-size: {BASE_PT}pt; line-height: 1.32; color: #1c1c1c; margin: 0; }}
h1 {{ font-size: {BASE_PT * 1.85:.1f}pt; margin: 0 0 2px; letter-spacing: -0.3px; color: #0f2b46; }}
h2 {{ font-size: {BASE_PT * 1.24:.1f}pt; margin: 11px 0 4px; padding-top: 6px;
      border-top: 2px solid #0f2b46; color: #0f2b46; }}
h3 {{ font-size: {BASE_PT * 1.08:.1f}pt; margin: 8px 0 3px; }}
p {{ margin: 4px 0; }}
hr {{ display: none; }}
table {{ border-collapse: collapse; width: 100%; margin: 5px 0; font-size: {BASE_PT * 0.90:.1f}pt; }}
th {{ background: #0f2b46; color: #fff; text-align: left; padding: 3px 5px; font-weight: 600; }}
td {{ border-bottom: 1px solid #dde3e9; padding: 2.5px 5px; vertical-align: top; }}
tr:nth-child(even) td {{ background: #f6f8fa; }}
table.num td:last-child, table.num th:last-child {{ text-align: right; }}
img {{ width: 100%; margin: 5px 0; border: 1px solid #dde3e9; border-radius: 3px; }}
ol, ul {{ margin: 4px 0; padding-left: 16px; }}
li {{ margin: 2px 0; }}
strong {{ color: #0f2b46; }}
em {{ color: #444; }}
code {{ background: #f0f2f5; padding: 1px 3px; border-radius: 2px; font-size: {BASE_PT * 0.9:.1f}pt; }}
blockquote {{ margin: 5px 0; padding-left: 9px; border-left: 3px solid #c9d3dc; }}
h1 + p {{ color: #555; font-size: {BASE_PT * 0.94:.1f}pt; }}
"""


def localizar_chrome(explicito: str | None) -> str:
    if explicito:
        return explicito
    for candidato in CANDIDATOS_CHROME:
        ruta = shutil.which(candidato) or (candidato if Path(candidato).exists() else None)
        if ruta:
            return ruta
    sys.exit("No se encontró Chrome/Chromium. Pásalo con --chrome.")


def construir_html() -> str:
    cuerpo = markdown.markdown(
        FUENTE.read_text(encoding="utf-8"), extensions=["tables", "attr_list"]
    )
    # La tabla de costos es la única con importes: sólo ahí se alinea a la derecha.
    cuerpo = re.sub(
        r"<table>(?=(?:(?!</table>).)*USD/mes)", '<table class="num">', cuerpo, flags=re.S
    )
    imagen = base64.b64encode(DIAGRAMA.read_bytes()).decode()
    cuerpo = cuerpo.replace(
        'src="arquitectura.png"', f'src="data:image/png;base64,{imagen}"'
    )
    return (
        "<!doctype html><html lang='es'><head><meta charset='utf-8'>"
        f"<style>{CSS}</style></head><body>{cuerpo}</body></html>"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chrome", default=None)
    args = parser.parse_args()

    with tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8") as tmp:
        tmp.write(construir_html())
        ruta_html = tmp.name

    subprocess.run(
        [
            localizar_chrome(args.chrome),
            "--headless",
            "--no-sandbox",
            "--disable-gpu",
            "--no-pdf-header-footer",
            f"--print-to-pdf={SALIDA}",
            ruta_html,
        ],
        check=True,
        capture_output=True,
    )
    print(f"generado {SALIDA.relative_to(RAIZ)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
