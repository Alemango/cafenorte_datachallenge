# CaféNorte — pipeline analítico multi-fuente

Reto técnico *Data Solutions Engineer* (Tuxpas). Concilia el POS de 40 tiendas,
el ERP legacy de inventario y la tienda Shopify en un solo modelo analítico, y
responde las cuatro preguntas de negocio del enunciado.

**Fecha de corte del análisis: 2026-03-31** (última fecha transaccional
observada). Todas las ventanas se anclan a esa fecha, no a `current_date()`, para
que la corrida sea reproducible.

---

## 1. Cómo correrlo

```bash
# 1. Coloca los 4 archivos fuente en data/raw/
#    sales.csv  inventory.json  ecommerce_orders.parquet  exchange_rates.csv
make setup          # venv + dependencias (necesita Python 3.10+ y Java 17/21)
make run            # bronze -> silver -> gold, ~2.5 min en una laptop
make test           # 60 tests
```

Salidas:

| Ruta | Contenido |
|---|---|
| `outputs/lake/{bronze,silver,gold}/` | modelo analítico en Parquet |
| `outputs/reportes/*.csv` | las 4 respuestas, legibles sin Spark |
| `outputs/reportes/reporte_calidad.json` | resultado de los 7 chequeos de calidad |

Otros comandos: `make silver` (se detiene antes de los marts),
`python -m cafenorte.pipeline --fecha-corte 2025-12-31` (congela otra ventana).

---

## 2. Las cuatro respuestas

### P1. Top 10 SKUs por rotación de inventario (últimos 6 meses)

Ventana **2025-10-01 a 2026-03-31**, canal físico, rotación = COGS del periodo /
inventario promedio valuado.

| SKU | Producto | Categoría | Unid. vendidas | COGS MXN | Inv. prom. MXN | Rotación | Días inv. |
|---|---|---|---:|---:|---:|---:|---:|
| CN-00057 | Premium Cafe Grano | cafe_grano | 603 | 56,864 | 44,345 | **1.28** | 142 |
| CN-00012 | Sándwich Comida Caliente | comida_caliente | 601 | 32,364 | 27,810 | 1.16 | 156 |
| CN-00041 | Gourmet Cafe Molido | cafe_molido | 577 | 25,372 | 22,665 | 1.12 | 163 |
| CN-00008 | Empanada Panaderia | panaderia | 557 | 11,263 | 10,453 | 1.08 | 169 |
| CN-00030 | Molinillo Mercancia | mercancia | 577 | 66,236 | 63,191 | 1.05 | 174 |
| CN-00037 | Tradicional Cafe Molido | cafe_molido | 610 | 78,141 | 75,302 | 1.04 | 175 |
| CN-00062 | Wrap Comida Caliente | comida_caliente | 560 | 39,541 | 38,468 | 1.03 | 177 |
| CN-00015 | Especial Cafe Molido | cafe_molido | 656 | 310,497 | 317,911 | 0.98 | 186 |
| CN-00034 | Estándar Cafe Molido | cafe_molido | 615 | 40,694 | 41,754 | 0.97 | 187 |
| CN-00051 | Termo Mercancia | mercancia | 586 | 135,825 | 140,730 | 0.96 | 189 |

> **Lectura de negocio:** el mejor SKU de la cadena rota **1.3 veces en seis
> meses**. Para una cafetería eso es muy lento: son ~142 días de inventario en
> el producto que más rápido se mueve. La conversación con el cliente no es "cuál
> es tu top 10", es "por qué toda la cadena trae ~5 meses de inventario en piso".
> Antes de sacar conclusiones definitivas hay que confirmar el punto de la
> sección 5: el ERP entrega *snapshots*, no movimientos, y no incluye mermas.

### P2. Tiendas con quiebres de más de 3 días (último trimestre)

Trimestre **2026-Q1**. Sólo **3 rachas** en toda la cadena superan los 3 días
consecutivos, y las tres duran exactamente 4:

| Tienda | Ciudad | SKU | Producto | Inicio | Fin | Días |
|---|---|---|---|---|---|---:|
| T015 | Reynosa | CN-00014 | Descafeinado Cafe Grano | 2026-02-09 | 2026-02-12 | 4 |
| T023 | Cancún | CN-00046 | Termo Mercancia | 2026-01-25 | 2026-01-28 | 4 |
| T038 | Cancún | CN-00040 | Chocolate Bebidas | 2026-03-18 | 2026-03-21 | 4 |

> **Por qué esta respuesta sola sería engañosa.** El umbral de 3 días esconde el
> problema real: en el trimestre hay **5,769 días-SKU con anaquel vacío**
> repartidos en las 40 tiendas (disponibilidad promedio 94.8%). Casi todos los
> quiebres duran 1 o 2 días — se reponen rápido, pero son constantes. Por eso el
> pipeline entrega también `p2_dias_en_cero_por_tienda.csv`, que es el número
> accionable para operaciones:
>
> | Tienda | Ciudad | Días-SKU en cero | Disponibilidad |
> |---|---|---:|---:|
> | T006 | León | 198 | 94.4% |
> | T025 | Chihuahua | 193 | 94.5% |
> | T036 | León | 189 | 94.0% |
> | T001 | CDMX | 188 | 94.7% |

### P3. Crecimiento MoM por canal (últimos 12 meses)

Ventana **2025-04 a 2026-03**, ingreso neto en MXN.

| Mes | Físico MXN | MoM | E-commerce MXN | MoM |
|---|---:|---:|---:|---:|
| 2025-04 | 1,532,102 | — | 383,213 | — |
| 2025-05 | 1,639,658 | +7.02% | 365,088 | −4.73% |
| 2025-06 | 1,577,691 | −3.78% | 339,352 | −7.05% |
| 2025-07 | 1,644,239 | +4.22% | 350,578 | +3.31% |
| 2025-08 | 1,642,587 | −0.10% | 343,800 | −1.93% |
| 2025-09 | 1,563,129 | −4.84% | 345,454 | +0.48% |
| 2025-10 | 1,648,071 | +5.43% | 376,497 | +8.99% |
| 2025-11 | 1,544,897 | −6.26% | 359,338 | −4.56% |
| 2025-12 | 1,648,255 | +6.69% | 346,649 | −3.53% |
| 2026-01 | 1,553,377 | −5.76% | 344,899 | −0.50% |
| 2026-02 | 1,434,265 | −7.67% | 323,251 | −6.28% |
| 2026-03 | 1,543,956 | +7.65% | 350,763 | +8.51% |

> **Lectura de negocio:** los dos canales están **planos**. El MoM promedio es
> +0.24% físico y −0.66% digital; el rango completo (−7.7% a +7.7%) es ruido
> mensual, no tendencia. Total del año: **$18.97M MXN** físico y **$4.23M MXN**
> digital (18.2% de la mezcla). Febrero es el piso en ambos canales los dos años
> con dato — 28 días explican parte, no todo.

### P4. Productos con margen negativo y en qué tiendas ocurren

Ventana de 12 meses, margen bruto = ingreso neto − (unidades × costo vigente el
día de la venta).

| SKU | Producto | Unid. | Ingreso MXN | COGS MXN | Margen MXN | Margen % |
|---|---|---:|---:|---:|---:|---:|
| CN-00015 | Especial Cafe Molido | 1,271 | 460,584 | 604,652 | **−144,068** | −31.3% |
| CN-00002 | Sándwich Comida Caliente | 1,152 | 151,728 | 195,903 | **−44,174** | −29.1% |
| CN-00001 | Sándwich Comida Caliente | 1,177 | 87,856 | 99,159 | **−11,303** | −12.9% |

**Dónde ocurre:** en **las 40 tiendas**, sin excepción — no es un problema de
ejecución local, es que el precio de lista quedó por debajo del costo de
proveedor. Ninguno de los tres se vende en Shopify. Pérdida acumulada en 12
meses: **−$199,594 MXN** (~1.1% del ingreso del canal físico).
`p4_margen_negativo_producto_tienda.csv` trae el detalle de las 121 combinaciones
producto × tienda, incluida una tienda que pierde dinero en un producto que a
nivel cadena es rentable (CN-00066 en una sola tienda, −$49).

---

## 3. Stack y por qué

**PySpark 3.5 en modo local + Parquet particionado, arquitectura medallion
(bronze / silver / gold).**

Hay que ser honesto sobre el trade-off: **330 mil filas no necesitan Spark**.
DuckDB o Polars resolverían esto más rápido y con menos ceremonia, y en un
proyecto donde el volumen fuera realmente el de este ZIP, esa sería la elección
correcta. Se eligió PySpark por dos razones concretas de este proyecto:

1. **La propuesta productiva de la sección 2.2 corre en AWS Glue, que es Spark.**
   El código de `transform/` y `marts/` se mueve a un job de Glue cambiando la
   creación de la sesión y las rutas `s3://`. Si el reto se resolviera en DuckDB,
   la propuesta de arquitectura y la implementación serían dos artefactos
   distintos, y el cliente pagaría una reescritura para pasar de una a otra.
2. **El volumen real del cliente no es el del ZIP.** 40 tiendas × 70 SKUs × 365
   días de snapshots son ~1M filas/año sólo de inventario, más el ticket a nivel
   línea. A 3–5 años con crecimiento, el orden de magnitud justifica un motor que
   no dependa de la memoria de una sola máquina.

Lo que **no** se usó y por qué: sin dbt (una sola persona, 8 modelos: el
overhead de configuración supera el beneficio); sin Airflow (el orquestador cabe
en un `argparse` y en producción lo hace Step Functions); sin Delta/Iceberg
(no hay `MERGE` ni viajes en el tiempo en este alcance — Parquet plano basta y se
lee desde Athena sin capa extra).

### Estructura

```
conf/settings.yaml            parámetros de negocio (ventanas, umbrales, CFDI)
src/cafenorte/
  config.py                   carga tipada del YAML
  spark.py                    sesión + escritura/lectura del lake
  ingest/bronze.py            lectura con esquema explícito, sin reglas de negocio
  transform/product_keys.py   conciliación de SKU entre los 3 sistemas
  transform/dimensions.py     dim_producto, dim_tienda, dim_costo (SCD2), dim_tipo_cambio
  transform/facts.py          fct_ventas (2 canales), fct_inventario_diario
  marts/business_questions.py las 4 preguntas
  quality/expectations.py     7 chequeos que detienen el pipeline
  pipeline.py                 orquestador / CLI
tests/                        60 tests (unitarios + verificación cruzada)
docs/                         propuesta técnica, diagrama, decisiones
```

### Modelo analítico

```
dim_producto ──┐
dim_tienda ────┼──> fct_ventas (fisico + ecommerce, MXN)  ──> p1, p3, p4
dim_costo ─────┼──> fct_ventas_costeadas (margen)         ──> p1, p4
dim_tipo_cambio┘
               └──> fct_inventario_diario / valuado       ──> p1, p2
```

---

## 4. Las cuatro trampas de los datos

Esto es lo que separa un pipeline que "corre sin error" de uno que da los
números correctos. Cada una está cubierta por un test.

### 4.1 El POS mezcla comprobantes que no son ventas

`tipo_comprobante` usa el catálogo CFDI 4.0 del SAT y trae cinco valores:

| Tipo | Significado | Filas | Tratamiento |
|---|---|---:|---|
| I | Ingreso (venta) | 82,518 | suma |
| E | Egreso (nota de crédito / devolución) | 3,079 | **resta** |
| P | Complemento de pago | 451 | **se excluye**: es la liquidación de una factura ya contada |
| T | Traslado de mercancía | 154 | **se excluye**: no es venta |
| N | Nómina | 288 | **se excluye**: ruido del POS |

`SUM(monto)` a secas da **$30.95M**; el ingreso neto real de las mismas fechas es
**$28.36M**. Un **9.1% de sobreestimación** que corre sin ningún error.
El test `test_no_se_sumaron_comprobantes_que_no_son_venta` falla si esta regla se
rompe.

### 4.2 El ERP escribe `"N/A"` en las lecturas de inventario faltantes

4,417 snapshots (1.9%) traen la cadena `"N/A"` en `cantidad_en_stock`. El reflejo
automático —`CAST(... AS INT)` y luego `COALESCE(..., 0)`— convierte cada uno en
un quiebre de stock inventado. El pipeline conserva la distinción entre *"hay
cero piezas"* y *"no sabemos"*: `unidades = NULL`, `lectura_valida = false`.

Además, un día sin lectura **corta** la racha de ceros en vez de puentearla. Es
la lectura conservadora: no se afirma un quiebre que no se observó.
`p2_quiebres_sensibilidad.csv` cuantifica cuánto depende la respuesta de ese
supuesto:

| Escenario | Rachas > 3 días | Tiendas |
|---|---:|---:|
| N/A corta la racha (**el que se reporta**) | 3 | 3 |
| N/A se puentea | 3 | 3 |
| N/A se trata como cero (el error clásico) | 4 | 4 |

### 4.3 El mapeo de SKU del ERP está incompleto

Los tres sistemas nombran distinto al mismo producto (`CN-00018` /
`ERP-PROV-MX-018-A` / `tradicional-cafe-molido-018`), y la tabla `sku_mappings`
del ERP no cubre todo:

* 5 SKUs del POS con ventas no aparecen en la tabla,
* 5 renglones traen `sku_erp` nulo,
* 6 handles de Shopify con órdenes no aparecen.

Un `JOIN` directo contra `sku_mappings` descarta **8,066 de 96,437 líneas de
venta (8.4%)** —incluido **CN-00001, uno de los tres productos con margen
negativo**— y el pipeline entrega la respuesta 4 incompleta sin quejarse.

**Solución:** los tres formatos comparten un consecutivo numérico, y en los 65
renglones del mapeo explícito coincide en los tres sistemas **sin una sola
excepción**. Se usa ese consecutivo como llave conforme y el mapeo del ERP como
**oráculo de validación**: si algún día deja de coincidir, el chequeo
`mapeo_sku_consistente` detiene el pipeline en vez de producir números malos.

### 4.4 Precios en tres monedas y costos que cambian en el tiempo

* Shopify factura en MXN (70%), USD (25%) y EUR (5%). Se convierte con el tipo de
  cambio **del día de la orden**. Con el USD moviéndose entre 17.13 y 18.83 en el
  periodo, una tasa fija sesga hasta 9% el ingreso del 30% de las órdenes. Una
  moneda sin tipo de cambio deja el importe en `NULL` y **falla** el pipeline; no
  se le aplica un `1.0` silencioso.
* `cost_history` es una dimensión SCD2 disfrazada: 282 cambios de costo entre 70
  productos, con cambios de proveedor. El margen usa un join *as-of* por fecha de
  venta. Usar el último costo conocido reescribiría el margen de todos los meses
  anteriores a cada cambio.

---

## 5. Supuestos e interpretaciones

Documentados aquí en vez de asumidos en silencio. Los que cambiarían un número
del reporte están marcados **⚠**.

**Ventanas de tiempo**

1. "Últimos N meses" = N meses calendario completos que terminan en el mes de la
   fecha de corte (2026-03). "Último trimestre" = **2026-Q1 calendario**, no los
   últimos 90 días. ⚠
2. La ventana de 6 meses de P1 coincide exactamente con la cobertura de
   snapshots del ERP (2025-10-01 a 2026-03-31). No hay inventario antes de esa
   fecha, así que no era posible una ventana distinta.
3. El POS tiene 18 meses de historia y Shopify 12. P3 y P4 se recortan a 12 para
   que la comparación entre canales sea sobre la misma ventana. ⚠

**Definiciones de negocio**

4. **Venta** = ingreso neto (facturas menos notas de crédito), sin IVA
   desglosado: el POS no separa impuestos y se asume que `monto` es el importe
   que el cliente pagó. ⚠ **Pregunta abierta para el cliente.**
5. **Rotación** = COGS / inventario promedio valuado a costo, la definición
   contable estándar. Se reporta también en unidades como contraste; las dos
   coinciden hasta el tercer decimal, así que la conclusión no depende de la
   elección.
6. El inventario promedio se calcula con **los 182 snapshots diarios**, no con
   (inicial + final)/2. Con la serie completa disponible, el atajo de dos puntos
   es frágil ante un día atípico.
7. P1 usa **sólo el canal físico**: los snapshots del ERP son por tienda y no
   cubren el almacén de Shopify. Mezclar el COGS digital con un inventario que no
   lo respalda infla la rotación. ⚠
8. **Quiebre** = lectura válida con stock = 0. "Más de 3 días" se lee estricto:
   rachas de **4 o más días consecutivos**. ⚠
9. **Margen** = margen bruto de proveedor. El ERP no publica flete, mermas ni
   gasto de operación, así que el margen real es **peor** que el reportado, no
   mejor.
10. E-commerce entra al modelo como la pseudo-tienda `ONLINE` para poder
    responder "en qué tiendas ocurre" con un solo modelo.

**Datos**

11. Los timestamps del POS ya vienen en **hora local de cada tienda** — se
    verificó que las 40 tiendas venden entre las 07:00 y las 21:00 en sus nueve
    husos horarios distintos. No se reinterpretan. Si vinieran en UTC, cada
    tienda de Tijuana aparecería vendiendo a las 23:00.
12. `fecha` de Shopify se trata como hora del centro de México. No afecta ningún
    resultado: sólo se usa a nivel día y mes.
13. Las columnas de PII de Shopify (nombre, correo, RFC, dirección) **se
    descartan en la ingesta**. No hacen falta para ninguna de las 4 preguntas y no
    deben viajar al lake. Ver la propuesta técnica para las implicaciones de
    LFPDPPP.
14. Los snapshots cubren 1,268 de las 2,800 combinaciones tienda × SKU posibles:
    se asume que cada tienda maneja un surtido parcial, no que falten datos.

---

## 6. Confiabilidad: cómo sé que los números están bien

**7 chequeos de calidad dentro del pipeline** (`outputs/reportes/reporte_calidad.json`).
Los de severidad `error` abortan la corrida:

| Chequeo | Resultado en esta corrida |
|---|---|
| `mapeo_sku_consistente` | la llave numérica coincide con `sku_mappings` en todos los renglones |
| `ventas_con_producto` | 0 de 96,437 líneas sin producto en catálogo |
| `ventas_convertidas_a_mxn` | 0 líneas sin tipo de cambio aplicable |
| `signo_coherente` | 0 líneas con unidades e importe de signo opuesto |
| `lecturas_inventario_validas` | 4,417 de 230,776 sin lectura (1.91%, dentro del umbral) |
| `grano_inventario_unico` | 0 duplicados fecha/tienda/producto |
| `inventario_no_negativo` | 0 lecturas negativas |

**60 tests** en tres niveles:

* **Lógica** (`test_product_keys.py`, `test_transformaciones.py`): reglas
  aisladas sobre datos sintéticos con resultados calculables a mano — que `E`
  reste, que `"N/A"` no sea cero, que una venta de mayo se costee con el costo de
  mayo, que una moneda desconocida deje `NULL`.
* **Marts** (`test_preguntas_negocio.py`): escenarios construidos para cada
  pregunta, incluidos los casos frontera de P2 (racha de 3 vs 4 días, hueco que
  corta la racha, tiendas que no se mezclan).
* **Verificación cruzada** (`test_integracion.py`): **recalcula las cifras clave
  leyendo los archivos fuente con pandas** —un camino que no comparte una sola
  línea con el pipeline de Spark— y las compara contra la capa gold. Los 12
  ingresos mensuales de cada canal, las rachas de quiebre, el margen de cada
  producto y las unidades del top 10 coinciden en ambas rutas.

Que un pipeline corra no prueba nada; que dos implementaciones independientes
lleguen al mismo número sí es evidencia.

---

## 7. Gestión de alcance: qué quedó fuera

Priorizado hacia la corrección de los números y la trazabilidad de las
decisiones, que es donde el reto pone el peso (25% + 20%).

**Dentro:** pipeline completo con capas, las 4 respuestas verificadas, 7
chequeos de calidad, 60 tests, propuesta AWS costeada, documentación de
supuestos.

**Fuera, a propósito:**

* **Tablero / notebook de visualización.** Las respuestas son 7 CSV y las tablas
  de este README. En producción esto es QuickSight (ver propuesta), y montar un
  tablero desechable aquí no habría probado nada nuevo.
* **Carga incremental.** El pipeline reprocesa todo en cada corrida. Con 330k
  filas tarda 2.5 minutos; el incremental es trabajo de la Fase 3 y agregarlo hoy
  sería complejidad sin beneficio.
* **Delta Lake / Iceberg.** No hay `MERGE`, ni actualizaciones en sitio, ni
  necesidad de viajar en el tiempo en este alcance.
* **CI en GitHub Actions.** El `Makefile` deja la suite lista para engancharse;
  no se configuró el workflow.
* **Análisis de mermas y de rotación por tienda.** El ERP no entrega movimientos
  transaccionales (lo dice su propia metadata), así que no es posible con estos
  datos. Está en las preguntas abiertas al cliente.

---

## 8. Documentos relacionados

| Archivo | Contenido |
|---|---|
| [`docs/propuesta_tecnica.md`](docs/propuesta_tecnica.md) | propuesta al director de CaféNorte: arquitectura AWS, costeo, fases, riesgos |
| [`docs/arquitectura.md`](docs/arquitectura.md) | diagrama de la arquitectura propuesta |
| [`AI_LOG.md`](AI_LOG.md) | bitácora de uso de IA |
