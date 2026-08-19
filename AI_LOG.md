# AI_LOG.md — Bitácora de uso de IA

> **Cómo llenar este archivo.** Está estructurado con las secciones que pide el
> reto y con notas de contexto sobre lo que efectivamente pasó en el desarrollo.
> Sustituye cada bloque `<!-- TODO -->` con tu versión. Los prompts y los
> hallazgos técnicos que aparecen como referencia son reales y verificables
> contra el código, pero **la narrativa tiene que ser tuya**: en la entrevista
> vas a tener que defender cada decisión sin la IA presente.

---

## 1. Herramientas usadas

<!-- TODO: completa con lo que realmente usaste. Incluye modelo y versión si la sabes. -->

| Herramienta | Modelo / versión | Para qué la usé |
|---|---|---|
| <!-- ej. Claude (Cowork) --> | <!-- ej. claude-opus-5 --> | <!-- exploración de datos, diseño del pipeline, redacción --> |
| | | |

**Entorno de trabajo:** <!-- TODO: IDE, terminal, notebooks, sistema operativo -->

---

## 2. Flujo de trabajo

<!-- TODO: describe cómo lo orquestaste. Preguntas guía:
     - ¿Un solo agente conversacional o varios en paralelo?
     - ¿Usaste plan mode / modo planeación antes de escribir código?
     - ¿Conectaste MCP servers? ¿A qué (sistema de archivos, git, algún SaaS)?
     - ¿En qué momento revisaste tú y en qué momento delegaste?
     - ¿Corriste los tests tú o los corrió el agente?
-->

**Orden en que trabajé** (este fue el orden real y vale la pena explicarlo,
porque no es el obvio):

1. **Perfilado antes que arquitectura.** Antes de escribir una línea de pipeline,
   se perfilaron las 4 fuentes: tipos, nulos, duplicados, cardinalidades, rangos
   de fecha, distribución de horas por huso horario, consistencia de precios
   unitarios. Las decisiones de diseño salieron de ahí, no de una plantilla.
2. **Diseño del modelo** (dimensiones, hechos, llave conforme) y de las
   interpretaciones de las 4 preguntas.
3. **Implementación por capas**, corriendo el pipeline al terminar cada una.
4. **Tests**, incluida una verificación cruzada en pandas que recalcula los
   resultados por un camino independiente.
5. **Documentación y propuesta**, al final, con los números ya verificados.

<!-- TODO: ajusta lo anterior a lo que hiciste tú, y agrega dónde intervino la IA
     en cada paso. -->

---

## 3. Prompts clave

> Formato: el prompt → lo que devolvió (resumido) → qué hiciste con eso y por qué.
> El reto pide entre 3 y 5. Abajo hay cinco esqueletos con el contexto técnico
> real de cada momento; reescríbelos con tus palabras y tus prompts.

### 3.1 Perfilado inicial de las fuentes

**Prompt:**
```
<!-- TODO: tu prompt real -->
```

**Qué devolvió (resumido):** un perfilado de las cuatro fuentes. Los hallazgos
que resultaron determinantes:

* `tipo_comprobante` en el POS tiene cinco valores (`I`/`E`/`P`/`T`/`N`), no dos;
* 4,417 snapshots de inventario traen la cadena `"N/A"` en vez de un número;
* la tabla `sku_mappings` del ERP no cubre 5 SKUs del POS ni 6 handles de Shopify;
* `cost_history` es una dimensión con vigencias, no un costo único por producto.

**Qué hice con eso:** <!-- TODO: ¿lo aceptaste tal cual? ¿verificaste cada hallazgo
tú mismo? ¿cuál te sorprendió? -->

---

### 3.2 Interpretación de los cinco tipos de comprobante

**Prompt:**
```
<!-- TODO -->
```

**Qué devolvió (resumido):** que los valores corresponden al catálogo CFDI 4.0
del SAT (I = Ingreso, E = Egreso, P = Pago, T = Traslado, N = Nómina) y la
propuesta de contar sólo `I` y restar `E`.

**Qué hice con eso:** <!-- TODO: ¿verificaste el catálogo CFDI por tu cuenta?
¿cuantificaste el impacto? (la suma ingenua sobreestima 9.1%) -->

---

### 3.3 Conciliación de SKU entre los tres sistemas

**Prompt:**
```
<!-- TODO -->
```

**Qué devolvió (resumido):** la observación de que los tres formatos comparten
un consecutivo numérico (`CN-00018` / `ERP-PROV-MX-018-A` /
`tradicional-cafe-molido-018`) y la propuesta de usarlo como llave conforme.

**Qué hice con eso:** <!-- TODO: la decisión de usar el mapeo explícito como
*oráculo de validación* en vez de como fuente de la llave, ¿fue tuya o suya?
¿Cómo comprobaste que la convención se sostiene en los 65 renglones mapeados?
Ver `transform/product_keys.py` y el chequeo `mapeo_sku_consistente`. -->

---

### 3.4 <!-- TODO: tu cuarto prompt clave -->

**Prompt:**
```
<!-- TODO -->
```

**Qué devolvió (resumido):** <!-- TODO -->

**Qué hice con eso:** <!-- TODO -->

---

### 3.5 <!-- TODO: tu quinto prompt clave -->

**Prompt:**
```
<!-- TODO -->
```

**Qué devolvió (resumido):** <!-- TODO -->

**Qué hice con eso:** <!-- TODO -->

---

## 4. Dónde la IA se equivocó o propuso algo subóptimo

> El reto pide **al menos uno**. Vale la pena documentar dos o tres: es la
> sección que mejor demuestra criterio. Abajo van casos reales del desarrollo,
> con el contexto suficiente para que los reconstruyas con tus palabras.

### Caso A — Cómo tratar las lecturas de inventario faltantes

**Lo que la IA propuso inicialmente:** limpiar `cantidad_en_stock` con un
`CAST(... AS INT)` y rellenar los nulos resultantes con `0`. Es el patrón por
defecto y suena razonable: "no había lectura, así que no había inventario".

**Por qué está mal:** los 4,417 `"N/A"` no significan "cero piezas", significan
"el ERP no reportó". Rellenarlos con cero **inventa quiebres de stock que nunca
ocurrieron**, y la pregunta 2 es exactamente sobre quiebres de stock. El
pipeline habría corrido sin un solo error y entregado una respuesta inflada.

**Cómo se detectó:** al revisar por qué el conteo de días en cero era mayor que
el de ceros explícitos en el archivo original.

**Cómo se corrigió:** `fct_inventario_diario` conserva la distinción con
`unidades = NULL` y `lectura_valida = false`; un día sin lectura no cuenta como
quiebre y además corta la racha. Además se agregó
`p2_quiebres_sensibilidad.csv`, que cuantifica los tres criterios posibles —el
escenario "N/A como cero" reporta 4 rachas contra las 3 reales.

<!-- TODO: reescribe con tu voz. ¿Cómo lo detectaste tú? -->

### Caso B — <!-- TODO: tu segundo caso -->

<!-- Candidatos reales que puedes desarrollar:

  * El `JOIN` directo contra `sku_mappings`. Es la solución obvia y la que
    cualquier asistente propone primero. Descarta ~6% de las líneas de venta,
    incluido CN-00001, que es uno de los tres productos con margen negativo. La
    respuesta 4 habría salido incompleta sin ningún error visible.

  * Usar el último costo conocido para todo el histórico en vez de un join
    as-of contra las vigencias de `cost_history`. Reescribe el margen de todos
    los meses anteriores a cada cambio de proveedor.

  * Convertir las órdenes de Shopify con un tipo de cambio fijo. Con el USD
    entre 17.13 y 18.83 en el periodo, sesga hasta 9% el 30% de las órdenes.

  * Usar (inventario_inicial + inventario_final) / 2 como denominador de la
    rotación, teniendo 182 snapshots diarios disponibles.
-->

---

## 5. Auto-crítica

<!-- TODO: un párrafo. Preguntas guía, en orden de importancia:

  1. ¿Qué parte del resultado consideras 100% tuya? (candidatos honestos: la
     decisión de qué preguntar al cliente y qué resolver con criterio; la
     elección de reportar la respuesta estricta de P2 junto con los días-SKU en
     cero porque la respuesta estricta sola sería engañosa; el juicio sobre el
     trade-off de usar PySpark para 330k filas)

  2. ¿Qué parte es mérito de la IA? (candidatos: la velocidad del perfilado
     inicial, el reconocimiento del catálogo CFDI, la observación del
     consecutivo numérico compartido, el andamiaje de los tests)

  3. ¿Cómo validaste que el output es correcto, más allá de que "corre sin
     error"? — Ésta es la pregunta que más pesa. Lo que hay en el repo:

     * 7 chequeos de calidad que **abortan** la corrida, no que advierten;
     * 48 tests de lógica sobre datos sintéticos con resultados calculables a
       mano (que `E` reste, que `"N/A"` no sea cero, que una venta de mayo se
       costee con el costo de mayo, que una racha de 3 días no se reporte);
     * 12 tests de verificación cruzada que **recalculan las cifras clave
       leyendo los archivos fuente con pandas** —un camino que no comparte una
       sola línea con el pipeline de Spark— y las comparan contra la capa gold:
       los 12 ingresos mensuales de cada canal, las rachas de quiebre, el margen
       de cada producto y las unidades del top 10.

     Que un pipeline corra no prueba nada. Que dos implementaciones
     independientes lleguen al mismo número sí es evidencia.

  4. ¿Qué harías distinto con más tiempo? ¿Qué parte del código te incomoda?
-->

---

## 6. Nota de honestidad

<!-- TODO (opcional pero recomendado): una o dos frases sobre el nivel de
     asistencia real. El reto dice explícitamente "no hay respuesta correcta
     sobre cuánta IA usar; queremos ver juicio, no pureza". Un log que exagera
     la intervención humana es más fácil de desarmar en la entrevista que uno
     que dice la verdad. -->
