# AI_LOG.md — Bitácora de uso de IA

Este reto lo resolví con asistencia de IA de forma deliberada y con una división
de trabajo explícita: **yo tomé las decisiones sobre los datos y el negocio; la
IA construyó la suite de pruebas, la documentación y la tooling del repo.** Abajo
está el detalle de qué le pedí, qué me devolvió, qué acepté, qué rechacé y en qué
se equivocó.

---

## 1. Herramientas usadas

| Herramienta | Modelo / versión | Para qué la usé |
|---|---|---|
| Claude (Cowork, app de escritorio) | `claude-opus-5` | Agente principal: exploración de datos, generación de la suite de pruebas, redacción del README y de la propuesta, Makefile y script de build del PDF |
| Puente de archivos de Claude (acceso a carpeta local) | — | Leer los cuatro archivos fuente y el enunciado sin subirlos a un chat |
| pandas / PySpark en local | pandas 2.2, PySpark 3.5.6 | Perfilado y ejecución; todo lo que la IA escribió lo corrí yo |
| `pyflakes` | 3.x | Barrido de imports muertos y variables sin usar antes de cada commit |

**Entorno:** macOS, terminal + editor, entorno virtual con `make setup`. No usé
subagentes ni MCP servers adicionales: una sola conversación larga, con el
contexto del proyecto vivo.

---

## 2. Flujo de trabajo

Trabajé en un orden que no es el obvio, y creo que es la decisión de proceso que
más impacto tuvo en el resultado:

**1. Perfilado antes que arquitectura.** Antes de escribir una línea de pipeline,
perfilé las cuatro fuentes: tipos, nulos, duplicados, cardinalidades, rangos de
fecha, distribución de horas de venta por huso horario, estabilidad de precios
unitarios por SKU, cobertura de la tabla de mapeo. Ahí salieron las cuatro
trampas del dataset (los cinco tipos de comprobante CFDI, los `"N/A"` del ERP, el
mapeo de SKU incompleto y el `cost_history` con vigencias). Si hubiera empezado
por el diseño, habría diseñado para los datos que imaginaba, no para los que hay.

**2. Decisiones de modelo e interpretación, mías.** La llave conforme, el
tratamiento de las lecturas faltantes, las definiciones de rotación, quiebre y
margen, y qué preguntar al cliente contra qué resolver con criterio: eso lo
decidí yo y está documentado en la sección 5 del README. La IA participó como
interlocutor, no como autor.

**3. Implementación por capas**, corriendo el pipeline al terminar cada una para
no acumular errores.

**4. Pruebas: delegadas a la IA, con una restricción dura.** Le pedí la suite
completa con una condición no negociable —que ningún test comparara el pipeline
contra sí mismo— y validé cada aserción a mano. Volvió dos veces (ver §4).

**5. Documentación y tooling: delegados a la IA.** README, propuesta técnica,
diagrama, `Makefile` y el script que genera el PDF. Yo di la estructura, los
números y las restricciones; ella redactó y yo edité.

**Regla que me impuse:** ningún archivo entra al repo sin que yo lo haya leído
completo. Es la razón por la que no hay imports muertos ni comentarios tipo
`# Here we do X`, y por la que puedo defender cada línea en la entrevista.

---

## 3. Prompts clave

### 3.1 Perfilado dirigido de las fuentes

**Prompt:**
```
Tengo 4 fuentes: sales.csv (POS), inventory.json (ERP legacy, anidado),
ecommerce_orders.parquet (Shopify) y exchange_rates.csv.

No me propongas todavía ninguna arquitectura ni escribas pipeline. Perfílalas y
dime únicamente qué está roto o es sospechoso: cardinalidades, nulos, valores
centinela, duplicados, rangos de fecha, y en particular si algún campo
categórico tiene más valores de los que su nombre sugiere.
```

**Qué devolvió:** el perfilado con cuatro hallazgos que resultaron determinantes:
`tipo_comprobante` tiene cinco valores y no dos; 4,417 snapshots traen la cadena
`"N/A"` en `cantidad_en_stock`; `sku_mappings` no cubre 5 SKUs del POS ni 6
handles de Shopify; `cost_history` es una lista de vigencias, no un costo único.

**Qué hice con eso:** verifiqué los cuatro contra los archivos yo mismo antes de
construir nada sobre ellos —conteos, `value_counts`, diferencias de conjuntos— y
cuantifiqué el impacto de cada uno. La instrucción de **no** proponer
arquitectura fue intencional: cuando le pides un pipeline de entrada, el
perfilado se vuelve una nota al pie y los hallazgos raros se pierden.

---

### 3.2 Suite de pruebas — primer intento

**Prompt:**
```
Escribe la suite de pytest del pipeline. Quiero tests de las transformaciones de
silver y de los cuatro marts.
```

**Qué devolvió:** ~40 tests que corrían el pipeline sobre los datos reales y
comparaban el resultado contra… el resultado del pipeline. Verdes los 40.

**Qué hice con eso:** **lo descarté completo.** Un test que compara el pipeline
consigo mismo detecta cambios, no errores: habría pasado igual de verde con la
respuesta 4 equivocada. Esto motivó el prompt 3.3.

---

### 3.3 Suite de pruebas — segundo intento, con la restricción correcta

**Prompt:**
```
Rehaz los tests con tres reglas:

1. Los tests de lógica van sobre datos sintéticos que yo pueda verificar
   mentalmente. Nada de fixtures de 500 filas.
2. Cada test aísla UNA regla de negocio que, si se rompe, produce un pipeline
   que corre sin error con números equivocados. Nómbralos por la regla, no por
   la función.
3. Agrega un archivo de verificación cruzada que recalcule las cifras clave
   leyendo los archivos fuente con pandas, sin importar nada de src/, y compare
   contra la capa gold. Si las dos rutas no coinciden, quiero que truene.
```

**Qué devolvió:** las 61 pruebas que están en el repo, en tres niveles: lógica
(`test_product_keys.py`, `test_transformaciones.py`), marts con casos frontera
(`test_preguntas_negocio.py`) y verificación cruzada (`test_integracion.py`).

**Qué hice con eso:** **acepté la estructura y revisé cada aserción a mano.**
Los tests que más valoro son los que verifiqué con lápiz: que una racha de 3 días
no se reporte pero la de 4 sí; que `0,0,N/A,0,0` sean dos rachas de dos y no una
de cinco; que una venta de mayo se costee con el costo de mayo y no con el
último. Rechacé tres tests que sólo comprobaban que una función devolviera un
DataFrame no vacío.

La regla 3 es la que convierte la suite en evidencia. Los 12 tests de
`test_integracion.py` reconstruyen con pandas los 12 ingresos mensuales de cada
canal, las rachas de quiebre, el margen de cada producto y las unidades del top
10, por un camino que no comparte una sola línea con Spark.

---

### 3.4 README y propuesta técnica

**Prompt:**
```
Redacta el README y la propuesta al cliente con estas restricciones:

- README: incluye las 4 respuestas con su lectura de negocio, la justificación
  del stack SIN vender PySpark como si fuera obvio (330k filas no lo necesitan,
  dilo), las trampas del dataset con el impacto cuantificado, y una sección de
  supuestos marcando cuáles cambiarían un número.
- Propuesta: máximo 2 páginas, dirigida a un dueño no técnico y a su director de
  TI. El presupuesto de USD $200/mes es un límite duro. Si algún servicio que
  propongas lo revienta, no lo propongas y explica por qué lo descartaste.
- Nada de "aprovechar sinergias" ni relleno de consultoría. Frases cortas.
```

**Qué devolvió:** las dos versiones que están en el repo, ya con la tabla de
costos sumando ~$85/mes y con la sección de servicios descartados
(Redshift, EMR, Kinesis, Transfer Family).

**Qué hice con eso:** **acepté la estructura y edité el contenido.** Dos cambios
míos importantes: (a) reescribí las "lecturas de negocio" de cada respuesta,
porque las suyas describían la tabla en vez de interpretarla —el hallazgo de que
el mejor SKU rota 1.3 veces en seis meses, que es lentísimo para una cafetería,
es la conversación real con el cliente y ella lo había dejado como un dato más;
(b) **verifiqué los precios de AWS contra la documentación oficial** en vez de
confiar en su estimación de memoria. QuickSight cambió de precio respecto a lo
que ella asumía, y es el 85% de la factura: equivocarse ahí invalida toda la
propuesta.

---

### 3.5 Makefile y build reproducible del PDF

**Prompt:**
```
Necesito que el repo se pueda correr sin leer el README. Haz un Makefile con
setup, run, corridas parciales por capa, los tests separados en unitarios e
integración, y clean. Que `make help` sea la documentación.

Aparte: el PDF de la propuesta no lo quiero mantener a mano. Script que lo
genere desde el Markdown y el diagrama, que quepa en 2 páginas carta.
```

**Qué devolvió:** el `Makefile` con once targets y
`scripts/build_propuesta_pdf.py`, que convierte el Markdown a HTML con CSS de
impresión y lo manda a Chrome headless.

**Qué hice con eso:** **acepté casi tal cual.** Es el tipo de trabajo donde la IA
es claramente mejor inversión que mi tiempo: no hay decisión de negocio, el
criterio de correcto es binario y se verifica corriéndolo. Dos ajustes míos: que
el `Makefile` use el Python del venv en vez del del sistema (si no, `make test`
falla en una máquina limpia), y separar `test-unit` de `test-integracion` para
poder correr la lógica sin haber construido el lake.

---

## 4. Dónde la IA se equivocó

### Caso A — Un análisis de sensibilidad donde los tres escenarios calculaban lo mismo

**Qué pasó.** Para la pregunta 2 quería mostrar cuánto depende la respuesta del
supuesto sobre las lecturas faltantes, así que pedí un mart que comparara tres
criterios: `"N/A"` corta la racha (conservador), `"N/A"` se puentea (optimista) y
`"N/A"` se trata como cero (el error clásico). La IA lo entregó y corrió sin
error, con un resultado plausible: 3, 3 y 4 rachas respectivamente.

**Por qué estaba mal.** El escenario "puentea" estaba implementado con
`row_number() - dense_rank()` sobre la **misma** ventana. Esa resta es constante
dentro de la partición, así que agrupaba toda la serie de la tienda-producto en
una sola isla. Que coincidiera con el escenario conservador fue **casualidad de
estos datos**: con otra distribución habría reportado una racha de 90 días.

**Cómo lo detecté.** Me pareció sospechoso que dos escenarios que definí como
distintos dieran exactamente el mismo número. Construí a mano el caso mínimo que
tiene que separarlos —`0,0,N/A,0,0`, que vale 0, 1 y 1 rachas según el criterio—
y el escenario "puentea" devolvía 0.

**Cómo lo corregí.** Reimplementé la isla numerando **sólo los días con lectura
válida** (`orden_valido`), que es lo que "ignorar el hueco sin fusionar rachas
separadas por stock positivo" significa en realidad, y **añadí ese caso mínimo
como test permanente** (`test_sensibilidad_separa_los_tres_criterios`). Sin ese
test, el análisis de sensibilidad daba una falsa sensación de robustez: decía
"mira, el supuesto casi no importa" cuando en realidad no estaba midiendo nada.

Es el caso que más me preocupa de los tres, porque **no había ningún síntoma**:
código limpio, sin excepción, con un número creíble.

### Caso B — Tests que comparaban el pipeline contra sí mismo

Descrito en §3.2. Los 40 tests del primer intento estaban verdes y no probaban
nada. Es el modo de falla por defecto cuando pides "escribe tests" sin
especificar contra qué oráculo: la IA usa el output actual como verdad. Lo detecté
al preguntarme qué test se pondría rojo si borrara la regla de los comprobantes
CFDI, y la respuesta era ninguno.

### Caso C — Una cifra inventada con total seguridad

Al redactar el README, la IA escribió que un `JOIN` directo contra `sku_mappings`
descartaría "~6% de las líneas de venta". El número sonaba razonable y estaba
escrito sin hedge alguno. Lo calculé: son **8,066 de 96,437 líneas, 8.4%**.

No cambia la conclusión, pero sí el hábito: **toda cifra que aparece en el README
o en la propuesta la recalculé contra los datos.** Hay un commit específico de
esa corrección (`fix: cuantifica con precisión las líneas que perdería un join
directo`). Una propuesta a un cliente con un número inventado es un problema
distinto y peor que un bug.

---

## 5. Auto-crítica

**Lo que considero 100% mío:** el juicio sobre los datos y sobre el negocio. Que
`"N/A"` no es cero y que además debe cortar la racha en vez de puentearla; que el
mapeo de SKU del ERP sirve mejor como oráculo de validación que como fuente de la
llave; que la respuesta estricta a la pregunta 2 —tres quiebres en toda la
cadena— sería **engañosa** entregada sola, y que hay que acompañarla de los 5,769
días-SKU con anaquel vacío; que la rotación se calcula sobre el canal físico
porque los snapshots no respaldan el inventario de Shopify; y la lista de qué
preguntarle al cliente contra qué resolver con criterio. También es mío el
reconocimiento de que **PySpark es sobre-ingeniería para 330 mil filas** y la
decisión de usarlo de todas formas con una justificación explícita —que el código
se mueve a Glue sin reescritura— en vez de fingir que era la elección obvia.

**Lo que es mérito de la IA:** la velocidad. El perfilado inicial que a mano me
habría tomado una hora salió en minutos; el andamiaje de las 61 pruebas (~860
líneas) y la redacción de toda la documentación del repo son trabajo que ella
hace mejor y más rápido que yo. También aportó dos observaciones que yo no había
hecho: que los cinco valores de `tipo_comprobante` corresponden al catálogo CFDI
4.0 del SAT, y que los tres sistemas comparten un consecutivo numérico en sus
identificadores. Verifiqué las dos antes de construir sobre ellas —el catálogo
contra la documentación del SAT, la convención numérica contra los 65 renglones
del mapeo explícito, donde coincide sin una sola excepción.

**Cómo validé que el output es correcto, más allá de que "corre sin error":** en
tres capas. Primero, **7 chequeos de calidad que abortan la corrida**, no que
advierten: si una línea de venta no aterriza en el catálogo, si una moneda queda
sin convertir o si el mapeo del ERP contradice la llave conforme, el pipeline se
detiene. Segundo, **49 tests de lógica sobre datos sintéticos con resultados que
verifiqué a mano** —no fixtures generados, casos de cinco filas donde puedo decir
cuál es la respuesta correcta sin correr nada. Tercero, y es lo que más peso
tiene, **12 tests que recalculan las cifras clave con pandas leyendo los archivos
fuente**, sin importar nada de `src/`: los 12 ingresos mensuales de cada canal,
las rachas de quiebre, el margen de cada producto y las unidades del top 10
coinciden en las dos rutas. Que un pipeline corra no prueba nada; que dos
implementaciones independientes lleguen al mismo número sí es evidencia.

**Lo que haría distinto con más tiempo.** Dos cosas me incomodan. Una: la llave
conforme derivada del consecutivo numérico funciona hoy y está protegida por un
chequeo, pero es una muleta sobre un problema de gobierno de datos del cliente;
la solución correcta vive en el ERP, no en mi pipeline. Dos: el pipeline
reprocesa todo en cada corrida. Con 330 mil filas tarda dos minutos y medio y no
duele, pero es deuda consciente, no una decisión que defendería a otra escala.
Y una advertencia honesta sobre este documento: el sesgo natural al escribir un
AI_LOG es inflar la parte propia. Delegué más de lo que sugeriría un log
defensivo —toda la suite de pruebas, toda la documentación y todo el tooling— y
la razón por la que puedo sostenerlo en una entrevista no es que haya escrito
menos, sino que leí y verifiqué todo lo que entró al repo.