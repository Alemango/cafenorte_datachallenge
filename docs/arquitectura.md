# Arquitectura productiva propuesta (AWS)

![Arquitectura AWS propuesta](arquitectura.png)

Fuente del diagrama: [`arquitectura.mmd`](arquitectura.mmd) (Mermaid).
Para regenerarlo:

```bash
mmdc -i docs/arquitectura.mmd -o docs/arquitectura.png -b white -w 1700
```

## Correspondencia entre la prueba de concepto y producción

| Prueba de concepto (este repo) | Producción (AWS) |
|---|---|
| `data/raw/` en disco | `s3://cafenorte-lake/raw/` |
| `SparkSession` local | AWS Glue 4.0 (Spark 3.3+) |
| `outputs/lake/{bronze,silver,gold}` | `s3://cafenorte-lake/{bronze,silver,gold}/` |
| `pipeline.py` (argparse) | Step Functions + EventBridge Scheduler |
| `quality/expectations.py` | el mismo módulo, dentro del job de Glue |
| `outputs/reportes/*.csv` | tablas registradas en Glue Data Catalog → Athena |
| lectura manual de los CSV | QuickSight (SPICE, refresco diario) |

Los módulos `transform/`, `marts/` y `quality/` no cambian: sólo cambian
`spark.py` (creación de la sesión) y las rutas de `config.py`. Ésa es la razón
principal por la que la prueba de concepto se escribió en PySpark y no en un
motor de un solo nodo.

## Notas de decisión

**Por qué dos jobs de Glue y no uno.** Separar `bronze→silver` de `silver→gold`
permite reprocesar los marts —que es donde cambian las reglas de negocio— sin
volver a normalizar todo el histórico, y deja un punto de reintento más fino
cuando falla una corrida.

**Por qué Athena y no Redshift.** Con ~1.5M de renglones al año, Parquet
particionado en S3 responde las consultas del tablero en segundos y cuesta
centavos. Redshift Serverless empezaría a cobrar más que toda la plataforma
junta. La migración futura es incremental porque el formato ya es el correcto.

**Por qué el catálogo lo registra el job y no un crawler.** Un crawler diario
cuesta ~$1.30/mes y agrega una pieza móvil que puede inferir un esquema
distinto al que el job acaba de escribir. Registrar las particiones desde el
propio job es determinista y gratis.
