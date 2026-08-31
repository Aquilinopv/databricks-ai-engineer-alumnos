# Databricks notebook source
# MAGIC %md
# MAGIC # S02 · Apéndice opcional: Lakeflow Pipelines, expectations y Jobs
# MAGIC **Para quienes quieran ir un paso más allá.** Este notebook no es requisito para S03–S08.
# MAGIC
# MAGIC En el notebook principal construimos el flujo con Spark y validamos la calidad contando
# MAGIC incumplimientos. Aquí conectamos ese trabajo con las herramientas administradas de Databricks:
# MAGIC
# MAGIC 1. **Expectations:** reglas declarativas con métricas automáticas.
# MAGIC 2. **Lakeflow Spark Declarative Pipelines:** declaramos qué tablas deben existir.
# MAGIC 3. **Lakeflow Jobs:** programamos y observamos la ejecución de notebooks y pipelines.
# MAGIC
# MAGIC > ⚠️ Las celdas normales de este apéndice sí se ejecutan como notebook. El bloque con
# MAGIC > decoradores `@dp...` debe copiarse al **editor de un pipeline**; está marcado claramente.

# COMMAND ----------

# MAGIC %md ## 0 · Conectar con tu catálogo de S01
# MAGIC Ejecuta esta celda y escribe el nombre completo, por ejemplo `neptuno_manuel_arguelles`.

# COMMAND ----------

# Si un Job inyectó el parámetro "catalogo", conservamos ese valor.
# Si abriste el notebook manualmente, creamos el widget visible.
try:
    dbutils.widgets.text("catalogo", "", "Tu catálogo de la S01")
except Exception:
    pass

print("✅ Escribe arriba el catálogo completo si ejecutas el notebook manualmente.")

# COMMAND ----------

import re

CATALOGO = dbutils.widgets.get("catalogo").strip().lower()
assert CATALOGO, "Escribe tu catálogo completo, por ejemplo: neptuno_manuel_arguelles"
assert re.fullmatch(r"neptuno_[a-z0-9_]+", CATALOGO), (
    "El catálogo debe comenzar por neptuno_ y usar minúsculas, números o guiones bajos."
)

catalogos = {fila.catalog.lower() for fila in spark.sql("SHOW CATALOGS").collect()}
assert CATALOGO in catalogos, f"No existe el catálogo '{CATALOGO}' en este workspace."

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.lab_lakeflow")
print(f"✅ Usaremos {CATALOGO}.silver como fuente y {CATALOGO}.lab_lakeflow como laboratorio.")

# COMMAND ----------

# MAGIC %md ## 1 · Lo que hicimos en el notebook principal
# MAGIC Una regla escrita en lenguaje natural sirve para documentar; la condición SQL es la que
# MAGIC realmente evalúa cada fila. Aquí las reunimos en un diccionario para que sean reutilizables.

# COMMAND ----------

REGLAS = {
    "descuento_en_rango": "Descuento BETWEEN 0 AND 1",
    "cantidad_positiva": "Cantidad > 0",
    "ingreso_no_negativo": "ingreso_linea >= 0",
}

resultados = []
for nombre, condicion_sql in REGLAS.items():
    violaciones = spark.sql(f"""
        SELECT COUNT(*) AS n
        FROM {CATALOGO}.silver.detalles_pedidos
        WHERE NOT ({condicion_sql})
    """).first()["n"]
    resultados.append((nombre, condicion_sql, violaciones))

display(spark.createDataFrame(resultados, ["regla", "condicion_sql", "violaciones"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ¿Qué agrega una expectation?
# MAGIC La condición sigue siendo SQL. La diferencia es que Lakeflow la conecta al flujo y registra
# MAGIC automáticamente cuántas filas cumplen o incumplen durante cada actualización.
# MAGIC
# MAGIC - `@dp.expect(...)`: registra la violación y conserva la fila.
# MAGIC - `@dp.expect_or_drop(...)`: registra la violación y descarta la fila.
# MAGIC - `@dp.expect_or_fail(...)`: detiene la actualización ante una violación.
# MAGIC
# MAGIC La decisión no es técnica: depende del impacto de permitir un dato malo o perder una fila.

# COMMAND ----------

# MAGIC %md ## 2 · Laboratorio opcional: crear un pipeline declarativo
# MAGIC
# MAGIC ### 2.1 Crear el pipeline en la interfaz
# MAGIC
# MAGIC 1. Ve a **Jobs & Pipelines**.
# MAGIC 2. Selecciona **Create → ETL pipeline**.
# MAGIC 3. Usa edición avanzada y crea una transformación **Python**.
# MAGIC 4. Configura como destino:
# MAGIC    - **Catalog:** tu catálogo `neptuno_...`
# MAGIC    - **Schema:** `lab_lakeflow`
# MAGIC 5. En la configuración del pipeline agrega:
# MAGIC    - Key: `catalogo_neptuno`
# MAGIC    - Value: el nombre completo de tu catálogo.
# MAGIC 6. Reemplaza el código de la transformación por el bloque siguiente.
# MAGIC
# MAGIC > Este bloque **no se ejecuta en el notebook**. Lakeflow interpreta los decoradores,
# MAGIC > construye el grafo y materializa la tabla cuando pulsas **Run pipeline**.

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC
# MAGIC CATALOGO = spark.conf.get("catalogo_neptuno")
# MAGIC
# MAGIC REGLAS = {
# MAGIC     "descuento_en_rango": "Descuento BETWEEN 0 AND 1",
# MAGIC     "cantidad_positiva": "Cantidad > 0",
# MAGIC     "ingreso_no_negativo": "ingreso_linea >= 0",
# MAGIC }
# MAGIC
# MAGIC @dp.materialized_view(
# MAGIC     name="detalles_pedidos_validados",
# MAGIC     comment="Detalles de pedidos con métricas declarativas de calidad.",
# MAGIC )
# MAGIC @dp.expect_all(REGLAS)
# MAGIC def detalles_pedidos_validados():
# MAGIC     return spark.read.table(f"{CATALOGO}.silver.detalles_pedidos")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Qué debes observar
# MAGIC
# MAGIC Después de ejecutar el pipeline:
# MAGIC
# MAGIC 1. El grafo muestra la vista `detalles_pedidos_validados`.
# MAGIC 2. La tabla aparece en `<tu_catalogo>.lab_lakeflow`.
# MAGIC 3. Las métricas de calidad muestran cuántas filas pasaron y fallaron cada expectation.
# MAGIC 4. El linaje conecta `silver.detalles_pedidos` con la nueva vista.
# MAGIC
# MAGIC Prueba opcional: cambia una expectation de `@dp.expect_all` por una regla individual con
# MAGIC `@dp.expect_or_drop` o `@dp.expect_or_fail`. Hazlo solo en el laboratorio, nunca sobre las
# MAGIC tablas que usarán las sesiones siguientes.

# COMMAND ----------

# MAGIC %md ## 3 · Laboratorio opcional: crear un Lakeflow Job
# MAGIC Un **pipeline** define datasets y dependencias de datos. Un **Job** orquesta tareas:
# MAGIC notebooks, pipelines, scripts o consultas, y decide cuándo y en qué orden correrlas.
# MAGIC
# MAGIC ### Opción rápida desde este notebook
# MAGIC
# MAGIC 1. Pulsa **Schedule** en la esquina superior derecha.
# MAGIC 2. Crea un Job con ejecución manual o una frecuencia de prueba.
# MAGIC 3. En los parámetros del notebook agrega:
# MAGIC    - Key: `catalogo`
# MAGIC    - Value: el nombre completo de tu catálogo.
# MAGIC 4. Guarda y pulsa **Run now**.
# MAGIC 5. Abre la ejecución y revisa duración, salida y estado.
# MAGIC
# MAGIC ### Opción completa desde Jobs & Pipelines
# MAGIC
# MAGIC 1. Ve a **Jobs & Pipelines → Create → Job**.
# MAGIC 2. Agrega una tarea **Notebook** y selecciona este apéndice.
# MAGIC 3. Define el parámetro `catalogo` con tu catálogo completo.
# MAGIC 4. Usa **serverless** si está disponible; si no, selecciona Jobs compute.
# MAGIC 5. En opciones avanzadas configura un reintento.
# MAGIC 6. Agrega una notificación de fallo si tu workspace permite correo.
# MAGIC 7. Ejecuta con **Run now** y revisa el historial.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Extensión: un Job con dos tareas
# MAGIC Si creaste el pipeline del bloque 2:
# MAGIC
# MAGIC 1. Agrega una tarea **Pipeline** que ejecute ese pipeline.
# MAGIC 2. Agrega después una tarea **Notebook** que ejecute este apéndice.
# MAGIC 3. Dibuja la dependencia `pipeline → validación`.
# MAGIC
# MAGIC Esa flecha es orquestación: la segunda tarea empieza únicamente si la primera termina bien.
# MAGIC En producción también configuraríamos horarios, reintentos, alertas y permisos.

# COMMAND ----------

# MAGIC %md ## 4 · Qué conservar para el resto del curso
# MAGIC
# MAGIC No necesitas conservar activo este pipeline ni su Job. Las sesiones posteriores usan las
# MAGIC tablas creadas por `notebook.py`, especialmente Silver, Gold y Change Data Feed.
# MAGIC
# MAGIC Quédate con esta distinción:
# MAGIC
# MAGIC - **Spark:** describe las transformaciones.
# MAGIC - **Expectations:** declaran y miden reglas de calidad dentro de Lakeflow.
# MAGIC - **Pipeline:** construye datasets respetando sus dependencias.
# MAGIC - **Job:** decide cuándo ejecutar tareas y qué hacer si fallan.
# MAGIC
# MAGIC Documentación oficial:
# MAGIC
# MAGIC - [Desarrollar pipelines con Python](https://docs.databricks.com/aws/en/ldp/developer/python-dev)
# MAGIC - [Expectations de calidad](https://docs.databricks.com/aws/en/ldp/expectations)
# MAGIC - [Configurar Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs/configure-job)
