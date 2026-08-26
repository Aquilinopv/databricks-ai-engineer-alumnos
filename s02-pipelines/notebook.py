# Databricks notebook source
# MAGIC %md
# MAGIC # Sesión 2 · Pipelines de datos (ETL) que alimentan la IA
# MAGIC **Databricks AI Engineer** — caso Neptuno
# MAGIC
# MAGIC La semana pasada cargamos todo de una vez, a mano. Eso era un **script**.
# MAGIC Hoy construimos un **pipeline**: idempotente, incremental, observable y con calidad declarada.
# MAGIC
# MAGIC > 📏 El descuento se aplica **una vez, en silver**. Si vive en cada consulta,
# MAGIC > alguna consulta se lo va a olvidar — y va a ser la que vea el directorio.

# COMMAND ----------

dbutils.widgets.text("alumno", "", "Tu nombre")
ALUMNO = dbutils.widgets.get("alumno").strip().lower()
assert ALUMNO, "Poné tu nombre en el widget."

CATALOGO   = f"neptuno_{ALUMNO}"
LANDING    = f"/Volumes/{CATALOGO}/bronze/landing"
CHECKPOINT = f"{LANDING}/_checkpoints"
print(f"Catálogo: {CATALOGO}\nLanding : {LANDING}")

# COMMAND ----------

# MAGIC %md ## 1 · Silver: la regla de negocio vive acá
# MAGIC `ingreso_linea` se calcula **una sola vez**, con el descuento aplicado.
# MAGIC A partir de este momento, nadie más tiene que acordarse de restarlo.

# COMMAND ----------

from pyspark.sql import functions as F

detalles = (spark.table(f"{CATALOGO}.bronze.detalles_pedidos")
    .withColumn("PrecioUnidad", F.col("PrecioUnidad").cast("decimal(12,2)"))
    .withColumn("Cantidad",     F.col("Cantidad").cast("int"))
    .withColumn("Descuento",    F.col("Descuento").cast("double"))
    # ── la regla de negocio, en un solo lugar del sistema ──
    .withColumn("ingreso_linea",
                F.round(F.col("PrecioUnidad") * F.col("Cantidad") * (1 - F.col("Descuento")), 2))
    .drop("_archivo_origen"))

detalles.write.mode("overwrite").saveAsTable(f"{CATALOGO}.silver.detalles_pedidos")

spark.sql(f"""
COMMENT ON TABLE {CATALOGO}.silver.detalles_pedidos IS
'Líneas de pedido tipadas. ingreso_linea YA tiene el descuento aplicado: para calcular ventas
 se suma ingreso_linea, nunca PrecioUnidad*Cantidad.'
""")
display(spark.table(f"{CATALOGO}.silver.detalles_pedidos").limit(5))

# COMMAND ----------

# MAGIC %md ## 2 · Auto Loader: enterarse solo de lo que llegó nuevo
# MAGIC El **checkpoint** es donde el pipeline recuerda qué archivos ya procesó.
# MAGIC Es lo que hace que correrlo dos veces no duplique nada.

# COMMAND ----------

def ingerir_pedidos() -> int:
    """Procesa los archivos nuevos de pedidos/. Devuelve cuántas filas entraron."""
    destino = f"{CATALOGO}.bronze.pedidos_incremental"

    (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT}/pedidos_schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", True)
        .load(f"{LANDING}/pedidos/")
        .withColumn("_archivo_origen", F.col("_metadata.file_path"))
        .withColumn("_ingesta_ts",     F.current_timestamp())
     .writeStream
        .option("checkpointLocation", f"{CHECKPOINT}/pedidos")
        .trigger(availableNow=True)
        .toTable(destino)
     .awaitTermination())

    return spark.table(destino).count()

# COMMAND ----------

# MAGIC %md
# MAGIC > 🪤 **Antes de subir nada:** un subdirectorio dentro de un Volume **no se crea solo**.
# MAGIC > Si copiás a `.../landing/pedidos/` sin haberlo creado, falla con `no such directory`.
# MAGIC > Desde el CLI: `databricks fs mkdir dbfs:/Volumes/<cat>/bronze/landing/pedidos`.
# MAGIC > Desde el notebook: `dbutils.fs.mkdirs(f"{LANDING}/pedidos")`.

# COMMAND ----------

dbutils.fs.mkdirs(f"{LANDING}/pedidos")
dbutils.fs.mkdirs(f"{LANDING}/detalles")
print("subdirectorios listos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alternativa verificada: `COPY INTO`
# MAGIC Si Auto Loader no arranca (permisos de checkpoint, cluster sin streaming), `COPY INTO`
# MAGIC enseña **exactamente la misma idea** y está medido: 3 meses → 70 pedidos · segunda corrida
# MAGIC → **0 filas** · cae un mes → entran **26 exactas**.
# MAGIC
# MAGIC ```sql
# MAGIC COPY INTO <catalogo>.bronze.pedidos_inc
# MAGIC FROM '/Volumes/<catalogo>/bronze/landing/pedidos'
# MAGIC FILEFORMAT = CSV FORMAT_OPTIONS('header'='true')
# MAGIC COPY_OPTIONS('mergeSchema'='true')
# MAGIC ```
# MAGIC El registro de archivos ya procesados cumple el mismo papel que el checkpoint.

# COMMAND ----------

# MAGIC %md ### Paso 1 — primera corrida (los meses que ya están en el landing)

# COMMAND ----------

total_1 = ingerir_pedidos()
print(f"Después de la 1ª corrida: {total_1:,} pedidos")

# COMMAND ----------

# MAGIC %md ### Paso 2 — se corre otra vez, **sin tocar nada**
# MAGIC Si el total no cambia, el pipeline es idempotente. Ése es el punto entero.

# COMMAND ----------

total_2 = ingerir_pedidos()
print(f"Después de la 2ª corrida: {total_2:,} pedidos")
assert total_2 == total_1, "❌ Se duplicaron filas: el checkpoint no está funcionando"
print("✅ Corrió de nuevo y no entró nada. El pipeline es idempotente.")

# COMMAND ----------

# MAGIC %md ### Paso 3 — cae un mes más
# MAGIC 👉 **Copiá 3 archivos más** de `pedidos/` al landing (o pedíselo al instructor)
# MAGIC y corré la celda de abajo.

# COMMAND ----------

total_3 = ingerir_pedidos()
print(f"Después de que cayeron meses nuevos: {total_3:,} pedidos (+{total_3 - total_2:,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### La prueba de que nada se re-escribió
# MAGIC Las filas viejas conservan su `_ingesta_ts` original. Solo las nuevas traen uno nuevo.

# COMMAND ----------

spark.sql(f"""
SELECT DATE_TRUNC('SECOND', _ingesta_ts) AS tanda, COUNT(*) AS filas
FROM {CATALOGO}.bronze.pedidos_incremental
GROUP BY 1 ORDER BY 1
""").display()

# COMMAND ----------

# MAGIC %md ## 3 · Calidad: declarar las reglas, no confiar en ellas
# MAGIC Reglas de **negocio**, no de tipos. Una expectativa que falla en silencio hoy
# MAGIC es una alucinación de tu agente dentro de tres sesiones.

# COMMAND ----------

EXPECTATIVAS = {
    "descuento_en_rango":   "Descuento BETWEEN 0 AND 1",
    "cantidad_positiva":    "Cantidad > 0",
    "ingreso_no_negativo":  "ingreso_linea >= 0",
}

fallas = {}
for nombre, regla in EXPECTATIVAS.items():
    n = spark.sql(f"""
        SELECT COUNT(*) AS n FROM {CATALOGO}.silver.detalles_pedidos
        WHERE NOT ({regla})
    """).first()["n"]
    fallas[nombre] = n
    print(f"{'✅' if n == 0 else '❌'} {nombre:22s} violaciones: {n}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Integridad referencial
# MAGIC ¿Hay líneas de detalle sin pedido cabecera? El generador de lotes lo garantiza —
# MAGIC pero **una garantía que no se verifica no es una garantía**.

# COMMAND ----------

huerfanos = spark.sql(f"""
SELECT COUNT(*) AS n
FROM {CATALOGO}.silver.detalles_pedidos d
LEFT ANTI JOIN {CATALOGO}.bronze.pedidos p USING (IdPedido)
""").first()["n"]
print(f"{'✅' if huerfanos == 0 else '❌'} líneas de detalle huérfanas: {huerfanos}")

# COMMAND ----------

# MAGIC %md ## 4 · Change Data Feed
# MAGIC Delta puede decirte **qué filas cambiaron entre dos versiones**, no solo el estado final.
# MAGIC
# MAGIC 🔗 No es plomería opcional: el **Vector Search de la sesión 4 exige CDF** para
# MAGIC mantener el índice sincronizado con la tabla.

# COMMAND ----------

spark.sql(f"ALTER TABLE {CATALOGO}.silver.detalles_pedidos SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
v_antes = spark.sql(f"DESCRIBE HISTORY {CATALOGO}.silver.detalles_pedidos").first()["version"]

spark.sql(f"""
UPDATE {CATALOGO}.silver.detalles_pedidos
SET Descuento = 0.25 WHERE IdPedido = (SELECT MIN(IdPedido) FROM {CATALOGO}.silver.detalles_pedidos)
""")

spark.sql(f"""
SELECT IdPedido, IdProducto, Descuento, ingreso_linea, _change_type
FROM table_changes('{CATALOGO}.silver.detalles_pedidos', {v_antes + 1})
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC > ⚠️ Fijate que `ingreso_linea` **no** se recalculó: el `UPDATE` tocó `Descuento` a mano,
# MAGIC > por fuera del pipeline. Ésa es exactamente la razón por la que las reglas de negocio
# MAGIC > tienen que vivir en el pipeline y no en updates sueltos.

# COMMAND ----------

# MAGIC %md ## 5 · Gold: la capa que la IA sí puede leer
# MAGIC Cada tabla gold **elimina una trampa concreta** de las que vimos en el Demo 0.

# COMMAND ----------

# MAGIC %md ### 5.1 Ventas — mata el error del descuento y la fecha equivocada

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.gold.ventas_por_categoria_mes AS
SELECT c.NombreCategoria                        AS categoria,
       DATE_TRUNC('MONTH', p.FechaPedido)       AS mes,
       ROUND(SUM(d.ingreso_linea), 2)           AS ingreso_neto,
       SUM(d.Cantidad)                          AS unidades,
       COUNT(DISTINCT p.IdPedido)               AS pedidos
FROM {CATALOGO}.silver.detalles_pedidos d
JOIN {CATALOGO}.bronze.pedidos    p ON d.IdPedido   = p.IdPedido
JOIN {CATALOGO}.bronze.productos  pr ON d.IdProducto = pr.IdProducto
JOIN {CATALOGO}.bronze.categorias c ON pr.IdCategoria = c.IdCategoria
GROUP BY 1, 2
""")

spark.sql(f"""
COMMENT ON TABLE {CATALOGO}.gold.ventas_por_categoria_mes IS
'Ventas netas por categoría y mes. ingreso_neto YA tiene el descuento aplicado y usa FechaPedido
 (cuándo se vendió), no FechaEnvio. NO existe información de costos: esta tabla no permite
 calcular margen ni rentabilidad.'
""")
display(spark.table(f"{CATALOGO}.gold.ventas_por_categoria_mes").orderBy("mes").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC > 🔑 Leé el último renglón del `COMMENT`: **declara lo que la tabla NO puede responder.**
# MAGIC > Eso es lo que le faltaba al Genie del Demo 0. Un modelo que lee «no existe información de
# MAGIC > costos» contesta *«no puedo»*. Uno que no lo lee, **improvisa un número distinto cada vez**.

# COMMAND ----------

# MAGIC %md ### 5.2 Inventario — mata la columna olvidada

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.gold.inventario_disponible AS
SELECT p.IdProducto, p.NombreProducto, c.NombreCategoria AS categoria,
       p.UnidadesEnExistencia, p.UnidadesEnPedido, p.NivelNuevoPedido,
       p.UnidadesEnExistencia + p.UnidadesEnPedido AS disponible_total,
       (p.UnidadesEnExistencia + p.UnidadesEnPedido) < p.NivelNuevoPedido AS requiere_reposicion
FROM {CATALOGO}.bronze.productos p
JOIN {CATALOGO}.bronze.categorias c ON p.IdCategoria = c.IdCategoria
WHERE p.Suspendido = 0
""")

spark.sql(f"""
COMMENT ON TABLE {CATALOGO}.gold.inventario_disponible IS
'Disponibilidad real de producto. disponible_total suma existencias MÁS UnidadesEnPedido (mercadería
 ya comprada al proveedor). Un producto sin stock físico pero con unidades en pedido NO requiere
 reposición. Excluye productos suspendidos.'
""")

spark.sql(f"""
SELECT COUNT(*) FILTER (WHERE UnidadesEnExistencia < NivelNuevoPedido) AS alerta_ingenua,
       COUNT(*) FILTER (WHERE requiere_reposicion)                     AS alerta_correcta
FROM {CATALOGO}.gold.inventario_disponible
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC > Los dos números no coinciden. **La diferencia son órdenes de compra que no hacían falta.**

# COMMAND ----------

# MAGIC %md ### 5.3 Meses completos — mata la comparación injusta

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.gold.ventas_mes_completo AS
WITH por_mes AS (
  SELECT DATE_TRUNC('MONTH', FechaPedido) AS mes,
         MAX(FechaPedido)                 AS ultimo_pedido,
         COUNT(*)                         AS pedidos
  FROM {CATALOGO}.bronze.pedidos GROUP BY 1
), corte AS (SELECT MAX(mes) AS mes_final FROM por_mes)
SELECT m.mes, m.pedidos, m.ultimo_pedido,
       m.mes < c.mes_final AS mes_completo
FROM por_mes m CROSS JOIN corte c
""")

spark.sql(f"""
COMMENT ON TABLE {CATALOGO}.gold.ventas_mes_completo IS
'Pedidos por mes con la marca mes_completo. El último mes del dataset está PARCIAL: compararlo
 contra un mes completo produce una caída falsa. Toda comparación temporal debe filtrar
 mes_completo = true.'
""")
display(spark.table(f"{CATALOGO}.gold.ventas_mes_completo").orderBy(F.col("mes").desc()).limit(4))

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 Mirá el último mes contra el anterior. Esa caída **no es del negocio: es un mes que no
# MAGIC > terminó.** Gold lo declara con una columna en vez de esperar que el analista se dé cuenta.

# COMMAND ----------

# MAGIC %md ## 6 · Tu entregable
# MAGIC 1. Pipeline incremental corriendo con **los 23 meses** cargados en tandas
# MAGIC 2. **Una expectativa propia** + una fila que la viole a propósito
# MAGIC 3. **Una tabla gold más**, con su regla de negocio en el `COMMENT`
# MAGIC 4. El job agendado para que corra solo
# MAGIC
# MAGIC ---
# MAGIC **La semana que viene:** ya tenemos datos confiables. Entra el primer LLM — y le volvemos
# MAGIC a hacer al Genie la pregunta del margen, pero apuntando a **nuestro gold**.
# MAGIC
# MAGIC **Apagá el compute.**

# COMMAND ----------

# TODO alumno — tu expectativa


# COMMAND ----------

# TODO alumno — tu tabla gold

