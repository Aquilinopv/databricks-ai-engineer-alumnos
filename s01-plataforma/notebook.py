# Databricks notebook source
# MAGIC %md
# MAGIC # Sesión 1 · La plataforma de datos desde cero
# MAGIC **Databricks AI Engineer** — caso Neptuno
# MAGIC
# MAGIC Al terminar este notebook vas a tener:
# MAGIC - tu propio catálogo gobernado en Unity Catalog,
# MAGIC - las 8 tablas de Neptuno en la capa **bronze**,
# MAGIC - y la red de seguridad de Delta funcionando (time travel).
# MAGIC
# MAGIC > 📏 **Regla de la casa:** bronze no se corrige, se re-procesa.

# COMMAND ----------

# MAGIC %md ## 0 · Tu identidad en el curso
# MAGIC Cada alumno trabaja en su propio catálogo para no pisarse.

# COMMAND ----------

import re, unicodedata

dbutils.widgets.text("alumno", "", "Tu nombre")
crudo = dbutils.widgets.get("alumno").strip()
assert crudo, "Escribe tu nombre en el widget de arriba antes de seguir."

def sanear(nombre: str) -> str:
    """Un catálogo de Unity Catalog no admite espacios ni tildes.
    'Demo Testing' -> 'demo_testing' · 'José Pérez' -> 'jose_perez'"""
    sin_tildes = unicodedata.normalize("NFKD", nombre).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-zA-Z0-9]+", "_", sin_tildes).strip("_").lower()

ALUMNO = sanear(crudo)
assert ALUMNO, "Pon tu nombre en el widget de arriba antes de seguir."

CATALOGO = f"neptuno_{ALUMNO}"
print(f"Hola, {crudo}.\nTu catálogo: {CATALOGO}")

# COMMAND ----------

# MAGIC %md ## 1 · Crear el catálogo, los esquemas y el Volume
# MAGIC La jerarquía de Unity Catalog: `metastore → catálogo → esquema → tabla | volume`.
# MAGIC
# MAGIC El **Volume** es almacenamiento de archivos *gobernado*: los CSV de hoy y los
# MAGIC documentos de la sesión 4 (RAG) van a vivir ahí.

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOGO}")
for capa in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{capa}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.bronze.landing")

LANDING = f"/Volumes/{CATALOGO}/bronze/landing"
print("Landing zone:", LANDING)
display(spark.sql(f"SHOW SCHEMAS IN {CATALOGO}"))

# COMMAND ----------

# MAGIC %md ## 2 · Traer los CSV de Neptuno a tu Volume
# MAGIC Los datos del curso viven en un volumen compartido, en modo lectura.
# MAGIC Esta celda los copia al tuyo; si ya están, no hace nada.

# COMMAND ----------

COMPARTIDO = "/Volumes/neptuno/ventas/landing"
ESPERADOS = ["categorias", "clientes", "detalles_pedidos", "empleados",
             "pedidos", "productos", "proveedores", "transportistas"]

ya_estan = {f.name for f in dbutils.fs.ls(LANDING)}
copiados = 0
for tabla in ESPERADOS:
    if f"{tabla}.csv" not in ya_estan:
        dbutils.fs.cp(f"{COMPARTIDO}/{tabla}.csv", f"{LANDING}/{tabla}.csv")
        copiados += 1
print(f"{copiados} archivos copiados · {len(ESPERADOS)-copiados} ya estaban")

# COMMAND ----------

# MAGIC %md ### Verificación: ¿están los ocho?

# COMMAND ----------

archivos = [f.name for f in dbutils.fs.ls(LANDING)]
print(f"{len(archivos)} archivos en el landing:")
for a in sorted(archivos):
    print(" ·", a)

faltan = [t for t in ESPERADOS if not any(t in a for a in archivos)]
assert not faltan, f"Faltan estos CSV en el landing: {faltan}"
print("\n✅ están los 8")

# COMMAND ----------

# MAGIC %md ## 3 · Bronze: cargar tal como llegó
# MAGIC Bronze guarda el dato **crudo**, más metadata de ingesta: de qué archivo vino y cuándo.
# MAGIC Esa metadata es lo que después permite responder «¿de dónde salió este número?».

# COMMAND ----------

from pyspark.sql import functions as F

for tabla in ESPERADOS:
    (spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{LANDING}/{tabla}.csv")
        # _metadata.file_path y no input_file_name(): esta última no está
        # permitida en Unity Catalog con acceso compartido.
        .withColumn("_archivo_origen", F.col("_metadata.file_path"))
        .withColumn("_ingesta_ts",    F.current_timestamp())
        .write.mode("overwrite")
        .saveAsTable(f"{CATALOGO}.bronze.{tabla}"))
    n = spark.table(f"{CATALOGO}.bronze.{tabla}").count()
    print(f"bronze.{tabla:20s} {n:>6,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación por efecto, no por fe
# MAGIC No alcanza con que el bloque no haya tirado error: hay que **contar**.

# COMMAND ----------

CONTEOS_ESPERADOS = {"categorias": 8, "clientes": 91, "detalles_pedidos": 2155,
                     "empleados": 9, "pedidos": 830, "productos": 77,
                     "proveedores": 29, "transportistas": 6}

errores = []
for tabla, esperado in CONTEOS_ESPERADOS.items():
    real = spark.table(f"{CATALOGO}.bronze.{tabla}").count()
    estado = "✅" if real == esperado else "❌"
    if real != esperado:
        errores.append((tabla, esperado, real))
    print(f"{estado} {tabla:20s} esperado {esperado:>6,} · real {real:>6,}")

assert not errores, f"Conteos que no cuadran: {errores}"
print("\n🎉 Neptuno está en tu bronze.")

# COMMAND ----------

# MAGIC %md ## 4 · Delta: la red de seguridad
# MAGIC Delta Lake es **Parquet + un log de transacciones**. De ese log sale todo lo demás:
# MAGIC transacciones ACID, versionado y *time travel*.

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY {CATALOGO}.bronze.productos").display()

# COMMAND ----------

# MAGIC %md ### El accidente (a propósito)
# MAGIC Alguien borra todas las bebidas. Sin backup.

# COMMAND ----------

# Guardamos la versión actual: si el notebook ya se corrió antes, la versión 0
# no es la carga limpia de HOY, y restaurar a 0 traería otro estado.
version_buena = (spark.sql(f"DESCRIBE HISTORY {CATALOGO}.bronze.productos")
                      .selectExpr("max(version) AS v").first()["v"])
antes = spark.table(f"{CATALOGO}.bronze.productos").count()
print(f"versión buena: {version_buena} · {antes} productos")
spark.sql(f"DELETE FROM {CATALOGO}.bronze.productos WHERE IdCategoria = 1")
despues = spark.table(f"{CATALOGO}.bronze.productos").count()
print(f"antes: {antes} · después del DELETE: {despues} · se fueron {antes - despues}")

# COMMAND ----------

# MAGIC %md ### Y vuelve

# COMMAND ----------

import time

def restaurar(tabla: str, version: int, intentos: int = 4):
    """Databricks puede lanzar un OPTIMIZE automático justo entre el borrado y la
    restauración. Cuando eso pasa, el RESTORE choca con esa transacción y falla con
    ConcurrentWriteException. No es un error nuestro: se reintenta y entra."""
    for i in range(1, intentos + 1):
        try:
            spark.sql(f"RESTORE TABLE {tabla} TO VERSION AS OF {version}")
            if i > 1:
                print(f"   (entró en el intento {i}: había una escritura automática en curso)")
            return True
        except Exception as err:
            if "CONCURRENT" not in str(err).upper() or i == intentos:
                raise
            time.sleep(2 * i)
    return False

restaurar(f"{CATALOGO}.bronze.productos", version_buena)
recuperado = spark.table(f"{CATALOGO}.bronze.productos").count()
print(f"después del RESTORE: {recuperado}")
assert recuperado == antes, "El restore no devolvió todas las filas"
print("✅ sin backup, sin drama")

# COMMAND ----------

# MAGIC %md ## 5 · La trampa del descuento
# MAGIC Siembra para la sesión 2. **No la resolvemos hoy.**
# MAGIC
# MAGIC El ingreso de una línea **no** es `PrecioUnidad * Cantidad`: hay un `Descuento`
# MAGIC que hay que restar. La diferencia es chica — y por eso llega a producción.

# COMMAND ----------

spark.sql(f"""
SELECT ROUND(SUM(PrecioUnidad * Cantidad), 2)                   AS ingreso_ingenuo,
       ROUND(SUM(PrecioUnidad * Cantidad * (1 - Descuento)), 2) AS ingreso_real,
       ROUND(100 * (1 - SUM(PrecioUnidad * Cantidad * (1 - Descuento))
                      / SUM(PrecioUnidad * Cantidad)), 2)       AS pct_de_error
FROM {CATALOGO}.bronze.detalles_pedidos
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC > **¿Cuánto da `pct_de_error`?** Ese número es el motivo por el que este curso existe.
# MAGIC > Nadie audita un error de esa magnitud. Y una IA que lee esta tabla sin saber que
# MAGIC > el descuento existe, lo va a cometer todas las veces.

# COMMAND ----------

# MAGIC %md ## 6 · Tu entregable
# MAGIC 1. Las 8 tablas en `bronze` ✅ (ya está)
# MAGIC 2. Tres consultas SQL que respondan preguntas de negocio reales
# MAGIC 3. Una tabla documentada con `COMMENT ON TABLE`
# MAGIC 4. Un cambio provocado y revertido con `RESTORE` ✅ (ya está)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 · Tres consultas de negocio
# MAGIC Estas tres están resueltas para que veas el patrón. La cuarta es tuya.

# COMMAND ----------

# MAGIC %md **1 · ¿Qué categoría vende más?** Fíjate que el ingreso lleva el descuento aplicado.

# COMMAND ----------

spark.sql(f"""
SELECT c.NombreCategoria                                             AS categoria,
       ROUND(SUM(d.PrecioUnidad * d.Cantidad * (1 - d.Descuento)), 2) AS ingreso_neto,
       SUM(d.Cantidad)                                                AS unidades
FROM {CATALOGO}.bronze.detalles_pedidos d
JOIN {CATALOGO}.bronze.productos  p ON d.IdProducto  = p.IdProducto
JOIN {CATALOGO}.bronze.categorias c ON p.IdCategoria = c.IdCategoria
GROUP BY 1 ORDER BY ingreso_neto DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC > 🔍 **Mira el número de Bebidas.** Es el mismo que el Genie del final de la clase
# MAGIC > va a llamar «margen». No es un margen: es la venta neta, con otro nombre.
# MAGIC > El margen necesitaría costos, y en Neptuno no hay tabla de costos.

# COMMAND ----------

# MAGIC %md **2 · ¿Qué cliente hace más pedidos?**

# COMMAND ----------

spark.sql(f"""
SELECT c.NombreCompania AS cliente, c.Pais, COUNT(*) AS pedidos
FROM {CATALOGO}.bronze.pedidos   p
JOIN {CATALOGO}.bronze.clientes  c ON p.IdCliente = c.IdCliente
GROUP BY 1, 2 ORDER BY pedidos DESC LIMIT 10
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **3 · ¿Qué transportista despacha más lento?**
# MAGIC Días entre el pedido y el envío. Ojo con las tres fechas: `FechaPedido` es cuándo se
# MAGIC vendió, `FechaEnvio` cuándo salió, y `FechaEntrega` es la fecha *comprometida*, no la real.

# COMMAND ----------

spark.sql(f"""
SELECT t.NombreCompania AS transportista,
       COUNT(*)                                                                   AS pedidos,
       ROUND(AVG(DATEDIFF(TO_DATE(p.FechaEnvio), TO_DATE(p.FechaPedido))), 1)     AS dias_promedio,
       MAX(DATEDIFF(TO_DATE(p.FechaEnvio), TO_DATE(p.FechaPedido)))               AS peor_caso
FROM {CATALOGO}.bronze.pedidos         p
JOIN {CATALOGO}.bronze.transportistas  t ON p.IdTransportista = t.IdTransportista
WHERE p.FechaEnvio IS NOT NULL
GROUP BY 1 ORDER BY dias_promedio DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC > 😏 Fíjate en los nombres frente a los números. El que más tarda y el que se llama
# MAGIC > «Expreso Veloz» no coinciden con lo que uno esperaría.

# COMMAND ----------

# MAGIC %md **4 · La tuya.** Escribe una consulta que responda algo que a ti te interese.

# COMMAND ----------

# TODO alumno — tu consulta


# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 · Documenta una tabla
# MAGIC Escribe lo que un colega nuevo necesitaría saber para **no** equivocarse con ella.
# MAGIC En la sesión 3 vas a descubrir que esto es exactamente lo que le falta al Genie del Demo 0.

# COMMAND ----------

# TODO alumno — ejemplo:
# spark.sql(f"""
# COMMENT ON TABLE {CATALOGO}.bronze.detalles_pedidos IS
# 'Hecho a nivel de línea: una fila = un producto dentro de un pedido. El ingreso NO es
#  PrecioUnidad*Cantidad: hay que aplicar el Descuento.'
# """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación final
# MAGIC Si la ejecución se cortó entre el borrado y la restauración, tus datos quedaron
# MAGIC incompletos. Esta celda lo detecta antes de que te vayas.

# COMMAND ----------

def verificacion_final():
    problemas = []
    for tabla, esperado in CONTEOS_ESPERADOS.items():
        real = spark.table(f"{CATALOGO}.bronze.{tabla}").count()
        if real != esperado:
            problemas.append(f"{tabla}: {real} filas, deberían ser {esperado}")
    if problemas:
        print("⚠️  Tus datos quedaron incompletos:")
        for p in problemas: print("   ·", p)
        print("\n   Vuelve a correr la celda de carga (sección 3) para dejarlos bien.")
    else:
        print("✅ Las 8 tablas están completas. Puedes cerrar tranquilo.")
    return not problemas

verificacion_final()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Antes de irte
# MAGIC **Apaga el compute.** Un cluster olvidado encendido es la forma más cara de no aprender nada.
# MAGIC
# MAGIC **La semana que viene:** hoy cargamos todo de una vez. En la vida real los datos llegan de a
# MAGIC poco, todos los días. En la sesión 2 el pipeline se entera solo de lo que llegó nuevo.
