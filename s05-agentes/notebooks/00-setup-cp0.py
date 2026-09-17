# Databricks notebook source
# /// script
# dependencies = ["mlflow==3.16.0", "databricks-openai==0.17.1", "databricks-mcp==0.9.2", "mcp==1.30.0", "jsonschema==4.23.0"]
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # S05 · 00 · Fundación compartida (CP0) — no es un patrón, es lo que usan los cuatro
# MAGIC Este notebook reemplaza al antiguo `notebook.py` único. Los cuatro patrones de S05 (chatbot,
# MAGIC pipeline, copiloto, agente) ahora viven en notebooks separados, uno por patrón:
# MAGIC 1. **`00-setup-cp0.py`** (este archivo) — catálogo, validación, caso reproducible. Sin esto no arranca nada.
# MAGIC 2. **`01-pipeline-uc-function-cp2.py`** — patrón **pipeline**: UC Functions.
# MAGIC 3. **`02-agente-responsesagent-cp3.py`** — patrón **agente**: `ResponsesAgent`, loop y trazas.
# MAGIC 4. **`03-conexiones-mcp-cp4.py`** — MCP managed/custom/external (infraestructura que consumen los tools).
# MAGIC 5. **`04-chatbot-genie-cp5.py`** — patrón **chatbot**: Genie como tool. **El Genie Space no se configura acá:
# MAGIC    se configura en la UI de Databricks** (Genie → New Space); este notebook solo lo consume.
# MAGIC 6. **`05-integracion-cierre-cp6.py`** — Agent Bricks/ALHF, pruebas E2E y entrega.
# MAGIC
# MAGIC El patrón **copiloto** (Claude Code) no tiene notebook: se configura en tu terminal (ver `CONSIGNA.md`, CP1).
# MAGIC
# MAGIC Cada notebook siguiente empieza con `%run ./0N-anterior` — corre el anterior en el mismo namespace
# MAGIC de Python y hereda sus variables (`CATALOGO`, `SCHEMA`, `EJEMPLO`, etc.). Puedes abrir cualquiera y
# MAGIC ejecutarlo de punta a punta: arrastra toda la cadena hasta acá.
# MAGIC
# MAGIC **Sobre los "CP":** son *checkpoints* — la unidad de evidencia que define `CONSIGNA.md` para cada
# MAGIC concepto (entrada → decisión → ejecución → resultado → aceptación). Es la convención de
# MAGIC `course-contract.md` (deck-craft) que empezamos a aplicar recién en S05; S01–S04 no la usan.
# MAGIC Ejecuta por bloques. La primera celda solo crea los campos; escribe tu catálogo antes de validar.

# COMMAND ----------

# Cada dbutils.widgets.text(nombre, valor_por_defecto, etiqueta) crea un campo editable
# arriba del notebook. Si el widget ya existe (por ejemplo porque volviste a ejecutar esta
# celda), Databricks CONSERVA el valor que ya escribiste — no lo pisa con el default.
dbutils.widgets.text("catalogo", "", "01 · Tu catálogo de S01–S02 (obligatorio)")  # vacío a propósito: es tuyo, no un valor del docente
dbutils.widgets.text("endpoint", "databricks-meta-llama-3-3-70b-instruct", "02 · Endpoint con tool calling")
dbutils.widgets.text("genie_space_id", "01f1a2b475f11570a24ad8fdd22efbf6", "03 · Genie curado Neptuno (catálogo neptuno_ai)")
dbutils.widgets.text("secret_scope", "", "04 · Scope opcional: solo ejercicio Secrets")
dbutils.widgets.text("secret_key", "", "05 · Key opcional: nunca escribas el secreto aquí")
print("Completa 'Tu catálogo de S01–S02' arriba. Luego ejecuta CP0.1; no ejecutes todo con el campo vacío.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CP0.1 · Validar configuración y datos
# MAGIC **Predice:** ¿qué debe suceder si el catálogo está vacío? Un error que indique cómo completarlo.
# MAGIC No creamos datos Neptuno alternativos. Se necesitan las tablas Gold de S02 en TU catálogo.
# MAGIC El espacio Genie docente consulta `neptuno_ai`; es un recurso compartido y se identifica explícitamente.
# MAGIC Dependencias del entorno serverless v5: `mlflow>=3.1,<4`, `databricks-openai`, `databricks-mcp`, `mcp==1.30.0`, `jsonschema`.
# MAGIC Añádelas en **Environment → Dependencies**, aplica y reinicia Python si el entorno lo solicita.

# COMMAND ----------

# Leemos los widgets UNA vez y los guardamos en variables Python en mayúsculas
# (CATALOGO, ENDPOINT, ...) — así el resto del notebook no vuelve a tocar dbutils.
import re, json, uuid, time
from datetime import datetime, timezone, timedelta
CATALOGO = dbutils.widgets.get("catalogo").strip()
ENDPOINT = dbutils.widgets.get("endpoint").strip()
GENIE_SPACE_ID = dbutils.widgets.get("genie_space_id").strip()

# Fallar rápido y con un mensaje claro es mejor que dejar que un catálogo vacío
# provoque un error críptico varias celdas más adelante.
if not CATALOGO:
    raise ValueError("Completa el widget 'Tu catálogo de S01–S02' y vuelve a ejecutar CP0.1.")
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", CATALOGO):
    raise ValueError("Usa un nombre simple de catálogo (letras, números y guion bajo).")
if not ENDPOINT or not re.fullmatch(r"[0-9a-fA-F]{32}", GENIE_SPACE_ID):
    raise ValueError("Completa endpoint y el ID de 32 caracteres del espacio Genie curado.")

# SCHEMA es TU espacio de trabajo dentro de tu propio catálogo: acá van a vivir las
# UC Functions y la tabla de evidencias que crea este curso, sin tocar el resto de tu catálogo.
SCHEMA = f"{CATALOGO}.s05_agentes"

# No basta con que las tablas EXISTAN: necesitan tener las columnas exactas que
# el resto del notebook va a usar. Lo comprobamos acá, antes de construir nada sobre ellas.
requeridas = {
 "ventas_por_categoria_mes": {"categoria", "mes", "ingreso_neto"},
 "inventario_disponible": {"NombreProducto", "UnidadesEnExistencia", "UnidadesEnPedido", "NivelNuevoPedido", "requiere_reposicion"},
}
for tabla, columnas in requeridas.items():
    fq = f"{CATALOGO}.gold.{tabla}"
    try:
        reales = set(spark.table(fq).columns)
    except Exception as exc:
        raise RuntimeError(f"No se puede leer {fq}. Revisa catálogo, permisos SELECT y entrega S02.") from exc
    if not columnas <= reales:  # columnas es subconjunto de reales
        raise ValueError(f"{fq}: faltan columnas {sorted(columnas-reales)}. Usa las tablas Gold de S02.")

# CREATE SCHEMA IF NOT EXISTS es idempotente: puedes correr esta celda mil veces
# sin que falle porque el schema "ya existe".
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA} COMMENT 'Laboratorio S05: funciones y evidencias del Copiloto Neptuno'")
print(f"CP0 OK · Datos: {CATALOGO}.gold · Objetos de clase: {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CP0.2 · Observar antes de preguntar
# MAGIC **Percepción:** primero reconocemos categorías y años reales; no suponemos 2025.
# MAGIC Elegimos un par con datos para que la prueba compare el agente con un resultado SQL independiente.

# COMMAND ----------

# Agregamos las ventas por categoría y año ANTES de tocar ningún LLM: es el resultado
# de referencia (calculado con SQL puro) contra el que vamos a comparar la tool en CP2.
perfil = spark.sql(f"""SELECT categoria, year(mes) anio,
 CAST(SUM(ingreso_neto) AS DECIMAL(18,2)) venta_neta
 FROM {CATALOGO}.gold.ventas_por_categoria_mes
 WHERE categoria IS NOT NULL AND mes IS NOT NULL
 GROUP BY categoria, year(mes) ORDER BY anio DESC, categoria""")
display(perfil)
filas = perfil.collect()
if not filas:
    raise ValueError("Gold no contiene ventas: completa S02 antes de S05.")

# EJEMPLO fija UN par categoría+año que sabemos que tiene datos reales en TU catálogo.
# Todo el resto del curso usa este par (nunca "Bebidas/2025" a ciegas) para que la demo
# funcione sin importar qué datos generaste en S02.
EJEMPLO = filas[0].asDict()
CATEGORIAS = sorted({r.categoria for r in filas})  # para validar que una categoría pedida exista de verdad
ANIOS = sorted({r.anio for r in filas})
print("Caso reproducible:", EJEMPLO)
print("CP0 completo · siguiente: 01-pipeline-uc-function-cp2.py")
