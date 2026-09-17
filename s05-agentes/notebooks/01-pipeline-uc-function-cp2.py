# Databricks notebook source
# /// script
# dependencies = ["mlflow==3.16.0", "databricks-openai==0.17.1", "databricks-mcp==0.9.2", "mcp==1.30.0", "jsonschema==4.23.0"]
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # S05 · 01 · Patrón PIPELINE — UC Function (CP2)
# MAGIC **Quién decide los pasos: el código, de antemano.** Una UC Function es una unidad callable con
# MAGIC firma tipada, registrada en Unity Catalog. Acá es puramente determinística (SQL puro); en
# MAGIC `docs/PATRONES-AGENTICOS-EXPLICADO.html` (pestaña Pipeline) hay un ejemplo de la misma función
# MAGIC envolviendo un LLM con `ai_query()` — sigue siendo pipeline porque la secuencia la fija el código,
# MAGIC no el modelo.
# MAGIC
# MAGIC Requiere haber corrido antes `00-setup-cp0.py` (catálogo, `SCHEMA`, `EJEMPLO`, `CATEGORIAS`, `ANIOS`).
# MAGIC
# MAGIC **Si es la primera vez que abres ESTE notebook:** el widget de catálogo de abajo está vacío,
# MAGIC aunque ya lo hayas completado en `00-setup-cp0.py` — cada notebook tiene su propia copia del
# MAGIC widget. Ejecuta la celda siguiente, completa el campo que aparece arriba y vuelve a ejecutar.
# MAGIC
# MAGIC **Si te da `ModuleNotFoundError`:** el entorno serverless de ESTE notebook también es propio
# MAGIC — abre el panel **Environment** (arriba a la derecha), confirma que están `mlflow`,
# MAGIC `databricks-openai`, `databricks-mcp`, `mcp==1.30.0` y `jsonschema`, agrega la que falte,
# MAGIC **Apply** y reinicia Python antes de volver a ejecutar.

# COMMAND ----------

dbutils.widgets.text("catalogo", "", "01 · Tu catálogo de S01–S02 (obligatorio)")
dbutils.widgets.text("endpoint", "databricks-meta-llama-3-3-70b-instruct", "02 · Endpoint con tool calling")
dbutils.widgets.text("genie_space_id", "01f1a2b475f11570a24ad8fdd22efbf6", "03 · Genie curado Neptuno (catálogo neptuno_ai)")
dbutils.widgets.text("secret_scope", "", "04 · Scope opcional: solo ejercicio Secrets")
dbutils.widgets.text("secret_key", "", "05 · Key opcional: nunca escribas el secreto aquí")
print("Si el campo 'catalogo' está vacío arriba, complétalo y ejecuta esta celda de nuevo antes de seguir.")

# COMMAND ----------

# MAGIC %run ./00-setup-cp0

# COMMAND ----------

# MAGIC %md
# MAGIC ## CP2.1 · UC Function: contrato, COMMENT y lectura
# MAGIC **Decisión:** el LLM verá la descripción y los parámetros; `COMMENT` explica cuándo sirve la herramienta.
# MAGIC **Acción:** esta función devuelve JSON con procedencia y número de filas. `null` con cero filas no equivale a ventas cero.
# MAGIC El SQL está escrito por nosotros y lee Gold. El LLM nunca recibe un ejecutor de SQL libre.

# COMMAND ----------

# Creamos la función UNA vez, en SQL puro (sin ningún LLM adentro): dado categoría+año,
# calcula la venta neta real y la devuelve como JSON con su fuente. Cada COMMENT (de la
# función y de cada parámetro) es lo que el modelo va a leer más adelante para decidir
# si esta tool sirve para una pregunta — por eso son tan específicos, no genéricos.
spark.sql(f"""
CREATE OR REPLACE FUNCTION {SCHEMA}.ventas_categoria(
 p_categoria STRING COMMENT 'Categoría exacta indicada por el usuario; pedirla si falta',
 p_anio INT COMMENT 'Año calendario indicado por el usuario; pedirlo si falta'
) RETURNS STRING READS SQL DATA
COMMENT 'Lee venta neta con descuentos de una categoría y año en Neptuno. Requiere ambos parámetros. No calcula margen ni costos. Devuelve JSON con fuente, filas y venta_neta; null si no hay datos.'
RETURN SELECT to_json(named_struct(
 'categoria', p_categoria, 'anio', p_anio,
 'venta_neta', CAST(SUM(v.ingreso_neto) AS DECIMAL(18,2)),  -- la regla de negocio: SUM ya viene con descuento aplicado desde S02
 'filas', COUNT(*), 'fuente', '{CATALOGO}.gold.ventas_por_categoria_mes'))  -- fuente explícita: evidencia de dónde salió el número
FROM {CATALOGO}.gold.ventas_por_categoria_mes v
WHERE lower(v.categoria)=lower(p_categoria) AND year(v.mes)=p_anio
""")
print("Creada", f"{SCHEMA}.ventas_categoria")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CP2.2 · Segunda herramienta: reposición
# MAGIC Predice qué cambia al considerar unidades en camino. El contrato devuelve hasta 20 productos; esa cota es visible.
# MAGIC No emitimos órdenes de compra: una respuesta es una recomendación sustentada en inventario.

# COMMAND ----------

# Segunda tool, mismo patrón que la anterior pero SIN parámetros: siempre devuelve
# el mismo tipo de resultado (una lista acotada), así que no necesita que el modelo
# le pase nada — el modelo solo decide SI la llama, no CON QUÉ argumentos.
spark.sql(f"""
CREATE OR REPLACE FUNCTION {SCHEMA}.productos_reponer()
RETURNS STRING READS SQL DATA
COMMENT 'Lista hasta 20 productos Neptuno que requieren reposición considerando stock y unidades en camino. Solo lectura; no emite compras ni actualiza inventario.'
RETURN SELECT to_json(named_struct('fuente','{CATALOGO}.gold.inventario_disponible',
 'limite',20,'productos',collect_list(named_struct('producto',NombreProducto,
 'stock',UnidadesEnExistencia,'en_camino',UnidadesEnPedido,'punto_reorden',NivelNuevoPedido))))
FROM (SELECT * FROM {CATALOGO}.gold.inventario_disponible
 WHERE requiere_reposicion=true ORDER BY NombreProducto LIMIT 20)  -- LIMIT 20 explícito: el modelo debe saber que la lista puede estar recortada
""")
print("Creada", f"{SCHEMA}.productos_reponer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CP2.3 · Descubrir herramientas con UCFunctionToolkit
# MAGIC La descripción no se duplica manualmente: el toolkit lee la función registrada.
# MAGIC Inspecciona `name`, `description`, `required`. Compara con COMMENT antes de conectar el modelo.

# COMMAND ----------

import mlflow
import importlib.metadata
# Registramos qué versión de cada librería quedó realmente instalada — si algo falla
# más adelante por un cambio de API, esto es lo primero que hay que revisar.
VERSIONES = {p:importlib.metadata.version(p) for p in ["mlflow", "databricks-openai", "databricks-mcp", "mcp", "jsonschema"]}
print("VERSIONES_ENTORNO:", json.dumps(VERSIONES, sort_keys=True))
from databricks.sdk import WorkspaceClient
from databricks_openai import DatabricksOpenAI, DatabricksFunctionClient, UCFunctionToolkit
from jsonschema import validate
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import ResponsesAgentRequest, ResponsesAgentResponse
w = WorkspaceClient()
uc_client = DatabricksFunctionClient(client=w)
UC_NAMES = [f"{SCHEMA}.ventas_categoria", f"{SCHEMA}.productos_reponer"]

# ACÁ está el momento clave del patrón pipeline: UCFunctionToolkit LEE las dos funciones
# que acabamos de crear y genera automáticamente su descripción de "tool" (el mismo
# formato que espera un LLM con function calling) a partir del COMMENT y los tipos SQL.
# No escribimos esa descripción a mano en ningún lado — por eso no se puede desincronizar.
toolkit = UCFunctionToolkit(function_names=UC_NAMES, client=uc_client)
UC_TOOLS = toolkit.tools
assert len(UC_TOOLS) == 2
# El toolkit usa nombres UC normalizados; asociamos por el nombre real generado.
UC_MAP = {}
for fq in UC_NAMES:
    one = UCFunctionToolkit(function_names=[fq], client=uc_client).tools[0]
    UC_MAP[one["function"]["name"]] = fq
print(json.dumps(UC_TOOLS, indent=2, ensure_ascii=False))  # así ve el modelo cada tool: name, description, parameters

# llm es el cliente que va a usar el agente (notebook 02) para hablar con el modelo.
# experimento es donde MLflow va a guardar cada corrida y cada traza de hoy.
llm = DatabricksOpenAI(workspace_client=w)
usuario = w.current_user.me().user_name
experimento = mlflow.set_experiment(f"/Users/{usuario}/S05-Neptuno-Agentes")
mlflow.set_registry_uri("databricks-uc")  # cuando registremos el agente (S08), va a Unity Catalog, no al registry clásico
print("Experimento MLflow:", experimento.experiment_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CP2.4 · Probar la función sin agente
# MAGIC Si falla aquí, revisar función/permisos/Gold; el LLM no reparará la fuente.
# MAGIC La referencia SQL de CP0 permite comprobar numéricamente el resultado del cliente UC.

# COMMAND ----------

# Todavía SIN ningún LLM de por medio: llamamos la función directo con el cliente de
# Unity Catalog, y comparamos el resultado contra EJEMPLO (calculado en el notebook 00
# con SQL independiente). Si esto no coincide, el problema está en la función, no en
# el modelo — por eso lo probamos antes de conectarle un agente.
r_uc = uc_client.execute_function(function_name=UC_NAMES[0], parameters={
 "p_categoria": EJEMPLO["categoria"], "p_anio": EJEMPLO["anio"]})
if r_uc.error:
    raise RuntimeError(f"UC falló: {r_uc.error}. Revisa EXECUTE y acceso serverless.")
venta_json = json.loads(r_uc.value)
assert abs(float(venta_json["venta_neta"]) - float(EJEMPLO["venta_neta"])) < 0.01  # margen de 1 centavo por redondeo
assert venta_json["filas"] > 0
print("CP2 OK:", venta_json)
print("Patrón pipeline cerrado · siguiente: 02-agente-responsesagent-cp3.py")
