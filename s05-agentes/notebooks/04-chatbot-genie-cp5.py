# Databricks notebook source
# /// script
# dependencies = ["mlflow==3.16.0", "databricks-openai==0.17.1", "databricks-mcp==0.9.2", "mcp==1.30.0", "jsonschema==4.23.0"]
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # S05 · 04 · Patrón CHATBOT — Genie como tool (CP5)
# MAGIC **Quién decide los pasos: la persona, conversando.** A diferencia de los otros tres patrones,
# MAGIC **el Genie Space NO se configura en un notebook.** Se configura en la UI de Databricks
# MAGIC (**Genie → New Space**: eliges catálogo/schema/tablas, instrucciones, SQL curado y sinónimos —
# MAGIC ver `docs/PATRONES-AGENTICOS-EXPLICADO.html`, pestaña Chatbot). El espacio docente
# MAGIC **Ventas Neptuno AI** ya existe y usa el catálogo `neptuno_ai` — no el tuyo.
# MAGIC
# MAGIC Este notebook solo hace una cosa: **consumir** ese Space ya configurado como una tool más del
# MAGIC agente del notebook 02, igual que hicimos con las UC Functions en el 01.
# MAGIC
# MAGIC Requiere haber corrido antes `03-conexiones-mcp-cp4.py` (hereda `agente`, `TOOL_DEFINITIONS`, `EXTRA_HANDLERS`, `preguntar`).
# MAGIC
# MAGIC **Si es la primera vez que abres ESTE notebook:** completa el widget de catálogo que aparece
# MAGIC al ejecutar la celda siguiente — cada notebook tiene su propia copia, no la hereda de otro.
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

# MAGIC %run ./03-conexiones-mcp-cp4

# COMMAND ----------

# MAGIC %md
# MAGIC ## CP5.1 · Genie como herramienta analítica real
# MAGIC La UC Function responde una pregunta fija; Genie interpreta una pregunta analítica en un espacio curado.
# MAGIC Este espacio docente **Ventas Neptuno AI** usa `neptuno_ai`, aunque tus funciones usen otro catálogo.
# MAGIC Inspecciona pregunta → SQL generado → filas → fuente. El SQL lo ejecuta Genie con sus permisos, no nuestro ejecutor libre.

# COMMAND ----------

# consultar_genie NO le pide a Genie que "ejecute SQL": le manda la pregunta en
# lenguaje natural, como en el chat de la UI, y Genie decide y corre el SQL con SUS
# propios permisos (no los del ejecutor de UC Functions). Acá solo recogemos evidencia:
# el SQL generado y el resultado, para poder citarlos después.
@mlflow.trace(span_type="TOOL")
def consultar_genie(pregunta: str):
    if not isinstance(pregunta,str) or not 8 <= len(pregunta) <= 500:
        raise ValueError("Pregunta Genie: entre 8 y 500 caracteres")
    # start_conversation_and_wait bloquea hasta que Genie termine de responder
    # (hasta 3 minutos) — no hay forma de "sondear" más rápido este endpoint.
    msg = w.genie.start_conversation_and_wait(space_id=GENIE_SPACE_ID,content=pregunta,timeout=timedelta(minutes=3))
    attachments = []
    for a in msg.attachments or []:
        d = a.as_dict()
        if a.query:
            # Si la respuesta trae una query, pedimos el resultado real del SQL —
            # no basta con el texto de la respuesta, queremos el statement ejecutado.
            qr = w.genie.get_message_attachment_query_result(space_id=GENIE_SPACE_ID,
                conversation_id=msg.conversation_id,message_id=msg.id,attachment_id=a.attachment_id)
            sql_response = qr.as_dict().get("statement_response", {})
            state = sql_response.get("status", {}).get("state")
            if state != "SUCCEEDED":
                raise RuntimeError(f"Genie SQL no completó: {state}. Abre la conversación y revisa warehouse/permisos.")
            d["query"] = {k:v for k,v in d.get("query",{}).items() if k != "thoughts"}
            d["resultado_sql"] = {"statement_response":sql_response}
        attachments.append(d)
    if not attachments:
        raise RuntimeError("Genie no devolvió evidencia. Revisa espacio, warehouse y permisos.")
    return {"ok":True,"fuente":"Genie Ventas Neptuno AI / neptuno_ai", "space_id":GENIE_SPACE_ID,
            "conversation_id":msg.conversation_id,"attachments":attachments}

# Primera prueba: llamamos consultar_genie DIRECTO (sin pasar por el agente todavía),
# para verificar que el Space responde y trae SQL antes de conectarlo como tool.
GENIE_QUESTION = "¿Cuál es la venta neta total de abril de 2026? Muestra el SQL."
GENIE_EVIDENCE = consultar_genie(GENIE_QUESTION)
print(json.dumps(GENIE_EVIDENCE,ensure_ascii=False,indent=2,default=str)[:16000])
assert any(a.get("query") for a in GENIE_EVIDENCE["attachments"]), "Genie no generó SQL; revisa pregunta y espacio"

# COMMAND ----------

# MAGIC %md
# MAGIC ## CP5.2 · Incorporar Genie a la allowlist
# MAGIC Agregamos un contrato y su manejador. El LLM elige cuándo usarlo; no hay router por palabras clave.
# MAGIC La prueba exige observar una llamada real a `consultar_genie` y una respuesta basada en el espacio identificado.

# COMMAND ----------

# Mismo patrón que buscar_documentos en el notebook 02: registramos el contrato (para
# que el modelo la vea y decida cuándo usarla) y el handler real. Ahora el agente tiene
# 4 tools disponibles: ventas_categoria, productos_reponer, buscar_documentos, consultar_genie.
TOOL_DEFINITIONS["consultar_genie"] = {"type":"function","function":{
 "name":"consultar_genie",
 "description":"Consulta análisis agregados de ventas Neptuno en el espacio docente curado neptuno_ai. Úsala para total mensual, comparaciones y preguntas analíticas que no cubren ventas_categoria. Solo lectura. No usar para margen/costos, escritura o preguntas sin período cuando lo necesitan.",
 "parameters":{"type":"object","properties":{"pregunta":{"type":"string","minLength":8,"maxLength":500}},"required":["pregunta"],"additionalProperties":False}}}
EXTRA_HANDLERS["consultar_genie"] = consultar_genie
# Esta vez SÍ pasamos por el agente completo (preguntar, no consultar_genie directo):
# el modelo tiene que elegir SOLO por su cuenta que esta pregunta necesita Genie.
r_genie = preguntar("Usa el espacio Genie curado: ¿cuál fue la venta neta total de abril de 2026? Identifica el catálogo fuente.")
print(texto_respuesta(r_genie))
assert any(x["tool"]=="consultar_genie" and x["resultado"].get("ok") for x in r_genie.custom_outputs["herramientas"])
TRACE_GENIE = mlflow.get_last_active_trace_id()
print("Los cuatro patrones ya están combinados en `agente`: pipeline + agente + MCP + chatbot.")
print("Siguiente: 05-integracion-cierre-cp6.py")
