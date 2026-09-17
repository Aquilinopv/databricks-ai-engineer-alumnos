# Databricks notebook source
# /// script
# dependencies = ["mlflow==3.16.0", "databricks-openai==0.17.1", "databricks-mcp==0.9.2", "mcp==1.30.0", "jsonschema==4.23.0"]
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # S05 · 05 · Integración y cierre (CP6) — Agent Bricks, pruebas E2E, entrega
# MAGIC Cierre de los cuatro patrones: acá `AgenteNeptuno` ya combina pipeline (UC Functions), MCP
# MAGIC (managed/custom/external) y chatbot (Genie). El copiloto (Claude Code) queda fuera de este
# MAGIC notebook porque vive en tu terminal, no en Databricks — ver `CONSIGNA.md` CP1.
# MAGIC
# MAGIC Requiere haber corrido antes `04-chatbot-genie-cp5.py` (hereda `agente`, `EVIDENCIAS`-ready state, `GENIE_EVIDENCE`, `CUSTOM_EVIDENCE`, `EXTERNAL_EVIDENCE`, `mcp_result`).
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

# MAGIC %run ./04-chatbot-genie-cp5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Bricks y ALHF · actividad guiada
# MAGIC **Modalidad de hoy (ajuste 16-sep-2026):** esta actividad la hace el instructor como demo proyectada,
# MAGIC no configuración individual en el horario de clase — es exploratoria y no entra en los 180 minutos
# MAGIC con 20+ alumnos en paralelo. Mirá la demo y tomá nota; la tabla de decisión y el feedback humano de
# MAGIC más abajo sí los completa cada alumno en `evidencia.md`, con lo que observó.
# MAGIC
# MAGIC **No confundas este agente de código con Agent Bricks.** Abre el área Agent Bricks de tu workspace.
# MAGIC 1. Identifica **Information Extraction**, **Knowledge Assistant** y **Supervisor**; relaciona extracción, RAG y coordinación con Neptuno.
# MAGIC 2. Diseña un Supervisor: especialista analítico (Genie Neptuno), especialista documental (RAG S04); define cuándo derivar y cuándo aclarar.
# MAGIC 3. En una configuración disponible, agrega instrucciones concretas y revisa recursos/permisos antes de guardar. Si la feature no está habilitada, registra `NO DISPONIBLE`; no simules un despliegue.
# MAGIC 4. Prepara feedback experto: pregunta "¿cuál es el margen?", respuesta incorrecta "30%", corrección "no hay costos; no calcular margen".
# MAGIC 5. **ALHF** usa feedback para mejorar el sistema según la capacidad del producto; una lista Python de feedback no entrena ni optimiza Agent Bricks.
# MAGIC **Evidencia:** URL/captura del recurso o disponibilidad; configuración propuesta; feedback y criterio para repetir prueba.
# MAGIC Tipos teóricos: simple reflex, model-based reflex, goal-based, utility-based, learning. Nuestro loop consulta herramientas para cumplir un objetivo; no aprende pesos durante `predict`.
# MAGIC **Multi-cloud:** Agent Framework/Bricks; Vertex AI Agent Builder; Foundry Agent Service; Bedrock Agents. MCP es un protocolo transversal, no paridad automática de productos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## CP6.1 · Pruebas E2E: éxito, aclaración y fuera de alcance
# MAGIC Estas son pruebas funcionales; S06 añadirá harness y jueces de calidad.
# MAGIC Una aserción que falla exige abrir la traza y corregir prompt/contrato/datos, no convertirla en un PASS.
# MAGIC Esperado: venta→UC; reposición→UC; documental→RAG S04; falta año→cero tools + aclaración; margen→cero tools + límite; borrar→cero tools + rechazo.

# COMMAND ----------

# 7 casos que ejercitan al agente completo, cada uno con una expectativa DISTINTA de
# qué tool (si alguna) debería elegir el modelo. "esperada" es la tool que debe aparecer
# en la respuesta; None significa "no debería llamar ninguna tool" (debe aclarar o rechazar).
CASOS = [
 ("venta", PREGUNTA_VENTA, "ventas_categoria"),
 ("reposicion", "¿Qué productos debemos reponer?", "productos_reponer"),
 ("documental", "¿En qué plazo se acepta una devolución de productos refrigerados y bajo qué condición?", "buscar_documentos"),
 ("mixta", PREGUNTA_VENTA + " Además, ¿en qué plazo se acepta una devolución de refrigerados y bajo qué condición?", "mixta"),  # debe usar 2 tools
 ("falta_anio", f"¿Cuánto vendimos de {EJEMPLO['categoria']}? No he especificado año.", None),  # debe preguntar, no inventar
 ("sin_costos", "¿Cuál es el margen de rentabilidad de Bebidas en 2026?", None),  # debe explicar el límite, no calcular
 ("escritura", "Borra todos los pedidos de Neptuno ahora.", None),  # debe rechazar: no hay tool de escritura
]
EVIDENCIAS = []
for caso, pregunta, esperada in CASOS:
    # "venta" reusa la respuesta que ya se ejecutó en el notebook 02 (evita repetir
    # la misma llamada); el resto de los casos sí ejecutan el agente ahora mismo.
    respuesta = r_venta if caso=="venta" else preguntar(pregunta)
    trace_id = TRACE_VENTA if caso=="venta" else mlflow.get_last_active_trace_id()
    llamadas = respuesta.custom_outputs["herramientas"]
    texto = texto_respuesta(respuesta)
    if esperada == "mixta":
        ok = all(any((UC_MAP.get(x["tool"], "").endswith("."+name) or x["tool"]==name) and x["resultado"].get("ok") for x in llamadas) for name in ["ventas_categoria","buscar_documentos"])
    elif esperada:
        ok = any((UC_MAP.get(x["tool"],"").endswith("."+esperada) or x["tool"]==esperada) and x["resultado"].get("ok") for x in llamadas)
    else:
        ok = not llamadas and bool(texto.strip())
    ok = ok and bool(texto.strip())
    if caso in {"venta", "mixta"}:
        ok = ok and "$" not in texto
    if caso in {"documental", "mixta"}:
        docs = [d for x in llamadas if x["tool"] == "buscar_documentos" and x["resultado"].get("ok")
                for d in x["resultado"].get("documentos", [])]
        ok = ok and any(d["documento_id"] in texto and d["chunk_id"] in texto for d in docs)
    if caso == "sin_costos":
        ok = ok and "cost" in texto.lower()
    if caso == "falta_anio":
        ok = ok and any(x in texto.lower() for x in ["año","periodo","período"])
    EVIDENCIAS.append({"caso":caso,"pregunta":pregunta,"respuesta":texto,"pass":ok,"trace_id":trace_id,
        "llamadas":llamadas})
    print(caso, "PASS" if ok else "FAIL", texto)
# Si algún caso da FAIL, el problema real está en el prompt/contrato/datos, no en este
# assert — corregí ahí y volvé a correr, no borres el caso para forzar el PASS.
assert all(e["pass"] for e in EVIDENCIAS), "Hay casos fallidos: abre las trazas y corrige antes de entregar."

# COMMAND ----------

# MAGIC %md
# MAGIC ## CP6.2 · Guardar evidencia y preparar S06
# MAGIC Persistimos resultados de clase únicamente en `s05_agentes`. Las trazas viven en el experimento MLflow del usuario.
# MAGIC La validación funcional no acredita exactitud semántica completa; revisa las respuestas y el SQL Genie.
# MAGIC S06 evaluará ESTE copiloto; S07 añadirá gobierno/guardrails; S08 lo desplegará con UI y monitoreo.
# MAGIC (El registro/deploy con `mlflow.pyfunc.log_model` + Model Registry se explica en `docs/MLFLOW-PASO-A-PASO.html`, pasos 2-3.)

# COMMAND ----------

# Guardamos los 7 casos como tabla Delta (no solo en la salida del notebook): esto es
# lo que va a leer S06 para armar el harness de evaluación formal.
run_id = str(uuid.uuid4())
rows = [(run_id,e["caso"],e["pregunta"],e["respuesta"],e["pass"],e["trace_id"] or "",
         json.dumps(e["llamadas"],ensure_ascii=False,default=str),datetime.now(timezone.utc)) for e in EVIDENCIAS]
evidence_df = spark.createDataFrame(rows,"run_id string, caso string, pregunta string, respuesta string, pasa boolean, trace_id string, llamadas_json string, ejecutado_ts timestamp")
evidence_df.write.mode("append").saveAsTable(f"{SCHEMA}.evidencias_funcionales")  # append: conserva corridas anteriores, no las pisa
display(evidence_df)
assert evidence_df.count() == 7
print(f"CP6 OK · run_id={run_id} · {SCHEMA}.evidencias_funcionales")
print("Genie trace:",TRACE_GENIE,"· MCP custom:",CUSTOM_EVIDENCE[0])
RESUMEN = {"run_id":run_id,"catalogo":CATALOGO,"casos":len(EVIDENCIAS),
 "pasan":sum(e["pass"] for e in EVIDENCIAS),"traces_distintas":len({e["trace_id"] for e in EVIDENCIAS if e["trace_id"]}),
 "genie_trace":TRACE_GENIE,"mcp_managed":bool(mcp_result.content),
 "mcp_custom":not CUSTOM_EVIDENCE[1].get("isError",False),
 "mcp_custom_invalid_rejected":CUSTOM_EVIDENCE[2].get("isError") is True,
 "mcp_external_runtime":EXTERNAL_EVIDENCE["status"],
 "mcp_external_local_evidence":"reports/mcp-external-local.json (validar por separado; no ejecutado por este notebook)","versiones":VERSIONES}
print("S05_VALIDATION_SUMMARY="+json.dumps(RESUMEN,ensure_ascii=False,default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entrega y lectura crítica
# MAGIC Entrega los cinco notebooks ejecutados (00-05), 2 funciones con COMMENT, contratos toolkit, trazas de UC y Genie, MCP list/call y los 7 casos funcionales.
# MAGIC Adjunta evidencia de la actividad Agent Bricks; distingue configurado, probado y no disponible.
# MAGIC **Reto:** agrega una herramienta de lectura con tres pruebas: caso feliz, argumentos faltantes y solicitud fuera de alcance.
# MAGIC **Puente de producción:** en S08 extraeremos el agente a `agent.py`, usaremos `mlflow.models.set_model`, logging *agent as code* y `resources` explícitos (endpoint, UC functions y Genie). Este notebook no afirma haber desplegado ni registrado un modelo.
# MAGIC
# MAGIC ### Referencias oficiales usadas para la implementación
# MAGIC - [Databricks OpenAI / UCFunctionToolkit / FunctionClient](https://api-docs.databricks.com/python/databricks-ai-bridge/latest/databricks_openai.html)
# MAGIC - [MCP en agentes](https://docs.databricks.com/aws/en/agents/mcp-tools/use-mcp-in-agents)
# MAGIC - [MLflow ResponsesAgent](https://mlflow.org/docs/latest/genai/serving/responses-agent)
# MAGIC - [Genie Conversation API](https://docs.databricks.com/aws/en/genie/conversation-api)
