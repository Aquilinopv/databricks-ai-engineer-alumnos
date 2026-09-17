# Databricks notebook source
# /// script
# dependencies = ["mlflow==3.16.0", "databricks-openai==0.17.1", "databricks-mcp==0.9.2", "mcp==1.30.0", "jsonschema==4.23.0"]
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # S05 · 03 · Conexiones MCP — managed / custom / external (CP4)
# MAGIC No es un patrón nuevo: es la infraestructura de transporte que cualquiera de los cuatro patrones
# MAGIC puede usar para exponer o consumir tools. Acá probamos las tres variantes contra el agente del
# MAGIC notebook 02. El mismo mecanismo (managed) es lo que usa tu Claude Code conectado en CP1.
# MAGIC
# MAGIC Requiere haber corrido antes `02-agente-responsesagent-cp3.py` (hereda `w`, `agente`, `preguntar`).
# MAGIC
# MAGIC **Si es la primera vez que abres ESTE notebook:** completa el widget de catálogo que aparece
# MAGIC al ejecutar la celda siguiente — cada notebook tiene su propia copia, no la hereda de otro.
# MAGIC
# MAGIC **Si te da `ModuleNotFoundError` (por ejemplo `databricks_mcp`):** el entorno serverless de
# MAGIC ESTE notebook también es propio — abre el panel **Environment** (arriba a la derecha),
# MAGIC confirma que están `mlflow`, `databricks-openai`, `databricks-mcp`, `mcp==1.30.0` y
# MAGIC `jsonschema`, agrega la que falte, **Apply** y reinicia Python antes de volver a ejecutar.

# COMMAND ----------

dbutils.widgets.text("catalogo", "", "01 · Tu catálogo de S01–S02 (obligatorio)")
dbutils.widgets.text("endpoint", "databricks-meta-llama-3-3-70b-instruct", "02 · Endpoint con tool calling")
dbutils.widgets.text("genie_space_id", "01f1a2b475f11570a24ad8fdd22efbf6", "03 · Genie curado Neptuno (catálogo neptuno_ai)")
dbutils.widgets.text("secret_scope", "", "04 · Scope opcional: solo ejercicio Secrets")
dbutils.widgets.text("secret_key", "", "05 · Key opcional: nunca escribas el secreto aquí")
print("Si el campo 'catalogo' está vacío arriba, complétalo y ejecuta esta celda de nuevo antes de seguir.")

# COMMAND ----------

# MAGIC %run ./02-agente-responsesagent-cp3

# COMMAND ----------

# MAGIC %md
# MAGIC ## CP4.1 · MCP managed: descubrir y llamar
# MAGIC MCP normaliza `tools/list` y `tools/call`; no elimina UC ni permisos.
# MAGIC El servidor managed expone las funciones de nuestro schema, sin compartir tokens en el notebook.

# COMMAND ----------

# "managed" = Databricks opera el servidor MCP por ti: el endpoint
# /api/2.0/mcp/functions/<catalogo>/<schema> expone AUTOMÁTICAMENTE cualquier UC
# Function de ese schema como una tool MCP — no escribimos ningún servidor nosotros.
from databricks_mcp import DatabricksMCPClient
mcp_uc = DatabricksMCPClient(
    server_url=f"{w.config.host.rstrip('/')}/api/2.0/mcp/functions/{CATALOGO}/s05_agentes",
    workspace_client=w)
from concurrent.futures import ThreadPoolExecutor
# list_tools() = "tools/list" del protocolo MCP: solo pregunta qué hay disponible,
# todavía no ejecuta nada.
with ThreadPoolExecutor(max_workers=1) as pool:
    mcp_tools = pool.submit(mcp_uc.list_tools).result(timeout=90)
print("MCP tools/list:", [(t.name,t.description) for t in mcp_tools])
mcp_venta = next(t for t in mcp_tools if t.name.endswith("ventas_categoria"))
# call_tool() = "tools/call": ACÁ sí se ejecuta de verdad, con los mismos permisos de
# Unity Catalog que ya probamos en el notebook 01 — MCP es solo el transporte.
with ThreadPoolExecutor(max_workers=1) as pool:
    mcp_result = pool.submit(mcp_uc.call_tool, mcp_venta.name, {"p_categoria":EJEMPLO["categoria"],"p_anio":EJEMPLO["anio"]}).result(timeout=120)
if getattr(mcp_result,"isError",False):
    raise RuntimeError(f"MCP tools/call falló: {mcp_result.content}")
print("MCP tools/call:", mcp_result.content)
assert mcp_result.content  # listar tools no prueba nada; esto sí es la ejecución real

# COMMAND ----------

# MAGIC %md
# MAGIC ## CP4.2 · MCP custom: servidor propio reproducible
# MAGIC Este servidor **custom local por stdio** usa datos sintéticos; no es un servicio externo de producción.
# MAGIC Ejecutamos el protocolo real en un subproceso temporal y lo cerramos al salir.
# MAGIC Compara: managed (Databricks opera), external (tercero opera), custom (nosotros operamos).

# COMMAND ----------

# "custom" = ACÁ operamos nosotros el servidor MCP, de punta a punta. Para que se
# pueda correr sin infraestructura externa, lo escribimos como texto (CUSTOM_SOURCE),
# lo guardamos en un archivo temporal, y lo arrancamos como subproceso que habla el
# protocolo MCP por stdio (entrada/salida estándar) — el mismo protocolo que managed,
# solo que el servidor lo operamos y apagamos nosotros.
import asyncio, sys, tempfile, pathlib
from concurrent.futures import ThreadPoolExecutor
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
CUSTOM_SOURCE = '''from mcp.server.fastmcp import FastMCP
mcp = FastMCP("neptuno-sintetico")
@mcp.tool()
def estado_pedido_demo(pedido_id: int) -> dict:
    """Consulta solo el pedido sintético 1001; no representa datos reales Neptuno."""
    if pedido_id != 1001:
        return {"encontrado": False, "fuente": "SINTETICO"}
    return {"pedido_id": 1001, "estado": "EN_PREPARACION", "fuente": "SINTETICO"}
if __name__ == "__main__":
    mcp.run(transport="stdio")
'''
async def probar_custom():
    with tempfile.TemporaryDirectory(prefix="s05_mcp_") as td:
        path = pathlib.Path(td)/"server.py"
        path.write_text(CUSTOM_SOURCE)
        params = StdioServerParameters(command=sys.executable,args=[str(path)])
        # stdio_client arranca el subproceso (nuestro server.py) y abre el canal de
        # comunicación; ClientSession habla el protocolo MCP sobre ese canal.
        async with stdio_client(params) as (read,write):
            async with ClientSession(read,write) as session:
                await session.initialize()
                listed = await session.list_tools()
                result = await session.call_tool("estado_pedido_demo",{"pedido_id":1001})  # llamada válida
                invalid = await session.call_tool("estado_pedido_demo",{"pedido_id":"no-entero"})  # llamada inválida a propósito
                if not invalid.isError:
                    raise AssertionError("MCP custom aceptó un pedido_id que viola el tipo integer")
                return [t.name for t in listed.tools], result.model_dump(mode="json"), invalid.model_dump(mode="json")
# Thread evita anidar asyncio.run en el event loop del notebook.
with ThreadPoolExecutor(max_workers=1) as pool:
    CUSTOM_EVIDENCE = pool.submit(lambda: asyncio.run(asyncio.wait_for(probar_custom(),timeout=40))).result(timeout=50)
print(CUSTOM_EVIDENCE)
assert "estado_pedido_demo" in CUSTOM_EVIDENCE[0]
assert not CUSTOM_EVIDENCE[1].get("isError",False)
assert CUSTOM_EVIDENCE[2].get("isError") is True

# COMMAND ----------

# MAGIC %md
# MAGIC ## CP4.3 · External y Secrets: credenciales fuera del prompt
# MAGIC Para un tercero se crea una conexión/MCP Service con permisos y endpoint revisados. La siguiente celda conecta un MCP público real; un proveedor privado requiere conexión y permisos.
# MAGIC **Ejercicio opcional:** prepara en Databricks Secrets un valor SINTÉTICO, escribe scope/key arriba y ejecuta.
# MAGIC `dbutils.secrets.get` recupera el valor en memoria; no se imprime, no se manda al LLM y no se registra en MLflow.
# MAGIC La autenticación de este laboratorio usa la identidad del notebook; no necesita esa API key.

# COMMAND ----------

# Secrets responde a una pregunta distinta de MCP: ¿cómo guardo una credencial sin
# escribirla en el código? dbutils.secrets.get la trae a memoria por un instante, para
# usarla, y nunca queda en texto plano en el notebook ni en las trazas.
scope, key = dbutils.widgets.get("secret_scope").strip(), dbutils.widgets.get("secret_key").strip()
if bool(scope) != bool(key):
    raise ValueError("Completa ambos campos scope/key, o deja ambos vacíos.")
if scope and key:
    secreto = dbutils.secrets.get(scope=scope,key=key)
    assert isinstance(secreto,str) and secreto
    del secreto  # lo borramos de memoria apenas lo confirmamos; nunca se imprime
    print("Secrets: lectura correcta; valor no mostrado ni persistido.")
else:
    print("Secrets: ejercicio opcional no ejecutado. MCP external público se ejecuta en CP4.4 sin API key.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CP4.4 · MCP external real: documentación pública Microsoft Learn
# MAGIC **Streamable HTTP**, servidor operado por un tercero; `tools/list` y `tools/call` reales.
# MAGIC La consulta enviada es pública y genérica; no enviamos documentos Neptuno, tokens ni datos del catálogo.
# MAGIC [Referencia oficial](https://learn.microsoft.com/en-us/training/support/mcp-developer-reference).
# MAGIC Esto demuestra el transporte externo. No es una fuente empresarial ni reemplaza el RAG S04.

# COMMAND ----------

# "external" = el servidor lo opera un TERCERO (Microsoft), no Databricks ni nosotros.
# El transporte cambia (HTTP en vez de stdio), pero el protocolo MCP es el mismo:
# también hacemos tools/list y tools/call reales.
from mcp.client.streamable_http import streamablehttp_client
async def probar_external():
    async with streamablehttp_client("https://learn.microsoft.com/api/mcp") as (read,write,_):
        async with ClientSession(read,write) as session:
            await session.initialize()
            listed = await session.list_tools()
            nombres = [t.name for t in listed.tools]
            if "microsoft_docs_search" not in nombres:
                raise RuntimeError("El MCP externo cambió su contrato: inspecciona tools/list.")
            result = await session.call_tool("microsoft_docs_search", {"query":"Azure Databricks Unity Catalog functions"})
            return nombres, result.model_dump(mode="json")
import socket

def es_error_dns(exc):
    if isinstance(exc, socket.gaierror) or "name resolution" in str(exc).lower() or "name or service not known" in str(exc).lower():
        return True
    children = list(getattr(exc,"exceptions",[]))
    if exc.__cause__ is not None:
        children.append(exc.__cause__)
    return any(es_error_dns(child) for child in children)

try:
    with ThreadPoolExecutor(max_workers=1) as pool:
        names, payload = pool.submit(lambda: asyncio.run(asyncio.wait_for(probar_external(),timeout=45))).result(timeout=55)
    assert not payload.get("isError",False) and payload.get("content")
    EXTERNAL_EVIDENCE = {"status":"PASS_IN_SERVERLESS","tools":names,"result":payload}
    print("EXTERNAL tools/list:",names)
    print("EXTERNAL tools/call:",json.dumps(payload,ensure_ascii=False)[:2500])
except Exception as exc:
    if not es_error_dns(exc):
        raise
    EXTERNAL_EVIDENCE = {"status":"UNAVAILABLE_IN_SERVERLESS",
        "cause":"El entorno serverless no resuelve learn.microsoft.com (DNS). No se cambió la política de red.",
        "required_demo_location":"LOCAL_PC","script":"scripts/mcp_external_demo.py"}
    print(json.dumps(EXTERNAL_EVIDENCE,ensure_ascii=False,indent=2))
    print("Continúa con 04-chatbot-genie-cp5.py; ejecuta la demo MCP external en tu PC siguiendo la siguiente celda.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### CP4.4b · Demo obligatoria MCP external desde PC (fuera de Databricks)
# MAGIC Si serverless no resuelve el servidor, esta parte se ejecuta en el PC del docente/alumno con acceso público.
# MAGIC Descarga el repositorio de la clase y sitúate en `s05-agentes`. No se requieren tokens ni cuenta Microsoft.
# MAGIC **Con uv:**
# MAGIC ```bash
# MAGIC uv run --with 'mcp==1.30.0' python scripts/mcp_external_demo.py --output reports/mcp-external-local.json
# MAGIC ```
# MAGIC **Con Python y pip:**
# MAGIC ```bash
# MAGIC python3 -m venv .venv-mcp
# MAGIC .venv-mcp/bin/python -m pip install -r scripts/requirements-mcp-external.txt
# MAGIC .venv-mcp/bin/python scripts/mcp_external_demo.py --output reports/mcp-external-local.json
# MAGIC ```
# MAGIC En Windows usa `.venv-mcp\Scripts\python.exe` en los dos últimos comandos.
# MAGIC Abre el JSON: debe registrar `PASS`, `LOCAL_PC`, herramientas descubiertas, consulta y resultado con URLs.
# MAGIC Adjunta ese JSON a la entrega. Un bloqueo DNS en serverless **no es PASS de MCP external**; la evidencia local se valida aparte.
# MAGIC La demo prueba que un cliente fuera de Databricks consume un MCP externo; managed muestra cómo exponer funciones UC gobernadas.
# MAGIC
# MAGIC Conexiones MCP cerradas · siguiente: `04-chatbot-genie-cp5.py`
