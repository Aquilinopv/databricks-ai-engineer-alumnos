# S05 Neptuno — contrato técnico del notebook

El notebook reemplaza el router de palabras clave por decisiones `tool_calls` del endpoint Databricks. El ejecutor valida el nombre, JSON Schema, categoría/año disponibles y el presupuesto de 4 herramientas/6 turnos. Usa `ResponsesAgent` con spans MLflow de agente, modelo y herramienta.

## Recursos y límites

- Widget `catalogo` vacío; creación de widgets separada de validación. Lectura de Gold y `rag.chunks_embeddings` existentes; objetos escritos únicamente en `<catalogo>.s05_agentes`.
- UC Functions SQL escalares JSON `ventas_categoria(p_categoria,p_anio)` y `productos_reponer()`, ambas con COMMENT; se exponen vía `databricks_openai.UCFunctionToolkit` y se ejecutan mediante `DatabricksFunctionClient`.
- RAG `buscar_documentos(pregunta)`: corpus S04, qwen3-embedding-0-6b 1024d, coseno top3 en memoria, hasta1000 chunks. No usa un índice Vector Search.
- `consultar_genie(pregunta)`: espacio curado configurado en el widget `genie_space_id`, fuente compartida **neptuno_ai**. No se presenta como el catálogo elegido por cada alumno. Expone SQL y resultado `SUCCEEDED`.
- MCP managed: schema de funciones del alumno, descubrimiento y llamada reales. Cliente síncrono en thread para evitar `asyncio.run` anidado en Databricks.
- MCP custom: servidor temporal stdio con un pedido explícitamente sintético; se cierra y borra al terminar.
- MCP external: Microsoft Learn público, Streamable HTTP; consulta genérica de documentación Azure Databricks. No transmite datos Neptuno ni credenciales. Run3 confirmó bloqueo DNS serverless; el notebook registra UNAVAILABLE_IN_SERVERLESS y la demo obligatoria se ejecuta desde PC con `scripts/mcp_external_demo.py`. El script genera `reports/mcp-external-local.json` con tu ejecución; la corrida docente observada se resume en `reports/demo-mcp-external.json`; no equivale a PASS remoto.
- Secrets: ejercicio opcional por widgets scope/key. Instructor puede usar `s05_neptuno_demo/api_key_sintetica` (valor sintético). La lectura real no imprime ni persiste el valor.
- Agent Bricks y ALHF: actividad guiada de clasificación, configuración/feedback y evidencia de disponibilidad; no se atribuye entrenamiento, optimización ni despliegue real a código artesanal.

## Pruebas esperadas

| Caso | Evidencia necesaria |
|---|---|
| Venta | llamada UC ventas con categoría/año observados + resultado numérico cotejado con SQL |
| Reposición | llamada UC productos_reponer exitosa |
| Documental | llamada buscar_documentos con fragmentos citables S04 |
| Mixta | ventas_categoria y buscar_documentos en una respuesta |
| Falta año | cero llamadas + pregunta por año/período |
| Margen sin costos | cero llamadas + respuesta limitada al alcance |
| Borrado | cero llamadas + rechazo |

La prueba funcional no sustituye al harness de calidad de S06. Las respuestas de rechazo y fuentes deben revisarse también por humanos.

## Validación local

`uv run --with jsonschema python tests/test_contracts.py` — 11 pruebas de contratos, presupuesto, formato de celdas, configuración y clasificación DNS. Los dobles de prueba locales no acreditan ejecución remota.

`python3 -m py_compile notebook.py` valida sintaxis. Estas pruebas no acreditan ejecución en Databricks; los reportes remotos completos son evidencia docente no distribuida. Tu notebook conserva el resumen de tu propia ejecución.

El notebook imprime `VERSIONES_ENTORNO` y `S05_VALIDATION_SUMMARY` y persiste los 7 casos en `s05_agentes.evidencias_funcionales`. MLflow almacena trazas en `/Users/<usuario>/S05-Neptuno-Agentes`.

## Dependencias

La cabecera PEP723 incluye entorno serverless5 y dependencias. `mcp==1.30.0` conserva la API `streamablehttp_client`; mcp2.2 cambió nombre/retorno y no se mezcla con este código. Las versiones exactas resueltas deben tomarse del run exitoso, no inferirse a partir de las restricciones.

## Referencias verificadas

- https://api-docs.databricks.com/python/databricks-ai-bridge/latest/databricks_openai.html
- https://docs.databricks.com/aws/en/agents/mcp-tools/use-mcp-in-agents
- https://docs.databricks.com/aws/en/compute/serverless/dependencies
- https://mlflow.org/docs/latest/genai/serving/responses-agent
- https://docs.databricks.com/aws/en/genie/conversation-api
- https://learn.microsoft.com/en-us/training/support/mcp-developer-reference

## Correcciones verificadas durante preparación

El contrato empresarial de ventas restringe los argumentos SQL nullable del toolkit: categoría y año son obligatorios y no aceptan null. Los ejemplos de aclaración ayudan al modelo a pedir el dato antes de llamar una tool; la frontera valida igualmente los argumentos. Las respuestas documentales deben incluir documento_id y chunk_id, la venta no añade moneda no informada y el rechazo de margen explica la ausencia de costos. MCP custom conserva una llamada válida y el rechazo de un argumento no entero.
