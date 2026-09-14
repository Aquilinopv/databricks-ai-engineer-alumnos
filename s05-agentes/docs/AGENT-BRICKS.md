# Agent Bricks: recorrido S05 y evidencia

Demostración del instructor observada el 14 de septiembre de 2026. Resumen distribuido: [`../reports/demo-bricks.json`](../reports/demo-bricks.json).
El entregable central es el Copiloto Neptuno con tools sobre el lakehouse. Este bloque compara alternativas administradas y practica feedback; no acredita un Supervisor ni Knowledge Assistant desplegados.

## Disponibilidad comprobada

| Capacidad | Resultado real | Qué mostrar |
|---|---|---|
| Information Extraction / `ai_extract` 2.1 | SQL `SUCCEEDED`, una fila, sin error | Extraer plazo y condición de la política Neptuno |
| Supervisor Agent listado REST | Scope `supervisor-agents` ausente | Recorrido UI sujeto a sesión; diseño de coordinación |
| Knowledge Assistant listado REST | Scope `knowledge-assistants` ausente | Recorrido UI sujeto a sesión; preguntas y guidelines |
| ALHF | Ejercicio de feedback preparado | No se ejecutó optimización administrada |

La ausencia de scopes del token CLI no demuestra que el usuario no tenga acceso por UI ni que no existan agentes. No se cambiaron permisos ni se crearon endpoints. Los reportes internos de acceso se conservan con el instructor y no forman parte de esta práctica. El resumen reproducible entregado está en `../reports/demo-bricks.json`.

## Demo ejecutada: extracción de política, 5 minutos

Texto procedente de `rag.chunks`, documento `politica_devoluciones`.

```sql
SELECT ai_extract('Productos refrigerados o congelados solo se aceptan dentro de 24 horas si existe evidencia de ruptura de cadena de frío.', '{"plazo_horas":{"type":"integer","description":"Plazo máximo de devolución en horas"},"condicion":{"type":"string","description":"Condición requerida para aceptar la devolución"}}', options => map('version','2.1')) AS extraccion;
```

Resultado observado: `response.plazo_horas.value = 24`; `response.condicion.value = "si existe evidencia de ruptura de cadena de frío"`; `error_message = null`.

Preguntar al grupo: ¿esta salida responde preguntas generales, extrae campos o coordina herramientas? Es extracción. No confundir esta llamada al motor de extracción con un agente administrado persistente.

## Recorrido UI documentado, 7 minutos

Abrir **Agents** en la navegación del workspace. El enlace histórico es `/ml/bricks`; usar el menú si cambia la ruta. Inspección UI realizada por el agente principal el 14-sep-2026: `/ml/bricks` redirige a `/ml/agents`; la lista muestra **No Agents available yet**. **Create Agent** ofrece Information Extraction (`ai_extract`), Supervisor Agent, Knowledge Assistant, Code your own agent y Genie Agent. Evidencia resumida: `../reports/demo-bricks.json`.

- **Create Agent → Information Extraction**: seleccionar tabla de documentos, columna de texto; definir `plazo_horas` entero y `condicion` texto; inspeccionar extracción y comparar versiones. La demo SQL anterior usa el mismo motor. No es necesario crear otro agente para realizar el ejercicio.
- **Knowledge Assistant**: adecuado para preguntas sobre políticas con citas. En un agente existente, revisar fuentes y **Examples**; abrir una pregunta y sus **Guidelines**. No presentar una búsqueda SQL por palabras como prueba de un Knowledge Assistant administrado.
- **Supervisor Agent**: se abrió `/ml/bricks/sa/build`. El formulario **New Supervisor Agent** ofrece **Add a Genie Space**, **Add a Knowledge Assistant**, **Add a UC Function**, **Add a UC Connection**, **Add a UC MCP Service**, **Instructions** y **Prompt Input**. Trae `system.ai.python_exec` por defecto. Para el diseño Neptuno de mínimo privilegio, quitar esa herramienta de ejecución general y dejar solo las herramientas requeridas; esto es una instrucción de diseño, no una modificación realizada. No se creó ni ejecutó el supervisor. No equiparar un router Python con un Supervisor Agent administrado.

Si no hay agente existente accesible, mostrar el diseño y la evidencia de extracción, y declarar que la práctica administrada no se ha ejecutado. No crear serving ni conceder permisos para salvar una demo.

## Ejercicio alumno: elegir patrón y aportar feedback, 10 minutos

1. Clasificar tres solicitudes: “ventas de abril por categoría” → Genie; “extrae plazo y condición de devolución” → Information Extraction; “combina ventas de Lácteos y su política logística” → Supervisor con especialistas.
2. Leer una respuesta problemática: “Todos los productos se devuelven dentro de 24 horas”. Explicar por qué generaliza indebidamente.
3. Escribir feedback verificable: “Para refrigerados o congelados, conserva el plazo de 24 horas y la condición de evidencia de ruptura de cadena de frío. No extiendas esta regla a otros productos. Cita política_devoluciones”.
4. Proponer una pregunta de regresión diferente y comprobar que la corrección no borre requisitos ni invente reglas.

Artefacto preparado en `alhf-ejercicio.json`: preguntas, guidelines y clasificación esperada. Es feedback para revisión humana; no prueba de ALHF ejecutado. En un Knowledge Assistant accesible, las preguntas pueden ingresarse en Examples y sus guidelines guardarse; después se repite la pregunta y se compara evidencia. La autorización CAN_MANAGE que exige el producto no se concede en esta práctica.

## Fuentes oficiales

- [Information Extraction](https://docs.databricks.com/aws/en/agents/agent-bricks/info-extraction): UI, SQL y esquema.
- [ai_extract](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_extract): versión 2.1 y salida estructurada.
- [Knowledge Assistant](https://docs.databricks.com/aws/en/agents/agent-bricks/knowledge-assistant): Examples y Guidelines.
- [Supervisor Agent](https://docs.databricks.com/aws/en/agents/agent-bricks/multi-agent-supervisor): coordinación y feedback.
- [API Knowledge Assistant](https://docs.databricks.com/api/knowledge-assistants/v1/knowledge-assistant): GET `/api/2.1/knowledge-assistants`.
- [API Supervisor](https://docs.databricks.com/api/supervisor-agents/v1/get-supervisor-agent): GET `/api/2.1/supervisor-agents`.
