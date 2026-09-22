# Supervisor · herramientas y bibliografía oficial

Verificado el 21 de septiembre de 2026. Las 14 etiquetas proceden de la captura del instructor. La explicación se contrasta con documentación oficial de Databricks. Son **referencias conceptuales**: no se han conectado ni probado todos estos tipos en este workspace. Los ejemplos adicionales son propuestas didácticas, no recursos nuevos creados.

La captura del instructor confirma que el guardado manual falla con el mismo mensaje de disponibilidad que la API. Poder abrir el editor o ver recursos en el selector no demuestra que el Supervisor se haya creado.

| Opción en tu interfaz | Para qué sirve | Qué seleccionar/configurar | Ejemplo conceptual Neptuno | Bibliografía oficial |
|---|---|---|---|---|
| Genie Agents | Especialista en consultas analíticas | Elegir el Genie; describir dominio y cuándo delegar. | Ventas de abril | [Crear un Genie Agent](https://docs.databricks.com/aws/en/genie-agents/set-up) |
| Knowledge Assistants | Respuestas sobre documentación | Seleccionar el asistente ya preparado y su alcance documental. | Políticas de reposición | [Knowledge Assistant sobre documentos](https://docs.databricks.com/aws/en/agents/agent-bricks/knowledge-assistant) |
| Supervisor Agents | Delegación a otro coordinador | Elegir un Supervisor existente y delimitar su responsabilidad. | Coordinador de logística | [Supervisor: tipos admitidos y configuración](https://docs.databricks.com/aws/en/agents/agent-bricks/multi-agent-supervisor) |
| Serving Endpoints | Agente/modelo publicado como servicio | Elegir un endpoint compatible con el contrato requerido; comprobar acceso. | Agente especializado ya desplegado | [Crear endpoints de Model Serving](https://docs.databricks.com/aws/en/machine-learning/model-serving/create-manage-serving-endpoints) |
| UC Tables | Datos tabulares gobernados | Seleccionar tablas concretas y describir su contenido. | Tabla de inventario | [Tablas Databricks](https://docs.databricks.com/aws/en/tables/) |
| Volumes | Archivos gobernados en Unity Catalog | Elegir el volumen y verificar acceso a sus archivos. | Archivos de soporte de la operación | [Volúmenes Unity Catalog](https://docs.databricks.com/aws/en/volumes/) |
| AI Search indexes | Recuperación mediante un índice | Elegir el índice preparado; Supervisor admite solo índices Delta Sync. | Recuperar fragmentos de políticas | [Crear endpoints e índices AI Search](https://docs.databricks.com/aws/en/ai-search/create-ai-search) |
| Dashboards | Análisis publicado reutilizable | Elegir un dashboard publicado; comprobar acceso. | Indicadores comerciales publicados | [Crear un dashboard](https://docs.databricks.com/aws/en/dashboards/tutorials/create-dashboard) |
| UC Functions | Operación con contrato de entrada/salida | Elegir función y describir parámetros, resultados y límites. | productos_reponer de S05 | [Funciones UC como herramientas](https://docs.databricks.com/aws/en/agents/custom-agents/create-custom-tool) |
| UC Connections | Acceso autenticado a servicios externos | Para HTTP: conexión, destino y autenticación; describir el servicio. | Consultar API de un proveedor | [Conexiones HTTP de Unity Catalog](https://docs.databricks.com/aws/en/query-federation/http) |
| UC MCP Services | Conjunto de herramientas expuesto por MCP | Seleccionar servicio UC y revisar herramientas expuestas. | Herramientas de un sistema externo | [Servicios MCP en Unity Catalog](https://docs.databricks.com/aws/en/agents/mcp-tools/mcp-services) |
| Apps | Agente personalizado alojado en una app | Seleccionar una app compatible; describir capacidades y comprobar acceso. | Agente de logística en Databricks Apps | [Agentes personalizados en Databricks Apps](https://docs.databricks.com/aws/en/agents/custom-agents/author-agent) |
| Built-in tools | Capacidades provistas por la plataforma | Inspeccionar las opciones efectivamente disponibles; no se crean recursos de negocio. | Catálogo documentado: SQL y sandbox | [Servicios MCP provistos por Databricks](https://docs.databricks.com/aws/en/agents/mcp-tools/built-in-mcp-services) |
| Web Search | Información de la web pública | Añadir si el workspace es elegible; requiere aprobación por búsqueda. | Consultar información pública reciente | [Supervisor: tipos admitidos y configuración](https://docs.databricks.com/aws/en/agents/agent-bricks/multi-agent-supervisor) |

## Cómo explicarlo en clase

1. Distingue una operación puntual (`UC Functions`) de un agente al que se delega una pregunta (`Genie`, `Knowledge Assistant`, otro `Supervisor`).
2. Distingue una fuente (`UC Tables`, `Volumes`, `AI Search indexes`) de un mecanismo de integración (`Connections`, `MCP Services`, `Apps`, `Serving Endpoints`).
3. Para cada recurso: explicar propósito, describir cuándo usarlo y comprobar el acceso de la identidad que hará la consulta. La descripción no concede permisos.
4. Actividad conceptual: elegir una opción para ventas, otra para documentos y otra para una regla de inventario. Solución docente: Genie; Knowledge Assistant o índice según diseño; UC Function. No se promete ejecutar estas combinaciones hoy.

## Distinciones que conviene mostrar

- **Conexión HTTP:** almacena destino y autenticación; no equivale por sí sola a un agente. [Conexiones HTTP](https://docs.databricks.com/aws/en/query-federation/http).
- **MCP Service:** recurso de Unity Catalog que presenta herramientas a los agentes. Revisar `tools/list`, contrato y permisos; un servicio puede exponer varias operaciones. [MCP Services](https://docs.databricks.com/aws/en/agents/mcp-tools/mcp-services).
- **Built-in tools:** la documentación de servicios provistos enumera DBSQL, sandbox y web search, además de conectores SaaS. Ese catálogo no demuestra que todas sus entradas aparezcan en este filtro o estén habilitadas aquí. [Catálogo oficial](https://docs.databricks.com/aws/en/agents/mcp-tools/built-in-mcp-services).
- **Ejecución de código:** la guía del Supervisor documenta una capacidad incorporada. La tarjeta `python_exec` de tu captura aparece como UC Function; ese rótulo no basta para afirmar que sea la misma implementación. No se modificó esa tarjeta. [Code execution](https://docs.databricks.com/aws/en/agents/agent-bricks/multi-agent-supervisor#code-execution).
- **Web Search:** confirmar elegibilidad; cada búsqueda pide aprobación al usuario. No confundir búsqueda pública con recuperación de documentos privados. [Guía y limitaciones](https://docs.databricks.com/aws/en/agents/agent-bricks/multi-agent-supervisor#limitations).

## Acceso y disponibilidad

La guía del Supervisor distingue acceso al agente de acceso a cada recurso. Según el tipo, intervienen `CAN QUERY`, `CAN USE`, `SELECT`, `READ VOLUME`, `EXECUTE` o `USE CONNECTION`, además del catálogo/esquema cuando corresponda. Consulta la [tabla oficial de permisos por subagente](https://docs.databricks.com/aws/en/agents/agent-bricks/multi-agent-supervisor#supported-subagents-and-tools). Aquí solo se documentan; no se han ampliado permisos.

## URLs para compartir

- [Supervisor: tipos admitidos y configuración](https://docs.databricks.com/aws/en/agents/agent-bricks/multi-agent-supervisor)
- [Servicios MCP provistos por Databricks](https://docs.databricks.com/aws/en/agents/mcp-tools/built-in-mcp-services)
- [Knowledge Assistant sobre documentos](https://docs.databricks.com/aws/en/agents/agent-bricks/knowledge-assistant)
- [Funciones UC como herramientas](https://docs.databricks.com/aws/en/agents/custom-agents/create-custom-tool)
- [Conexiones HTTP de Unity Catalog](https://docs.databricks.com/aws/en/query-federation/http)
- [Servicios MCP en Unity Catalog](https://docs.databricks.com/aws/en/agents/mcp-tools/mcp-services)
- [Crear endpoints e índices AI Search](https://docs.databricks.com/aws/en/ai-search/create-ai-search)
- [Tablas Databricks](https://docs.databricks.com/aws/en/tables/)
- [Volúmenes Unity Catalog](https://docs.databricks.com/aws/en/volumes/)
- [Agentes personalizados en Databricks Apps](https://docs.databricks.com/aws/en/agents/custom-agents/author-agent)
- [Crear endpoints de Model Serving](https://docs.databricks.com/aws/en/machine-learning/model-serving/create-manage-serving-endpoints)
- [Crear un dashboard](https://docs.databricks.com/aws/en/dashboards/tutorials/create-dashboard)
- [Crear un Genie Agent](https://docs.databricks.com/aws/en/genie-agents/set-up)
