# S05 · Construye el Copiloto de Datos Neptuno

**Duración de clase: 180 minutos reloj, incluidos dos descansos de 5 minutos.**

Tu entrega es un agente que consulta datos y documentos de Neptuno mediante herramientas, responde con evidencia y conserva trazas. Reutilizaremos este mismo agente en S06, S07 y S08. El caso SBS pertenece a un laboratorio complementario independiente.

## Antes de empezar

Abre `notebook.py` y conserva una copia personal. Necesitas tu catálogo de S01–S02, tablas gold, el corpus Neptuno de S04, un endpoint con tool calling, permisos para crear/ejecutar UC Functions, acceso MCP y un Genie Space curado. El notebook muestra el catálogo como campo vacío: escribe el tuyo y ejecuta la validación antes de crear recursos. El Genie docente, cuando se usa, consulta `neptuno_ai`; no supone que consulte automáticamente tu catálogo.

Si falta un recurso, registra el error y recupéralo siguiendo el bloque del notebook. Un mock, un listado de tools o una captura antigua no reemplaza una llamada real. Las pruebas de MCP custom con datos sintéticos se identifican como tales.

Crea `entregas/s05/evidencia.md` en tu copia del repositorio de alumnos. Añade una exportación del notebook sin credenciales y las salidas que permitan revisar cada checkpoint. No se requiere un archivo de un alumno real para validar preparación de la clase: el instructor conserva su recorrido resuelto por separado.

<a id="cp0"></a>
## CP0 · Configura tu entorno

1. Ejecuta la celda que muestra los widgets y completa `catalogo`, el endpoint y la configuración de Genie.
2. Ejecuta la validación. Revisa identidad, catálogo, tablas de entrada y recursos externos.
3. Guarda los nombres de los recursos y los estados de validación, sin tokens ni secretos.

**Aceptación:** el catálogo no es un valor docente insertado en el código; la validación confirma las tablas necesarias. Distingues el catálogo personal del catálogo del Space docente.

<a id="cp1"></a>
## CP1 · Verifica la herramienta comercial

1. Lee el contrato de `ventas_categoria(p_categoria, p_anio)` y `productos_reponer()` en el notebook.
2. Explica por qué venta neta aplica descuento y por qué no permite inferir margen.
3. Usa `EJEMPLO["categoria"]` y `EJEMPLO["anio"]`, seleccionados de tus datos por CP0. Ejecuta `ventas_categoria` con ese par y contrasta con la agregación SQL de referencia.
4. Inspecciona nombre, descripción y argumentos expuestos por `UCFunctionToolkit`.
5. Prueba una categoría inexistente y mejora un `COMMENT` para describir límites.

Los pares como Bebidas/2025 de las slides ilustran la forma de preguntar; no garantizan filas en tu catálogo. El notebook usa el par observado.

**Conserva:** entrada, SQL de referencia, salida de la función, contrato de la tool y resultado negativo.

**Aceptación:** SQL y tool coinciden; categoría inexistente no se presenta como una venta comprobada de cero; el contrato excluye margen sin costos.

<a id="cp2"></a>
## CP2 · Ejecuta el agente y lee una traza

1. Abre la clase `AgenteNeptuno(ResponsesAgent)` y localiza el ciclo de llamadas, la lista permitida y los límites.
2. Usa `PREGUNTA_VENTA`, construida con el par observado en `EJEMPLO`. Antes de adaptar un ejemplo de las slides, sustituye categoría y año por ese par.
3. Pregunta: «¿Cuál es el plazo y condición para devolver refrigerados?».
4. Pregunta por margen con la categoría y el año observados en `EJEMPLO`.
5. Abre una traza nueva de MLflow y ubica la solicitud de tool, argumentos, resultado y síntesis.
6. Reformula una pregunta conservando su intención y compara la selección.

**Conserva:** tres preguntas, respuestas, tool calls, identificador o enlace de traza y una reformulación.

**Aceptación:** el modelo elige la tool mediante una llamada real; la respuesta comercial coincide con datos; la política conserva fuente; margen sin costos produce limitación explícita. Una impresión de un router por palabras no satisface este checkpoint.

<a id="cp3"></a>
## CP3 · Descubre e invoca MCP

1. Ejecuta la conexión managed de UC: lista tools, inspecciona un esquema e invoca una operación.
2. Contrasta su resultado con la UC Function directa. Explica cómo un cliente autorizado puede consumir el mismo recurso desde fuera de Databricks.
3. Ejecuta el servidor custom y su cliente con los archivos indicados en el notebook. Conserva el nombre de tool y una llamada válida e inválida. Identifica los datos sintéticos del ejercicio.
4. Revisa el bloque Secrets: scope y key se configuran; el valor se obtiene al ejecutar y nunca se imprime.
5. El DNS serverless bloquea Microsoft Learn: CP3.4 registra `UNAVAILABLE_IN_SERVERLESS`, que no es PASS remoto. Ejecuta la práctica externa obligatoria desde tu PC, en `s05-agentes`: `uv run --with 'mcp==1.30.0' python scripts/mcp_external_demo.py --output reports/mcp-external-local.json`. Alternativamente instala `scripts/requirements-mcp-external.txt` en tu entorno Python y ejecuta el script. Descubre herramientas e invoca una búsqueda de documentación. No requiere API key. El ejercicio Secrets usa un valor expresamente sintético para demostrar almacenamiento y recuperación; no es una credencial del proveedor. Cuando un proveedor exige autenticación, su clave debe recuperarse desde Secrets.

**Conserva:** modalidades managed/custom/external, servidor, tool, argumentos no sensibles y resultado. Separa evidencia real, sintética y pendiente.

**Aceptación:** managed y custom ejecutan operaciones reales; listar tools no basta. Recuperar un secreto no demuestra una llamada externa. La integración externa pasa con una respuesta real de Microsoft Learn desde PC y el reporte `status=PASS`, `execution_location=LOCAL_PC`. Conserva también la limitación `UNAVAILABLE_IN_SERVERLESS`; no la declares resuelta. La demo Secrets pasa al recuperar el valor sintético sin imprimirlo; no se declara una integración autenticada.

### Prepara el ejercicio Secrets con un valor sintético

En tu PC usa el perfil CLI que apunta a tu workspace. Sustituye `s05_tu_alias` por un nombre propio único; estos comandos solo son para el ejercicio sintético:

```sh
databricks secrets create-scope s05_tu_alias
databricks secrets put-secret s05_tu_alias api_key_sintetica --string-value DEMO-NO-CREDENTIAL
databricks secrets list-acls s05_tu_alias
```

Comprueba que `MANAGE` corresponde al creador. No concedas acceso a `ALL_USERS` ni a todos los usuarios. Alternativa para crear el scope: abre `https://<tu-workspace>#secrets/createScope` y elige **Creator**. Si tu entorno no permite ese alcance, pide un scope restringido al instructor; no amplíes permisos para continuar.

En los widgets del notebook escribe el scope propio y `api_key_sintetica`. Ejecuta CP3.3: `dbutils.secrets.get` recupera el valor en memoria, sin imprimirlo ni guardarlo en trazas. El instructor usa un scope de demostración distinto; sus permisos no se heredan a tu cuenta. Para una API key real se utiliza entrada interactiva o el mecanismo seguro del proveedor, nunca el valor de ejemplo ni una clave pegada en el repositorio.

Consulta [Secrets y permisos](https://docs.databricks.com/aws/en/security/secrets/) y [comandos CLI](https://docs.databricks.com/aws/en/dev-tools/cli/reference/secrets-commands).

<a id="cp4"></a>
## CP4 · Delega una pregunta analítica a Genie

1. Revisa el Space y sus tablas: el ejemplo docente usa Neptuno curado (`neptuno_ai`).
2. Ejecuta la tool Genie con «¿Cuál fue la venta neta total de abril de 2026? Identifica el catálogo fuente.».
3. Ejecuta una pregunta que requiera Genie desde el agente.
4. Inspecciona pregunta, SQL, resultado y respuesta final: métrica neta, descuento y periodo.
5. Explica cómo aclararías «¿Cómo vamos este año?» con un dataset cuyo último mes es parcial.

**Conserva:** ID del Space, pregunta, tool call del agente, SQL y resultado.

**Aceptación:** la llamada procede del agente; el SQL consulta las tablas previstas y la síntesis no cambia métrica ni periodo. Una conversación manual en la UI solo prueba el Space, no su integración.

<a id="decision"></a>
## Decisión · Compara Bricks y aplica feedback humano

En `evidencia.md`, completa esta tabla para Neptuno:

| Necesidad | Capacidad candidata | Entrada y salida | Prueba que exigirías |
|---|---|---|---|
| Políticas con citas | Knowledge Assistant | Documento → respuesta sustentada | Sin evidencia debe abstener |
| Orden a campos | Information Extraction | Documento → campos tipados | Fecha ausente no se inventa |
| Ventas y políticas | Supervisor | Pregunta → delegaciones → síntesis | Conserva fuentes de ambos dominios |

Observa la demo de extracción del instructor si está disponible y registra su salida real. El ejercicio de diseño no equivale a desplegar los tres servicios.

Escribe un feedback humano accionable: «Al pedir margen respondió venta neta; no hay costos, debe explicar el límite». Define una pregunta para volver a comprobar la corrección y otra para detectar una regresión. Explica el sentido de ALHF sin afirmar que realizaste entrenamiento automático.

Elige agente propio o capacidad administrada para Neptuno y justifica dos criterios: control, calidad, mantenimiento o costo. Añade la equivalencia conceptual con Vertex AI Agent Builder, Foundry Agent Service o Bedrock Agents; no supongas compatibilidad de SDK.

**Aceptación:** distingues las tres capacidades, presentas feedback específico y explicas una decisión con límites.

<a id="cp5"></a>
## CP5 · Integra, recupera y entrega

1. Combina `PREGUNTA_VENTA` con «¿Qué plazo y condición aplican a devoluciones de refrigerados?». Mantén categoría y año observados en `EJEMPLO`.
2. Comprueba que se consultaron ambas fuentes y que la respuesta permite reconocer cuál respalda cada afirmación.
3. Provoca un fallo acotado: categoría inexistente o argumento inválido. Conserva error y recuperación, sin borrar recursos ni ampliar permisos.
4. Intercambia evidencia con un compañero: debe poder señalar una afirmación y encontrar su respaldo.
5. Entrega `evidencia.md` y notebook exportado sin secretos.

**Tu evidencia debe incluir:** CP0–CP4; pregunta compuesta; caso negativo; decisión Bricks/ALHF; traza actual; límites y pendientes explícitos.

**Aceptación:** las pruebas mínimas pasan por sus efectos, no solo por celdas verdes. Cada integración requerida tiene evidencia de ejecución. Cualquier pendiente permanece visible para recuperación; no declares completo lo que no ejecutaste.

## Puente a S06–S08

S06 convertirá estas preguntas en un harness con métricas y jueces. S07 protegerá las mismas tools y fuentes. S08 desplegará el agente con UI y monitoreo. Conserva entradas, resultados y versiones: una demo exitosa no es todavía una evaluación robusta ni una aplicación en producción.
