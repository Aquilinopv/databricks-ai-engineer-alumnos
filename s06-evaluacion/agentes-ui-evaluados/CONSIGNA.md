# S06 complementario: evaluar los agentes del kit UI

Evalúa respuestas reales del espacio Genie creado con el kit y, cuando el workspace lo permita, del Supervisor que combina ventas e inventario. El notebook es autónomo: incluye las 16 preguntas y sus referencias SQL congeladas; no requiere archivos locales docentes ni ejecutar S05.

En el workspace docente existe Genie `01f1b5b8d58c1aaebabfeee15024000f`. La creación del Supervisor quedó bloqueada porque su funcionalidad no está disponible. No hay endpoint Supervisor ni evaluación real de sus ocho casos. El adaptador Responses del notebook está parametrizado y **no validado**. No reemplaces el Supervisor por un agente Custom para presentar resultados como si fueran del mismo agente.

## Preparación y ejecución

1. Importa `notebook.py` a Databricks y configura Environment con las dependencias declaradas: MLflow 3.16.0, SDK 0.140.0, OpenAI y pandas.
2. Deja `target=genie`, verifica el espacio y el endpoint del juez. `supervisor_endpoint` queda vacío y no bloquea Genie. Necesitas acceso al espacio y lectura de `neptuno_ai.ventas` y `neptuno_manuel_arguelles.gold.inventario_disponible`.
3. Ejecuta CP0 y CP1. El modo `recompute` recalcula tres oracles independientes antes de preguntar al agente y compara contra el freeze docente del 21 de septiembre de 2026. Si falla la igualdad, revisa/versiona el gold set antes de CP2. No cambies referencias para acomodar respuestas observadas.
4. Si reproduces expresamente el corte docente sin consultar tablas actuales, selecciona `oracle_mode=frozen_teacher` y declara esa limitación. No confundas el freeze con una validación de datos actuales.
5. Ejecuta CP2 una vez; CP3–CP6 reutilizan esas ocho respuestas. Reejecutar CP2 vuelve a llamar al agente; reejecutar CP4 vuelve a llamar al juez.
6. Abre Review App, revisa tres trazas y guarda valoraciones humanas con evidencia. Crear la sesión no completa la revisión. No compartir ni asignar usuarios durante esta práctica.

Para evaluar Supervisor en otro workspace habilitado, primero construye el Supervisor del kit con su Genie y herramienta de reposición, publica su endpoint real y configura `target=supervisor` y `supervisor_endpoint`. Verifica el contrato Responses y la auditoría expuesta antes de interpretar métricas. Si cambian tablas o configuración respecto del kit docente, crea una nueva versión del dataset y de sus oracles. La ejecución de Genie no depende de este paso.

## Correspondencia con S06

| Checkpoint original | Aplicación al kit UI | Evidencia esperada |
|---|---|---|
| CP0: conectar el agente S05 | Seleccionar Genie o Supervisor; validar recursos y Environment | IDs reales y estado del recurso; bloqueo Supervisor explícito |
| CP1: congelar preguntas y referencias | 16 casos, 8 por target; tres SQL independientes; ejemplos vistos, benchmark reservado y preguntas nuevas | Freeze, SQL, resultados y SHA del subconjunto seleccionado |
| CP2: separar inferencia y observación | Adaptador Genie con conversación nueva por pregunta; contrato Responses para Supervisor | 8 respuestas reales del target, trazas, SQL generado, tablas, IDs, errores y latencia |
| CP3: reglas observables y recuperación | Diagnóstico de cifras presentes, errores y latencia; RAG N/A | Resultado por caso y denominador aplicable; sin atribuir corrección global a coincidencias numéricas |
| CP4: juez con rúbrica | Correctness y relevance, JSON estricto; `mlflow.genai.evaluate` sobre outputs existentes | Veredictos, razones, errores de juez y run MLflow; sin segunda inferencia del agente |
| CP5.1: BLEU/ROUGE | Mantener el microexperimento en el notebook S06 original | Explicar por qué solapamiento textual no demuestra verdad; no duplicarlo aquí |
| CP5.2: revisión humana | Sesión privada con tres trazas reales; ningún usuario asignado | Tres valoraciones humanas guardadas, evidencia y corrección esperada |
| CP6: exportar y decidir | Artefactos JSON, conteos por split y denominadores; revisión pendiente impide aprobación | `evaluacion_s06_ui.json`, `dataset_s06_ui.json`, `scores_s06_ui.json` |

## Qué se está midiendo

El notebook S06 original evalúa el **ResponsesAgent de S05**, que orquesta funciones UC, recuperación documental y una llamada a Genie. Este complementario evalúa **el espacio Genie del kit directamente** y prepara la evaluación del **Supervisor del kit**. Son sujetos distintos: no mezcles sus resultados ni atribuyas la cobertura documental del primero al segundo.

La respuesta Genie puede consistir en texto y una tabla. El adaptador conserva ambos, más el SQL y el resultado crudo para auditoría. Una cifra presente es un diagnóstico: no acredita que corresponda a la categoría, periodo o fuente correctos. El juez y el humano deben contrastar esas asociaciones, el redondeo por línea, el mes parcial de mayo, las aclaraciones y la negativa a inventar costos o moneda.

No hay corpus documental recuperado con etiquetas de relevancia en este kit. Precision, recall y faithfulness RAG son **N/A**. Tampoco se evalúa selección interna de herramientas del Supervisor cuando el endpoint no expone auditoría. No fabriques listas de herramientas a partir del texto de la respuesta.

El reporte separa errores de ejecución, casos juzgados y juicios faltantes. Correctness y relevance reportan `n/denominator` sobre juicios válidos; el total de ocho intentos permanece visible. El diagnóstico numérico usa únicamente casos numéricos ejecutados sin error. Latencia incluye red y herramientas; p95 con ocho observaciones es descriptivo, no una garantía de servicio. Reporta por separado ejemplos vistos, benchmark reservado y preguntas nuevas; no entrenes sobre el benchmark reservado para después declararlo prueba independiente.

## Entrega

- Los tres JSON exportados como artefactos de MLflow y los IDs del experimento y runs.
- Tres revisiones humanas con referencia SQL, valoración y corrección esperada. Si siguen pendientes, declara `pendiente_humano`; no presentes el trabajo como aprobado para producción.
- Una decisión de mejora basada en una discrepancia real y un caso nuevo de regresión. Preserva los errores, respuestas originales y versiones del dataset.
- Una declaración explícita de cobertura: target ejecutado, 8 casos planeados, intentados, errores y juzgados. Para Supervisor bloqueado: 8 planeados, 0 intentados, sin score.

El muestreo online es un diseño para S08; estas trazas son offline. Este notebook no publica agentes, cambia permisos, notifica personas ni acredita revisión humana por sí mismo.

## Validación serverless docente

El runner `scripts/run_databricks.py` importa exclusivamente este notebook a `/Shared/curso-databricks-ai-engineer/s06-ui-evaluados/notebook` y lo ejecuta con `target=genie` y `oracle_mode=recompute`. Guarda el submit, estado, salida y reporte en `reports/`. Usa `--poll RUN_ID` para recuperar evidencia. Tanto runner como notebook verifican que Review App corresponda al workspace de ejecución; un enlace de otro workspace se suprime. Esta comprobación no cambia el estado pendiente de la revisión humana.

### Diferencia entre reporte docente y repetición del alumno

Los scripts docentes comparan tablas numéricas completas con el oracle y pueden registrar una comprobación de sustentación sobre SQL/tablas (`evidence_support`). Esas comprobaciones no son el scorer `cifra_referencia_presente` del notebook. El notebook del alumno ejecuta el diagnóstico de presencia numérica más el juez de correctness/relevance; **no reproduce automáticamente el 6/6 de tablas exactas ni el 6/6 de sustentación del reporte docente**. La sustentación SQL tampoco equivale a faithfulness RAG. Compara métricas por nombre, definición y denominador antes de contrastar corridas.

La selección de Review App también depende de cada corrida: prioriza errores o desacuerdos, después una aclaración y completa hasta tres trazas. Si todos pasan, puede seleccionar G07, G01 y G02. El reporte docente puede haber seleccionado G01, G06 y G08. Ambos son muestreos explícitos para revisión pendiente, no tres valoraciones humanas ya realizadas.

Corrida integral validada: `408870128707351` (`SUCCESS`, 21/09/2026), con los tres oracles recalculados, 8/8 inferencias sin errores, 8/8 juicios válidos, correctness **7/8**, relevance **8/8** y cifra presente **6/6**. La discrepancia queda preservada para revisión; no se regeneraron respuestas para buscar un aprobado. Evidencia: `reports/evaluation-408870128707351.json` y `reports/notebook-validation.json`. Las dos corridas previas quedaron conservadas como depuración del contrato JSON del juez, no como resultados de calidad válidos.
