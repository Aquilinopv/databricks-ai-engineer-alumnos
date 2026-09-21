# S06 · Evaluar el Copiloto Neptuno

**Práctica distribuida:** tiempos integrados en la agenda docente de 3 h; prepara CP0 antes del inicio cuando sea posible. Trabaja en pareja: operador y revisor. El agente conserva su implementación S05; tu producto es evidencia para decidir qué corregir.

## Antes de comenzar — CP0

Importa `notebook.py` en `s06-evaluacion` al lado de la carpeta `s05-agentes`, que ya debe contener los notebooks 00–04 de S05. Abre Environment, añade las versiones del encabezado y aplica. Completa `catalogo` con tu catálogo S01–S05. Ejecuta widgets y `%run`; si tu estructura es distinta, edita la ruta de esa celda. No pegues tokens. Necesitas Gold S02, embeddings S04, funciones UC y acceso al Genie compartido.

**Aceptación:** existen `agente`, `preguntar`, `rag_rows` y `UC_MAP`; las demos heredadas terminan. Si una demo falla, registra su celda y error como prerrequisito fallido: no atribuyas una calidad cero al agente por no poder evaluar.

## CP1 · Preguntas y referencias

Ejecuta la consulta SQL independiente. Lee el documento que imprime la celda. Los diez casos cubren ventas, dos aclaraciones, falta de costos, escritura, documento, pregunta compuesta, Genie, ausencia documental e inyección.

Antes de inferir, anota cuáles deben usar herramientas y cuáles deben abstenerse. Confirma o corrige `relevant_docs` leyendo el corpus completo: la pregunta pide resumir un documento, por eso la unidad de relevancia es documento. Estas etiquetas propuestas NO son una anotación humana ya validada.

**Aceptación:** dataset con preguntas, respuestas esperadas, fuente de cada referencia y hash. Añade un undécimo caso de tu negocio sin copiar una respuesta generada como verdad. La cifra Genie exige cotejo del SQL del espacio `neptuno_ai`; no la compares con otro catálogo.

## CP2 · Ejecutar y observar

Ejecuta el adapter sobre el dataset. Abre una traza y localiza: pregunta → decisión del modelo → herramienta → evidencia → respuesta. Anota la latencia y la diferencia entre respuesta final y resultado de tool.

**Aceptación:** una salida real por caso (10 base + casos añadidos), con `trace_id`, auditoría y tiempo. No sustituyas errores por textos vacíos. Una respuesta plausible sin fuente requiere revisión.

## CP3 · Puntuar recuperación y comportamiento

Calcula a mano el ejemplo recuperados `{A,B,C}`, relevantes `{A,D}`: precisión `1/3`, recall `1/2`. Después calcula ambos para tu caso documental. Observa que deduplicamos documento, no chunk. Explica por qué una cita presente no prueba faithfulness.

**Aceptación:** scorer de herramientas, comparación numérica tool contra SQL y dos métricas RAG con denominadores visibles. Recall N/A cuando no hay documentos relevantes en el corpus (caso sin evidencia); no se divide por cero. Precisión 0 si recupera contexto irrelevante. N/A cuando no existe etiqueta; nunca convertir N/A a 1. Una llamada a tool correcta no prueba una respuesta final correcta.

## CP4 · MLflow y juez

Ejecuta `mlflow.genai.evaluate` y abre su run. Distingue API moderna GenAI de `mlflow.evaluate` legacy. Revisa corrección, relevancia y sustentación por separado. El juez usa el mismo modelo del agente: registra auto preferencia como riesgo. Prueba un criterio distinto o segundo modelo solo si tienes endpoint autorizado; conserva la versión del juez.

**Aceptación:** run ID, tabla por caso y razón breve del juez. Selecciona al menos un desacuerdo plausible para revisión. Un promedio alto no compensa una operación prohibida.

## CP5.1 · Métricas tradicionales

Ejecuta el ejemplo sintético BLEU/ROUGE. Compara paráfrasis correcta frente a negación incorrecta. BLEU aparece de 0 a 100 y ROUGE-L F1 de 0 a 1. Este microejemplo NO es una medición del agente real.

**Aceptación:** explica cuál puede obtener mejor solapamiento y por qué eso no acredita verdad. BLEU no debe ser gate de seguridad del copiloto.

## CP5.2 · Valoración humana y diseño online

Abre el enlace de Review App que crea el notebook, sin compartirlo a terceros. Hay tres trazas reales cargadas. En cada una lee pregunta, evidencia y respuesta, y guarda EXPECTED_RESPONSE. En tu entrega registra además: `case_id`, `trace_id`, aprobada/rechazada, evidencia concreta, corrección esperada y responsable humano. No atribuyas al sistema una revisión que hiciste tú, ni declares aprobada una fila aún pendiente.

Alternativa UI: Experiments → S06-Neptuno-Evaluacion → Labeling sessions → Create session → EXPECTED_RESPONSE; agregar trazas y guardar valoración. Si falla por permisos, registra el bloqueo y conserva tus valoraciones en JSON; no declares Review App operativa en ese entorno.

Diseña muestreo online para S08: 10% de tráfico, 100% de errores, exclusión de datos sensibles, responsable diario y umbral de alerta. Hoy no hay tráfico de producción ni evaluación online desplegada.

**Aceptación:** tres valoraciones humanas concretas y al menos un fallo confirmado convertido en caso de regresión (si no observaste fallos, crea un caso adversarial nuevo y márcalo como tal).

### Plantilla para `revision_humana.json`

Completa una entrada por cada traza revisada (mínimo tres). `Expected Response` en Review App guarda la respuesta esperada; tu veredicto y justificación se conservan también en este JSON. `pending` nunca cuenta como aprobación.

```json
[
  {
    "case_id": "documento",
    "trace_id": "REEMPLAZAR_POR_TRACE_REAL",
    "status": "pending",
    "evidence": "Documento/chunk o SQL y dato concreto cotejado",
    "corrected_response": "Escribir referencia correcta o explicar por qué la original es aceptable",
    "human_reviewer": "Nombre del alumno que realizó la revisión"
  }
]
```

Usa `approved` o `rejected` solo después de cotejar la evidencia y guardar la valoración. No atribuyas una valoración automática a un humano.

## CP6 · Entregar y decidir

En el run `S06-evidencia-exportada`, descarga `evaluacion_s06.json`, `dataset_s06.json` y `scores_s06.json`. Entrega junto a `revision_humana.json` (tus tres valoraciones) y `decision.md`: aprobar/no aprobar, motivo, métricas por tipo de caso, limitaciones, una corrección y cómo volverás a medirla. La revisión humana pendiente bloquea la aprobación de producción.

No se exige que el agente obtenga todo PASS: se exige medir honestamente, aislar el defecto y reproducirlo. No cambies referencias para mejorar una nota.

Fuentes: [MLflow custom scorers](https://mlflow.org/docs/latest/genai/eval-monitor/scorers/custom/), [GenAI evaluation](https://mlflow.org/docs/latest/genai/eval-monitor/quickstart/), [Review App](https://docs.databricks.com/aws/en/mlflow3/genai/human-feedback/concepts/labeling-sessions).
