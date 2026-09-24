# Demostración en vivo · S07

Abre el notebook de continuidad. CP0–CP3 muestran el histórico y Review App; CP4 crea y mide **otra serie**; CP5 vuelve a mostrar el histórico.

## Dónde obtener el ID del espacio

En Databricks abre Genie y selecciona el espacio que usarás como origen. En una URL con `/genie/rooms/<space_id>`, copia solamente el identificador del espacio, sin parámetros de la URL. Confirma nombre, tablas y sus 20 Benchmarks antes de pegarlo en `source_space_id`. El usuario que ejecuta debe poder crear clones y consultar esas tablas. Un clon mejorado usado como origen se convierte en tu V0 actual, no en el baseline histórico.

## Recorrido

1. Ejecuta CP0–CP4 (configuración) con `paso_live=lectura`. Define un `serie_id` nuevo y el `source_space_id` confirmado. Necesitas acceso a sus tablas, warehouse, Genie, MLflow y juez.
2. Selecciona `preparar` y ejecuta CP4.1: snapshot del origen y clon V0. Verifica las 20 preguntas exportadas.
3. Selecciona `congelar` y ejecuta CP4.2: SQL de referencia → gold inmutable.
4. Selecciona `evaluar_V0`, ejecuta CP4.3 y espera el reporte de 20 casos. Inspecciona fallos, errores y juicios ausentes.
5. Edita `CAMBIO_V1`; selecciona `crear_V1` y ejecuta CP4.4. Después `evaluar_V1` → CP4.5.
6. Edita `CAMBIO_V2` (solo el delta); `crear_V2` → CP4.6; `evaluar_V2` → CP4.7. V2 conserva las instrucciones de V1.
7. Ejecuta CP4.8: compara TU serie, identifica correcciones y regresiones. No sustituye el histórico 17/19/20.
8. Pon `version_activa=V0`, `paso_live=activar` y ejecuta CP4.9. Abre el enlace devuelto: ese es el rollback de la demo. Para volver a V2 cambia `version_activa=V2` e incrementa `intento_live`. Los reportes permanecen.
9. V3/V4: CP4.10 permite crear/evaluar un nuevo delta; la tabla incluye versiones extra, el gráfico heredado solo V0/V1/V2.

## Cómo observar el proceso

`series/<serie_id>/source-config.json` captura el origen. `series.json` conserva versiones, IDs, padre, cambio, configuración y eventos. `gold20.json` fija referencias. `reports/V*.json` guarda casos y run; `V*-config.json` la configuración evaluada; `V*-verified.json` confirma la configuración al finalizar. Abrir un enlace Genie permite revisar las instrucciones capturadas; en este recorrido los cambios se aplican desde las celdas. Editar por UI después de capturar dispara la protección de integridad.

El evaluador ejecuta 20 SQL de precheck antes de cada versión; luego 20 conversaciones independientes y hasta 20 juicios LLM, con trazas MLflow y persistencia incremental. No utiliza el evaluador nativo de Benchmarks ni crea etiquetas humanas.

## Recuperación

No usar Run all con una acción seleccionada: ejecuta celdas individuales. Los intentos se consumen en memoria antes de operar y se preservan archivos que impiden sobrescritura. Si falla, revisa estado y reportes antes de incrementar `intento_live`. Un parcial no se puede sobrescribir ni usar como corrida completa para crear la siguiente versión: conserva el diagnóstico e inicia otra serie si necesitas repetir. Si la creación remota se interrumpe, `reports/V*-created.json` permite localizar el clon creado; nunca se borra automáticamente. No ejecutar dos notebooks sobre la misma serie simultáneamente.

Rollback cambia **el puntero y enlace de la demo**; no despliega un endpoint, no restaura datos ni edita el espacio fuente. Volver a empezar significa otro `serie_id`, no borrar evidencia. Si cambian datos o gold, es un examen nuevo; no comparar como si solo hubiese cambiado el agente.

## Tiempo y pruebas

Las tres corridas son 60 preguntas más hasta 60 juicios. Su duración depende de Genie/warehouse/juez y puede exceder los 45 minutos iniciales de clase. Iniciar corridas antes y mostrar un paso en vivo; identificar cualquier resultado preparado.

Pruebas locales del controlador (Genie simulado) y evaluador: `python -m unittest -v test_live_workflow test_benchmark`. No equivalen a las 60 inferencias reales. La validación remota del material se documenta por separado.
