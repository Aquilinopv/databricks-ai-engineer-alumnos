# S07 · Gobierno y seguridad de IA

[Índice de materiales](index.html). Descarga el repositorio y abre los HTML en tu navegador; GitHub muestra su código, no reproduce las presentaciones.

## Material de clase

- [1 · Evaluación: manual primero](slides/01-evaluacion-manual-automatica.html).
- [3 · Responsible AI](slides/03-responsible-ai.html).
- [4 · S07: aplicar controles](slides/04-gobierno-controles.html).
- [5 · Qué significa gobernar IA](slides/05-gobierno-ia.html).
- [Cómo implementamos los guardrails](slides/06-implementacion-guardrails.html).

## Notebook principal — en esta carpeta

[notebook.py](notebook.py): laboratorio S07 sobre AgenteNeptuno, Unity Catalog y guardrails. [Abrir el notebook validado](https://dbc-0410b264-20c7.cloud.databricks.com/editor/notebooks/1155658220803956?o=7474657121564806).

Antes de ejecutar: completar S02/S04/S05, configurar catálogo y revisar la ruta `%run` a S05. El widget de moderación usa `ai_gateway` y el endpoint8B con Safety activo. La configuración del filtro afecta todas las llamadas a ese endpoint. Otro workspace necesita sus propios recursos y permisos.

Validación: Job251323974338111SUCCESS;4/4casos del agente,4/4componentes,2/2pruebas de moderación;6llamadas al filtro. [Evidencia](recursos/evidencia/guardrails-validacion.json). No es una certificación universal de seguridad. Vector Search no configurado.

## Recursos de apoyo — arrastre de S06

- [Notebook de comparación Ianbal](recursos/ianbal/validacion_final.py), con [prerrequisitos y ejecución](recursos/ianbal/README.md).
- [Guía de primera ejecución en slides](recursos/primera-ejecucion.html).
- [Repositorio IaC de Ianbal](https://github.com/manuelarguelles/ianbal-genie-iac): configuración, versiones y referencias del benchmark.

Las trazas de Review App ya están preparadas; la demostración de clase abre el formulario sin guardar valoraciones. Los enlaces Databricks requieren permisos independientes del acceso a GitHub.
