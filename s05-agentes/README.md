# S05 · Agentes sobre Neptuno

**3 horas reloj, incluidas dos pausas de 5 minutos.** Construimos el agente del Copiloto de Datos que continuaremos evaluando en S06, protegiendo en S07 y desplegando en S08. El caso SBS es un laboratorio complementario separado.

Paquete validado por el instructor: 7/7 casos en Databricks y 11 pruebas locales. Reproduce los checkpoints en tu entorno; resultados de ejemplo en [reports/demo-funcional.json](reports/demo-funcional.json).

## Orden de trabajo

1. Lee [CONSIGNA.md](CONSIGNA.md): CP0–CP5 y criterios de entrega.
2. Importa [notebook.py](notebook.py) a Databricks. Ejecuta la celda de widgets, escribe tu catálogo y después valida configuración.
3. Sigue el notebook en orden. Revisa el contrato en [IMPLEMENTACION-NOTEBOOK.md](IMPLEMENTACION-NOTEBOOK.md) si necesitas diagnosticar un fallo.
4. Para MCP externo, usa tu PC: serverless bloquea el DNS de Microsoft Learn. Desde esta carpeta ejecuta:

```sh
uv run --with 'mcp==1.30.0' python scripts/mcp_external_demo.py --output reports/mcp-external-local.json
```

Si usas tu propio entorno Python, instala [scripts/requirements-mcp-external.txt](scripts/requirements-mcp-external.txt) y ejecuta [scripts/mcp_external_demo.py](scripts/mcp_external_demo.py). El reporte anterior es un archivo que crearás, no un recurso faltante.

5. Revisa [docs/AGENT-BRICKS.md](docs/AGENT-BRICKS.md) y completa el ejercicio [docs/alhf-ejercicio.json](docs/alhf-ejercicio.json).
6. Crea `entregas/s05/evidencia.md` en tu copia del repositorio y entrega notebook exportado sin secretos. CONSIGNA detalla el contenido.

## Prerrequisitos

- S01–S02: catálogo propio y tablas gold de Neptuno.
- S04: `rag.chunks_embeddings`, con embeddings Qwen3 de 1024 dimensiones.
- Endpoint que soporte tool calling, permisos UC y acceso al Genie Space curado.
- Para Secrets, scope/key de demostración con valor explícitamente sintético; nunca publiques valores de credenciales.
- En la PC: Python y `uv`, o entorno Python con las dependencias indicadas.

## Evidencia entregada

- [reports/demo-bricks.json](reports/demo-bricks.json): extracción observada y opciones UI, sin identidades ni datos de cuenta.
- [reports/demo-mcp-external.json](reports/demo-mcp-external.json): resumen de llamada externa real desde PC.
- [tests/test_contracts.py](tests/test_contracts.py): pruebas locales; sus dobles de prueba no acreditan Databricks.

No se entregan reportes internos de workspace ni juicios privados. La demo Bricks no afirma Supervisor/Knowledge Assistant desplegados ni optimización ALHF ejecutada. External desde PC no equivale a éxito desde serverless.
