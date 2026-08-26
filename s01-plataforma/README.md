# Sesión 1 · La plataforma de datos desde cero
**miércoles 26 de agosto · 3 h**

## Qué vas a lograr
Moverte con soltura en Databricks y entender el modelo Lakehouse. Al terminar vas a tener
**los datos de Neptuno cargados en tu propio catálogo gobernado** — el cimiento del proyecto.

## Temas
- La plataforma: workspace, notebooks, clusters y SQL warehouses
- Lakehouse y arquitectura medallón (bronze / silver / gold)
- Unity Catalog: catálogos, esquemas, tablas, Volumes y permisos
- SQL en Databricks y Delta Lake: transacciones y viaje en el tiempo

## Tu entregable
1. Las **8 tablas** de Neptuno en tu esquema `bronze`
2. **Tres consultas SQL** que respondan preguntas de negocio reales
3. **Una tabla documentada** con `COMMENT ON TABLE`
4. Un cambio provocado y revertido con `RESTORE`

Subilo a `entregas/tu-nombre/`.

## Cómo empezar
1. Importá `notebook.py` a tu workspace de Databricks.
2. Escribí tu nombre en el widget de arriba del notebook.
3. Seguí las celdas en orden.

## La pregunta con la que cerramos
En la última parte vas a calcular un porcentaje de error sobre las ventas.
**Anotá ese número.** En la sesión 2 lo hacemos desaparecer para siempre.

---

### Si te trabás
- **«No tengo permisos para crear el catálogo»** → avisá en el chat; puede ser configuración de tu cuenta.
- **«El notebook no encuentra los CSV»** → revisá que estén en tu Volume `bronze/landing`,
  y que el nombre del archivo sea exactamente el esperado.
- **«Mi cluster no arranca»** → usá el SQL Warehouse para las celdas de SQL y avisá.
