# Databricks AI Engineer — repositorio de alumnos

Bienvenido. Acá vive **todo el material que vas a usar en clase** y también **tus entregas**.

- **8 sesiones**, lunes y miércoles, 3 horas cada una.
- Del **26 de agosto** al **21 de septiembre de 2026**.
- Un solo proyecto atraviesa las 8 sesiones: **«Copiloto de Datos»**, sobre los datos de *Neptuno*,
  una distribuidora de alimentos importados.

## Cómo está organizado

```
s01-plataforma/      ← una carpeta por sesión
  README.md            la consigna: qué se hace y qué hay que entregar
  notebook.py          el notebook de la clase (se importa a Databricks)
  entregas/            👈 acá subís tu trabajo
s02-pipelines/
...
recursos/            datasets y material común a varias sesiones
```

## Cómo entregar

Cada sesión tiene su carpeta `entregas/`. Subí tu trabajo en una subcarpeta con tu nombre:

```
s01-plataforma/entregas/tu-nombre/notebook.py
```

Este repositorio es **público**, así que hay dos caminos según tu acceso:

**Si sos colaborador del repositorio** (te agregamos en la primera clase):

```bash
git clone https://github.com/manuelarguelles/databricks-ai-engineer-alumnos.git
cd databricks-ai-engineer-alumnos
git checkout -b entrega-s01-tu-nombre
# copiás tu notebook a s01-plataforma/entregas/tu-nombre/
git add s01-plataforma/entregas/tu-nombre
git commit -m "Entrega S01 - tu nombre"
git push origin entrega-s01-tu-nombre
```

**Si todavía no sos colaborador**, hacés un *fork* (botón «Fork» arriba a la derecha), trabajás
sobre tu copia y después abrís el Pull Request desde ahí. Funciona igual.

En los dos casos terminás abriendo un **Pull Request** contra `main`.
Si nunca usaste Git, no te preocupes: la primera vez lo hacemos juntos en clase.

> **Trabajá solo dentro de tu carpeta.** No modifiques los notebooks de la clase ni las entregas
> de otros: si querés experimentar sobre un notebook, hacete una copia dentro de tu carpeta.

## Antes de la primera clase

1. Tener acceso al workspace de Databricks (te lo pasamos por separado).
2. Tener Git instalado y una cuenta de GitHub.
3. Nada más. El curso arranca desde cero.

## El proyecto que vas a construir

| Fase | Sesiones | Qué construís |
|---|---|---|
| Datos | 1–2 | Un lakehouse gobernado y un pipeline que lo alimenta solo |
| Inteligencia | 3–5 | Un LLM sobre esos datos, búsqueda sobre documentos y un agente con herramientas |
| Confianza | 6–7 | Evaluación con métricas y guardrails de seguridad |
| Producción | 8 | El copiloto desplegado, con interfaz y monitoreo |

Al final tenés un artefacto real de portafolio — no un ejercicio de juguete.
