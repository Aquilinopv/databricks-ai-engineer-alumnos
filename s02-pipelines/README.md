# Sesión 2 · Pipelines de datos (ETL)
**lunes 31 de agosto · 3 h**

## Qué vas a lograr
Convertir el script de la sesión 1 en un **pipeline de verdad**: que se pueda correr dos veces sin
duplicar nada, que procese solo lo que llegó nuevo, y que tenga las reglas de negocio escritas
adentro en vez de en la memoria de alguien.

## Temas
- Transformaciones con PySpark y SQL
- Ingesta incremental con Auto Loader; Change Data Feed
- Pipelines declarativos y orquestación con Jobs
- Calidad de datos: por qué la IA es tan buena como los datos que la alimentan

## Tu entregable
1. Pipeline **bronze → silver → gold** corriendo con los **23 meses** de datos, cargados en tandas
2. **Una expectativa de calidad propia**, y una fila que la viole a propósito
3. **Una tabla gold más**, con su regla de negocio explicada en el `COMMENT`
4. El job **agendado** para que corra solo

Súbelo a `entregas/tu-nombre/`.

## Lo que resolvemos hoy
El error que encontraste al final de la sesión 1. Hoy se arregla **una vez, en un solo lugar**,
y no vuelve nunca más. Eso es un pipeline.

## Prerrequisito
Necesitas tu `bronze` de la sesión 1. Si no llegaste a terminarlo, avisa **antes** de la clase.
