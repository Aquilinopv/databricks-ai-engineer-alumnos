# Manifiesto de lotes — Neptuno

**23 meses** · 830 pedidos · 2,155 líneas de detalle

| Mes | Pedidos | Líneas de detalle |
|---|---:|---:|
| 2024-07 | 22 | 59 |
| 2024-08 | 25 | 69 |
| 2024-09 | 23 | 57 |
| 2024-10 | 26 | 73 |
| 2024-11 | 25 | 66 |
| 2024-12 | 31 | 81 |
| 2025-01 | 33 | 85 |
| 2025-02 | 29 | 79 |
| 2025-03 | 30 | 77 |
| 2025-04 | 31 | 81 |
| 2025-05 | 32 | 96 |
| 2025-06 | 30 | 76 |
| 2025-07 | 33 | 77 |
| 2025-08 | 33 | 84 |
| 2025-09 | 37 | 95 |
| 2025-10 | 38 | 106 |
| 2025-11 | 34 | 89 |
| 2025-12 | 48 | 114 |
| 2026-01 | 55 | 152 |
| 2026-02 | 54 | 122 |
| 2026-03 | 73 | 178 |
| 2026-04 | 74 | 180 |
| 2026-05 | 14 | 59 |
| **total** | **830** | **2,155** |

## Cómo se usa en clase
1. Se copian los primeros meses al Volume y se corre el pipeline.
2. Cae un mes más → se vuelve a correr → **entra solo lo nuevo** (Auto Loader).
3. Se modifica una fila de silver → se lee el **Change Data Feed** para ver qué cambió.

> Los archivos de `pedidos/` y `detalles/` del mismo mes son consistentes entre sí:
> ninguna línea de detalle queda sin su cabecera.
