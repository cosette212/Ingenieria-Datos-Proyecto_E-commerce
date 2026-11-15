# Prueba comparativa: CSV vs Parquet

- Fecha de la prueba: 2026-11-15 09:57
- Equipo: Windows-11-10.0.26200-SP0 | Python 3.12.5 | pandas 3.1.1
- Historico simulado: 484,500 filas, 10 columnas
- CSV: 365 archivos (uno por dia), 29.8 MB en total
- Parquet: 1 archivo, 3.0 MB (compresion snappy)
- Cada consulta se corrio 3 veces; se reporta la mediana.

| Consulta | CSV (s) | Parquet (s) | Veces mas rapido |
|---|---:|---:|---:|
| A. Lectura completa (10 columnas) | 1.12 | 0.05 | 23.0x |
| B. Top 5 productos por importe (4 columnas) | 0.77 | 0.05 | 17.0x |
| C. Unidades por mes (2 columnas) | 0.88 | 0.04 | 21.0x |

Relacion de tamano en disco (CSV / Parquet): 10.0x.
Resultados identicos entre ambos formatos: si.

Nota: los tiempos dependen del equipo; lo importante es la proporcion.
