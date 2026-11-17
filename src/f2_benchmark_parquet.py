"""
FASE 2 - Paso 5: PRUEBA COMPARATIVA  CSV (muchos archivos)  vs  Parquet (un archivo).

Para que la diferencia se note, el script construye un HISTORICO SIMULADO de ventas:
toma las lineas de pedido reales del almacen y las repite ~100 veces con fechas repartidas
en ~1 anio (cerca de 500 mil filas). Luego guarda ese historico de dos formas:
  - data/benchmark/csv/ventas_AAAA-MM-DD.csv   (un CSV por dia: ~365 archivos)
  - data/benchmark/ventas_hist.parquet         (un solo archivo columnar)
y mide cuanto tardan 3 consultas equivalentes en cada formato (mediana de 3 corridas).

Resultados: se imprimen y se guardan en docs/Fase 2/benchmark_resultado.md

Uso:
    python src/benchmark_parquet.py
"""
import math
import platform
import shutil
import sqlite3
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import pyarrow  # noqa: F401
except ImportError:
    sys.exit("ERROR: falta pyarrow. Con el entorno activo ejecuta: pip install pyarrow")

ROOT = Path(__file__).resolve().parent.parent
WAREHOUSE = ROOT / "data" / "warehouse" / "tienda_dwh.db"
BENCH = ROOT / "data" / "benchmark"
CSV_DIR = BENCH / "csv"
PARQUET = BENCH / "ventas_hist.parquet"
REPORTE = ROOT / "docs" / "Fase 2" / "benchmark_resultado.md"

COPIAS = 100        # cuantas veces se repite la tabla real para simular historico
DIAS_EXTRA = 275    # reparte las fechas hacia atras (con los ~90 dias reales ~ 1 anio)
REPETICIONES = 3


# ---------------------------------------------------------------- datos
def cargar_base():
    conn = sqlite3.connect(WAREHOUSE)
    df = pd.read_sql_query(
        """
        SELECT l.id_pedido, p.id_cliente, p.fecha_hora, l.id_producto, l.cantidad,
               l.precio_unitario, l.descuento, p.estado, p.metodo_pago, p.canal_venta
        FROM linea_pedido AS l
        JOIN pedido AS p ON p.id_pedido = l.id_pedido
        """, conn)
    conn.close()
    df["fecha"] = pd.to_datetime(df["fecha_hora"], errors="coerce").dt.normalize()
    df = df.drop(columns=["fecha_hora"]).dropna(subset=["fecha", "cantidad", "precio_unitario",
                                                        "descuento"])
    df["cantidad"] = df["cantidad"].astype("int64")
    df["descuento"] = df["descuento"].astype("float64")
    return df


def construir_historico(base):
    rng = np.random.default_rng(42)
    partes = []
    for _ in range(COPIAS):
        copia = base.copy()
        desplazamiento = pd.to_timedelta(rng.integers(0, DIAS_EXTRA, size=len(base)), unit="D")
        copia["fecha"] = base["fecha"] - desplazamiento
        partes.append(copia)
    return pd.concat(partes, ignore_index=True)


def escribir_formatos(hist):
    if BENCH.exists():
        shutil.rmtree(BENCH)
    CSV_DIR.mkdir(parents=True)
    dias = hist["fecha"].dt.strftime("%Y-%m-%d")
    for dia, grupo in hist.groupby(dias):
        grupo.to_csv(CSV_DIR / f"ventas_{dia}.csv", index=False, encoding="utf-8")
    hist.to_parquet(PARQUET, index=False, compression="snappy")


# ------------------------------------------------------------ consultas
def leer_csv(columnas):
    archivos = sorted(CSV_DIR.glob("*.csv"))
    partes = [pd.read_csv(a, usecols=columnas) for a in archivos]
    return pd.concat(partes, ignore_index=True)


def leer_parquet(columnas):
    return pd.read_parquet(PARQUET, columns=columnas)


def consulta_a(leer):
    """Lectura completa: todas las columnas, cuenta filas y suma unidades."""
    df = leer(None)
    return int(len(df)), int(df["cantidad"].sum())


def consulta_b(leer):
    """Top 5 productos por importe (usa 4 de las 10 columnas)."""
    df = leer(["id_producto", "cantidad", "precio_unitario", "descuento"])
    importe = df["cantidad"] * df["precio_unitario"] * (1 - df["descuento"])
    top = importe.groupby(df["id_producto"]).sum().nlargest(5)
    return [(int(k), float(v)) for k, v in top.items()]


def consulta_c(leer):
    """Unidades vendidas por mes (usa 2 de las 10 columnas)."""
    df = leer(["fecha", "cantidad"])
    fechas = pd.to_datetime(df["fecha"])
    por_mes = df["cantidad"].groupby(fechas.dt.to_period("M")).sum()
    return [(str(k), int(v)) for k, v in por_mes.items()]


def son_iguales(a, b):
    """Compara resultados; los decimales se comparan con tolerancia minima."""
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        return len(a) == len(b) and all(son_iguales(x, y) for x, y in zip(a, b))
    if isinstance(a, float) or isinstance(b, float):
        return math.isclose(a, b, rel_tol=1e-9)
    return a == b


def medir(funcion, leer):
    tiempos, resultado = [], None
    for _ in range(REPETICIONES):
        t0 = time.perf_counter()
        resultado = funcion(leer)
        tiempos.append(time.perf_counter() - t0)
    return statistics.median(tiempos), resultado


def tamano_mb(ruta):
    if ruta.is_dir():
        return sum(f.stat().st_size for f in ruta.glob("*.csv")) / 1024 / 1024
    return ruta.stat().st_size / 1024 / 1024


def main():
    if not WAREHOUSE.exists():
        sys.exit("ERROR: no existe el almacen. Ejecuta load_raw.py y transform.py primero.")

    print("1/4 Leyendo lineas de pedido del almacen ...")
    base = cargar_base()
    print(f"    filas reales: {len(base)}")
    print("2/4 Construyendo historico simulado ...")
    hist = construir_historico(base)
    print(f"    filas simuladas: {len(hist):,}")
    print("3/4 Escribiendo CSV por dia y Parquet (puede tardar un poco) ...")
    escribir_formatos(hist)
    n_csv = len(list(CSV_DIR.glob("*.csv")))
    mb_csv, mb_parquet = tamano_mb(CSV_DIR), tamano_mb(PARQUET)
    print(f"    archivos CSV: {n_csv} | tamano CSV: {mb_csv:.1f} MB | Parquet: {mb_parquet:.1f} MB")

    print("4/4 Midiendo consultas (mediana de 3 corridas) ...")
    pruebas = [
        ("A. Lectura completa (10 columnas)", consulta_a),
        ("B. Top 5 productos por importe (4 columnas)", consulta_b),
        ("C. Unidades por mes (2 columnas)", consulta_c),
    ]
    filas = []
    iguales = True
    for nombre, funcion in pruebas:
        t_csv, r_csv = medir(funcion, leer_csv)
        t_par, r_par = medir(funcion, leer_parquet)
        coinciden = son_iguales(r_csv, r_par)
        iguales = iguales and coinciden
        filas.append((nombre, t_csv, t_par, t_csv / t_par if t_par > 0 else float("inf"), coinciden))

    print(f"\n{'Consulta':<46}{'CSV (s)':>10}{'Parquet (s)':>13}{'Veces mas rapido':>18}")
    for nombre, t_csv, t_par, veces, _ in filas:
        print(f"{nombre:<46}{t_csv:>10.2f}{t_par:>13.2f}{veces:>17.1f}x")
    print(f"\nTamano en disco: CSV {mb_csv:.1f} MB ({n_csv} archivos) | Parquet {mb_parquet:.1f} MB "
          f"(1 archivo) | relacion CSV/Parquet: {mb_csv / mb_parquet:.1f}x")
    print("Resultados identicos entre formatos:", "SI" if iguales else "NO (revisar)")

    REPORTE.parent.mkdir(parents=True, exist_ok=True)
    lineas = [
        "# Prueba comparativa: CSV vs Parquet",
        "",
        f"- Fecha de la prueba: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"- Equipo: {platform.platform()} | Python {platform.python_version()} | "
        f"pandas {pd.__version__}",
        f"- Historico simulado: {len(hist):,} filas, 10 columnas",
        f"- CSV: {n_csv} archivos (uno por dia), {mb_csv:.1f} MB en total",
        f"- Parquet: 1 archivo, {mb_parquet:.1f} MB (compresion snappy)",
        f"- Cada consulta se corrio {REPETICIONES} veces; se reporta la mediana.",
        "",
        "| Consulta | CSV (s) | Parquet (s) | Veces mas rapido |",
        "|---|---:|---:|---:|",
    ]
    for nombre, t_csv, t_par, veces, _ in filas:
        lineas.append(f"| {nombre} | {t_csv:.2f} | {t_par:.2f} | {veces:.1f}x |")
    lineas += [
        "",
        f"Relacion de tamano en disco (CSV / Parquet): {mb_csv / mb_parquet:.1f}x.",
        f"Resultados identicos entre ambos formatos: {'si' if iguales else 'NO'}.",
        "",
        "Nota: los tiempos dependen del equipo; lo importante es la proporcion.",
    ]
    REPORTE.write_text("\n".join(lineas) + "\n", encoding="utf-8")
    print(f"\nReporte guardado en {REPORTE}")
    return 0 if iguales else 1


if __name__ == "__main__":
    sys.exit(main())
