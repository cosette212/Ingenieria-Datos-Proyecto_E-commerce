"""
FASE 2 - Paso 4: EXPORTAR la capa normalizada a Apache PARQUET.

Lee las tablas del modelo 3FN desde el almacen y escribe:
  data/curated/<tabla>.parquet   -> capa historica en formato columnar (una por tabla)
  data/sample/                   -> muestra de 100 filas de linea_pedido en CSV, JSON y Parquet

Despues de escribir cada Parquet lo vuelve a leer y compara el numero de filas con el almacen.

Requiere:  pip install pyarrow   (ya viene en requirements.txt)

Uso:
    python src/export_parquet.py
"""
import sqlite3
import sys
from pathlib import Path

import pandas as pd

try:
    import pyarrow  # noqa: F401  (pandas lo usa por debajo para escribir Parquet)
except ImportError:
    sys.exit("ERROR: falta pyarrow. Con el entorno activo ejecuta: pip install pyarrow")

ROOT = Path(__file__).resolve().parent.parent
WAREHOUSE = ROOT / "data" / "warehouse" / "tienda_dwh.db"
CURATED = ROOT / "data" / "curated"
SAMPLE = ROOT / "data" / "sample"

TABLAS = ["categoria", "subcategoria", "producto", "cliente", "pedido", "linea_pedido",
          "inventario", "movimiento_inventario", "evento_comportamiento",
          "rechazado_integridad"]

# columnas de fecha/hora que se guardan como TIMESTAMP real en Parquet
COLUMNAS_FECHA = {
    "cliente": ["fecha_registro"],
    "pedido": ["fecha_hora"],
    "inventario": ["ultima_actualizacion"],
    "movimiento_inventario": ["fecha_hora"],
    "evento_comportamiento": ["fecha_hora_evento"],
}


def leer_tabla(conn, tabla):
    df = pd.read_sql_query(f"SELECT * FROM {tabla}", conn)
    # enteros con vacios: pandas los lee como decimales; se pasan a entero que admite nulos
    for _, nombre, tipo, *_ in conn.execute(f"PRAGMA table_info({tabla})"):
        if tipo == "INTEGER" and nombre in df.columns:
            df[nombre] = pd.to_numeric(df[nombre], errors="coerce").astype("Int64")
    for col in COLUMNAS_FECHA.get(tabla, []):
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def main():
    if not WAREHOUSE.exists():
        sys.exit("ERROR: no existe el almacen. Ejecuta load_raw.py y transform.py primero.")
    CURATED.mkdir(parents=True, exist_ok=True)
    SAMPLE.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(WAREHOUSE)
    errores = 0

    print(f"{'tabla':<26}{'filas':>8}{'KB parquet':>12}  verificacion")
    for tabla in TABLAS:
        df = leer_tabla(conn, tabla)
        ruta = CURATED / f"{tabla}.parquet"
        df.to_parquet(ruta, index=False, compression="snappy")
        releido = pd.read_parquet(ruta)
        ok = len(releido) == len(df) and list(releido.columns) == list(df.columns)
        errores += 0 if ok else 1
        print(f"{tabla:<26}{len(df):>8}{ruta.stat().st_size / 1024:>12.1f}  "
              f"{'OK (releido igual)' if ok else 'FALLA'}")

    # muestra en los tres formatos que pide el entregable
    muestra = leer_tabla(conn, "linea_pedido").head(100)
    muestra.to_csv(SAMPLE / "muestra_linea_pedido.csv", index=False, encoding="utf-8")
    muestra.to_json(SAMPLE / "muestra_linea_pedido.json", orient="records",
                    force_ascii=False, indent=2)
    muestra.to_parquet(SAMPLE / "muestra_linea_pedido.parquet", index=False, compression="snappy")
    print(f"\nMuestra de 100 filas escrita en {SAMPLE} (CSV, JSON y Parquet)")
    conn.close()

    if errores:
        sys.exit(f"HUBO {errores} TABLAS CON PROBLEMAS")
    print("EXPORTACION terminada. Siguiente paso: python src/benchmark_parquet.py")


if __name__ == "__main__":
    main()
