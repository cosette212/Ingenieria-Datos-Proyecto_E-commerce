"""
FASE 3 - Paso 3: EXPORTAR la CAPA LIMPIA a Apache PARQUET.

Escribe en data/clean/ (un Parquet por tabla):
  cliente, producto, pedido, linea_pedido, inventario, movimiento_inventario,
  evento_comportamiento   -> capa limpia (validada e imputada)
  categoria, subcategoria -> tablas de referencia de la Fase 2
  dlq, imputacion_log     -> auditoria de lo rechazado, corregido e imputado
Tambien escribe data/clean/dlq_auditoria.csv (para abrir en Excel; esta carpeta SI se sube a Git).

Despues de escribir cada archivo lo vuelve a leer y compara filas y columnas.
data/curated/ (Fase 2) NO se toca: queda como evidencia de la capa normalizada sin limpiar.

Uso:
    python src/f3_export_clean.py
"""
import sys

import pandas as pd

try:
    import pyarrow  # noqa: F401  (pandas lo usa por debajo para escribir Parquet)
except ImportError:
    sys.exit("ERROR: falta pyarrow. Con el entorno activo ejecuta: pip install pyarrow")

from f3_comun import CLEAN_DIR, conectar, exigir_tablas

# (nombre del archivo, tabla en el almacen)
TABLAS = [
    ("cliente", "clean_cliente"),
    ("producto", "clean_producto"),
    ("pedido", "clean_pedido"),
    ("linea_pedido", "clean_linea_pedido"),
    ("inventario", "clean_inventario"),
    ("movimiento_inventario", "clean_movimiento_inventario"),
    ("evento_comportamiento", "clean_evento_comportamiento"),
    ("categoria", "categoria"),
    ("subcategoria", "subcategoria"),
    ("dlq", "dlq"),
    ("imputacion_log", "imputacion_log"),
]

COLUMNAS_FECHA = {"fecha_registro", "fecha_hora", "ultima_actualizacion", "fecha_hora_evento",
                  "registrado_en"}


def leer_tabla(conn, tabla):
    df = pd.read_sql_query(f"SELECT * FROM {tabla}", conn)
    for _, nombre, tipo, *_ in conn.execute(f"PRAGMA table_info({tabla})"):
        if tipo == "INTEGER" and nombre in df.columns:
            df[nombre] = pd.to_numeric(df[nombre], errors="coerce").astype("Int64")
        if nombre in COLUMNAS_FECHA and nombre in df.columns:
            df[nombre] = pd.to_datetime(df[nombre], errors="coerce")
    return df


def main():
    conn = conectar(foreign_keys=False)
    exigir_tablas(conn, [t for _, t in TABLAS], "python src/f3_validate.py (y f3_impute.py)")
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    print(f"{'archivo':<26}{'filas':>8}{'KB':>9}  verificacion")
    errores = 0
    for nombre, tabla in TABLAS:
        df = leer_tabla(conn, tabla)
        ruta = CLEAN_DIR / f"{nombre}.parquet"
        df.to_parquet(ruta, index=False, compression="snappy")
        releido = pd.read_parquet(ruta)
        en_almacen = conn.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
        ok = (len(releido) == len(df) == en_almacen) and list(releido.columns) == list(df.columns)
        errores += 0 if ok else 1
        print(f"{nombre:<26}{len(df):>8}{ruta.stat().st_size / 1024:>9.1f}  "
              f"{'OK (releido igual al almacen)' if ok else 'FALLA'}")

    dlq = leer_tabla(conn, "dlq")
    csv_dlq = CLEAN_DIR / "dlq_auditoria.csv"
    dlq.to_csv(csv_dlq, index=False, encoding="utf-8-sig")
    print(f"\nDLQ de auditoria para Excel: {csv_dlq} ({len(dlq)} filas)")
    conn.close()

    if errores:
        sys.exit(f"HUBO {errores} ARCHIVOS CON PROBLEMAS")
    print("EXPORTACION terminada. Capa limpia en data/clean/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
