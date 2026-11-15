"""
FASE 2 - Paso 2: TRANSFORM (la "T" de ELT).

Ejecuta, DENTRO del almacen (SQLite), los scripts SQL:
  sql/01_staging.sql      -> tipos correctos + desanidar JSON (1FN) + traduccion de tipos (Join roto)
  sql/02_modelo_3fn.sql   -> tablas normalizadas (3FN) con llaves primarias y foraneas

Las funciones to_int, to_real, to_date, to_ts, to_text son "casts seguros": si un valor no se
puede convertir devuelven NULL en lugar de detener el proceso.

Uso:
    python src/transform.py
"""
import math
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WAREHOUSE = ROOT / "data" / "warehouse" / "tienda_dwh.db"
SQL_DIR = ROOT / "sql"


# ------------------------------------------------------------ casts seguros
def to_text(v):
    if v is None:
        return None
    s = str(v).strip()
    return s if s != "" else None


def to_int(v):
    """'105', ' 105 ', 105, 105.0 -> 105.   'veinte', '', None, 3.7 -> None."""
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v) if math.isfinite(v) and v.is_integer() else None
    s = str(v).strip()
    if s == "":
        return None
    try:
        return int(s)
    except ValueError:
        try:
            f = float(s)
            return int(f) if math.isfinite(f) and f.is_integer() else None
        except ValueError:
            return None


def to_real(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return float(v)
    if isinstance(v, (int, float)):
        f = float(v)
        return f if math.isfinite(f) else None
    s = str(v).strip()
    if s == "":
        return None
    try:
        f = float(s)
        return f if math.isfinite(f) else None
    except ValueError:
        return None


def to_date(v):
    """Acepta AAAA-MM-DD valida. Cualquier otra cosa -> None."""
    s = to_text(v)
    if s is None:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        return None


def to_ts(v):
    """Acepta AAAA-MM-DD HH:MM:SS o AAAA-MM-DDTHH:MM:SS. Devuelve 'AAAA-MM-DD HH:MM:SS'."""
    s = to_text(v)
    if s is None:
        return None
    for formato in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, formato).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return None


def conectar():
    if not WAREHOUSE.exists():
        sys.exit("ERROR: no existe el almacen. Ejecuta primero: python src/load_raw.py")
    conn = sqlite3.connect(WAREHOUSE)
    conn.execute("PRAGMA foreign_keys = ON")
    for nombre, funcion in (("to_text", to_text), ("to_int", to_int), ("to_real", to_real),
                            ("to_date", to_date), ("to_ts", to_ts)):
        conn.create_function(nombre, 1, funcion, deterministic=True)
    return conn


def ejecutar_sql(conn, archivo):
    print(f"Ejecutando {archivo.name} ...")
    conn.executescript(archivo.read_text(encoding="utf-8"))


def main():
    conn = conectar()
    try:
        ejecutar_sql(conn, SQL_DIR / "01_staging.sql")
        ejecutar_sql(conn, SQL_DIR / "02_modelo_3fn.sql")
        conn.commit()

        print("\nTablas del modelo normalizado (3FN):")
        for tabla in ("categoria", "subcategoria", "producto", "cliente", "pedido",
                      "linea_pedido", "inventario", "movimiento_inventario",
                      "evento_comportamiento", "rechazado_integridad"):
            n = conn.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
            print(f"  {tabla:<24}{n:>7} filas")

        print("\nRechazados por integridad referencial:")
        filas = conn.execute("SELECT tabla_destino, motivo, COUNT(*) FROM rechazado_integridad "
                             "GROUP BY 1, 2 ORDER BY 1, 2").fetchall()
        if not filas:
            print("  (ninguno)")
        for tabla, motivo, n in filas:
            print(f"  {tabla:<24}{motivo:<36}{n:>5}")
    finally:
        conn.close()
    print("\nTRANSFORM terminado. Siguiente paso: python src/validate_phase2.py")


if __name__ == "__main__":
    main()
