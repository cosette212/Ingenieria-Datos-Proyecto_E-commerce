"""
FASE 3 - Utilidades compartidas por los scripts f3_*.py

  - Rutas del proyecto
  - conectar(): abre el almacen y registra los MISMOS casts seguros de la Fase 2
    (se importan de f2_transform.py para que las dos fases interpreten los tipos igual)
"""
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

from f2_transform import to_date, to_int, to_real, to_text, to_ts

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
WAREHOUSE = ROOT / "data" / "warehouse" / "tienda_dwh.db"
SQL_DIR = ROOT / "sql"
CLEAN_DIR = ROOT / "data" / "clean"
DLQ_DIR = ROOT / "data" / "dlq"
DOCS_F3 = ROOT / "docs" / "Fase3"

# (entidad en la DLQ, vista de validacion, tabla de la capa limpia, expresion de "problema propio")
# "Problema propio" = el registro tiene una falla en SUS datos (no solo por culpa de su padre).
ENTIDADES = [
    ("cliente", "v3_cliente", "clean_cliente",
     "e01 + e02 + e03 + n01 + n02 + n03 > 0"),
    ("producto", "v3_producto", "clean_producto",
     "e04 + e05 + n04 + n05 + n06 > 0"),
    ("pedido", "v3_pedido", "clean_pedido",
     "e06 + e07 + e08 + e09 + n07 + n08 + n09 + n10 + n11 + i01 + i02 > 0"),
    ("linea_pedido", "v3_linea", "clean_linea_pedido",
     "lin_invalida = 1"),
    ("inventario", "v3_inventario", "clean_inventario",
     "e14 + e15 + e16 + e17 + e18 + n16 + n17 + n18 + i05 > 0"),
    ("movimiento_inventario", "v3_movimiento", "clean_movimiento_inventario",
     "e19 + e20 + e21 + n19 + n20 + n21 > 0"),
    ("evento_comportamiento", "v3_evento", "clean_evento_comportamiento",
     "e22 + e23 + e24 + e25 + e26 + n22 + n23 + n24 + n25 + i07 + i08 > 0"),
]


def ahora_id():
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def conectar(foreign_keys=True):
    """Abre el almacen con los casts seguros registrados. Termina con mensaje claro si falta algo."""
    if not WAREHOUSE.exists():
        sys.exit("ERROR: no existe el almacen. Ejecuta primero f2_load_raw.py y f2_transform.py")
    conn = sqlite3.connect(WAREHOUSE)
    if foreign_keys:
        conn.execute("PRAGMA foreign_keys = ON")
    for nombre, funcion in (("to_text", to_text), ("to_int", to_int), ("to_real", to_real),
                            ("to_date", to_date), ("to_ts", to_ts)):
        conn.create_function(nombre, 1, funcion, deterministic=True)
    return conn


def existe_tabla(conn, nombre):
    fila = conn.execute("SELECT 1 FROM sqlite_master WHERE name = ? AND type IN ('table', 'view')",
                        (nombre,)).fetchone()
    return fila is not None


def exigir_tablas(conn, nombres, paso_previo):
    faltan = [n for n in nombres if not existe_tabla(conn, n)]
    if faltan:
        sys.exit(f"ERROR: faltan las tablas {', '.join(faltan)}. Ejecuta antes: {paso_previo}")
