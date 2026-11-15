"""
FASE 2 - Paso 1: LOAD (la "L" de ELT).

Carga los datos CRUDOS de data/raw en el almacen destino (SQLite) SIN transformarlos:
  - CSV  -> tablas raw_* con todas las columnas como TEXT (tal cual llegaron)
  - JSON -> una fila por registro, guardado como texto JSON (aun con sus listas anidadas)
  - Stream -> una fila por linea de evento

El almacen queda en data/warehouse/tienda_dwh.db. Cada corrida lo recarga completo
(es idempotente: correrlo dos veces da el mismo resultado).

Uso:
    python src/load_raw.py                      (usa el batch mas reciente)
    python src/load_raw.py --batch 20261111_212120
"""
import argparse
import csv
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"
WAREHOUSE = ROOT / "data" / "warehouse" / "tienda_dwh.db"

CSV_TABLAS = {
    "clientes.csv": "raw_clientes",
    "productos.csv": "raw_productos",
    "pedidos.csv": "raw_pedidos",
}
JSON_TABLAS = {
    "lineas_pedido.json": "raw_lineas_pedido",
    "inventario.json": "raw_inventario",
}


def elegir_batch(batch_id):
    if batch_id:
        carpeta = RAW / f"batch_id={batch_id}"
        if not carpeta.is_dir():
            sys.exit(f"ERROR: no existe {carpeta}")
        return carpeta
    carpetas = sorted(RAW.glob("batch_id=*"))
    if not carpetas:
        sys.exit("ERROR: no hay carpetas batch_id=... en data/raw. "
                 "Ejecuta primero: python src/ingest_batch.py")
    return carpetas[-1]


def cargar_csv(conn, ruta, tabla):
    with open(ruta, newline="", encoding="utf-8") as f:
        lector = csv.DictReader(f)
        columnas = lector.fieldnames
        conn.execute(f'DROP TABLE IF EXISTS "{tabla}"')
        defs = ", ".join(f'"{c}" TEXT' for c in columnas)
        conn.execute(f'CREATE TABLE "{tabla}" ({defs})')
        marcas = ", ".join("?" for _ in columnas)
        filas = ([fila[c] for c in columnas] for fila in lector)
        conn.executemany(f'INSERT INTO "{tabla}" VALUES ({marcas})', filas)
    return conn.execute(f'SELECT COUNT(*) FROM "{tabla}"').fetchone()[0]


def cargar_json(conn, ruta, tabla):
    with open(ruta, encoding="utf-8") as f:
        envoltura = json.load(f)
    conn.execute(f'DROP TABLE IF EXISTS "{tabla}"')
    conn.execute(f'CREATE TABLE "{tabla}" (registro_json TEXT, _batch_id TEXT, '
                 f'_ingest_ts TEXT, _source_file TEXT)')
    filas = ((json.dumps(r, ensure_ascii=False), envoltura["_batch_id"],
              envoltura["_ingest_ts"], envoltura["_source_file"])
             for r in envoltura["registros"])
    conn.executemany(f'INSERT INTO "{tabla}" VALUES (?, ?, ?, ?)', filas)
    return conn.execute(f'SELECT COUNT(*) FROM "{tabla}"').fetchone()[0]


def cargar_eventos(conn):
    conn.execute("DROP TABLE IF EXISTS raw_eventos")
    conn.execute("CREATE TABLE raw_eventos (linea_json TEXT, _source_file TEXT)")
    carpeta = RAW / "eventos_stream"
    archivos = sorted(carpeta.glob("*.jsonl")) if carpeta.is_dir() else []
    if not archivos:
        print("  AVISO: no hay eventos de stream en data/raw/eventos_stream "
              "(raw_eventos queda vacia)")
        return 0
    for archivo in archivos:
        with open(archivo, encoding="utf-8") as f:
            filas = [(linea.strip(), archivo.name) for linea in f if linea.strip()]
        conn.executemany("INSERT INTO raw_eventos VALUES (?, ?)", filas)
    return conn.execute("SELECT COUNT(*) FROM raw_eventos").fetchone()[0]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--batch", help="batch_id a cargar (por defecto, el mas reciente)")
    args = p.parse_args()

    carpeta = elegir_batch(args.batch)
    WAREHOUSE.parent.mkdir(parents=True, exist_ok=True)
    print(f"Cargando {carpeta.name} en {WAREHOUSE}")

    conn = sqlite3.connect(WAREHOUSE)
    try:
        for archivo, tabla in CSV_TABLAS.items():
            n = cargar_csv(conn, carpeta / archivo, tabla)
            print(f"  OK  {tabla:<20}{n:>7} filas")
        for archivo, tabla in JSON_TABLAS.items():
            n = cargar_json(conn, carpeta / archivo, tabla)
            print(f"  OK  {tabla:<20}{n:>7} registros JSON")
        n = cargar_eventos(conn)
        print(f"  OK  {'raw_eventos':<20}{n:>7} eventos")

        conn.execute("DROP TABLE IF EXISTS meta_carga")
        conn.execute("CREATE TABLE meta_carga (batch_id TEXT, cargado_en TEXT)")
        conn.execute("INSERT INTO meta_carga VALUES (?, ?)",
                     (carpeta.name.replace("batch_id=", ""),
                      datetime.now().isoformat(timespec="seconds")))
        conn.commit()
    finally:
        conn.close()
    print("LOAD terminado. Siguiente paso: python src/transform.py")


if __name__ == "__main__":
    main()
