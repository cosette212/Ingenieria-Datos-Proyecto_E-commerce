"""
Ingesta BATCH (por lotes) - paradigma ELT.

Lee los archivos de data/source (CSV y JSON) y los ATERRIZA TAL CUAL en data/raw,
sin limpiar nada (la transformacion se hace despues, ya dentro del almacen: ELT).
Cada corrida crea una carpeta nueva: data/raw/batch_id=AAAAMMDD_HHMMSS/
con una copia de cada archivo y un manifest.json (conteos, huella SHA-256, estado).

Uso:
    python src/ingest_batch.py
"""
import hashlib
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "data" / "source"
RAW = ROOT / "data" / "raw"
LOGS = ROOT / "logs"

ARCHIVOS_CSV = ["clientes.csv", "productos.csv", "pedidos.csv"]
ARCHIVOS_JSON = ["lineas_pedido.json", "inventario.json"]


def configurar_log():
    LOGS.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(LOGS / "ingest_batch.log", encoding="utf-8"),
                  logging.StreamHandler(sys.stdout)],
    )


def sha256(ruta):
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        for bloque in iter(lambda: f.read(8192), b""):
            h.update(bloque)
    return h.hexdigest()


def ingestar_csv(nombre, destino, batch_id, ts):
    # dtype=str: TODO se lee como texto, no se "adivinan" tipos en la zona cruda.
    # keep_default_na=False: los vacios se quedan como "" (no se convierten en NaN).
    df = pd.read_csv(SOURCE / nombre, dtype=str, keep_default_na=False)
    df["_batch_id"] = batch_id
    df["_ingest_ts"] = ts
    df["_source_file"] = nombre
    df.to_csv(destino / nombre, index=False, encoding="utf-8")
    return len(df)


def ingestar_json(nombre, destino, batch_id, ts):
    with open(SOURCE / nombre, encoding="utf-8") as f:
        datos = json.load(f)                     # si el JSON esta roto, falla aqui
    envoltura = {"_batch_id": batch_id, "_ingest_ts": ts, "_source_file": nombre,
                 "registros": datos}             # se conserva la estructura jerarquica
    with open(destino / nombre, "w", encoding="utf-8") as f:
        json.dump(envoltura, f, ensure_ascii=False)
    return len(datos)


def main():
    configurar_log()
    inicio = time.time()
    ahora = datetime.now()
    batch_id = ahora.strftime("%Y%m%d_%H%M%S")
    ts = ahora.isoformat(timespec="seconds")
    destino = RAW / f"batch_id={batch_id}"
    destino.mkdir(parents=True, exist_ok=True)

    logging.info("=== INICIA ingesta batch %s ===", batch_id)
    manifiesto = {"batch_id": batch_id, "inicio": ts, "archivos": []}
    fallos = 0

    for nombre in ARCHIVOS_CSV + ARCHIVOS_JSON:
        entrada = {"archivo": nombre}
        try:
            if nombre.endswith(".csv"):
                filas = ingestar_csv(nombre, destino, batch_id, ts)
            else:
                filas = ingestar_json(nombre, destino, batch_id, ts)
            entrada.update(estado="OK", registros=filas, sha256=sha256(SOURCE / nombre))
            logging.info("OK   %-20s %6d registros", nombre, filas)
        except Exception as e:                   # un archivo malo NO detiene a los demas
            fallos += 1
            entrada.update(estado="ERROR", error=str(e))
            logging.error("FALLO %-20s %s", nombre, e)
        manifiesto["archivos"].append(entrada)

    manifiesto["duracion_segundos"] = round(time.time() - inicio, 2)
    manifiesto["archivos_con_error"] = fallos
    with open(destino / "manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifiesto, f, ensure_ascii=False, indent=2)

    logging.info("=== TERMINA en %.2f s | errores: %d | carpeta: %s ===",
                 manifiesto["duracion_segundos"], fallos, destino)
    return 1 if fallos else 0                    # codigo 1 = fallo (lo usara el orquestador)


if __name__ == "__main__":
    sys.exit(main())
