"""
Demuestra el "Join roto": producto_id llega como TEXTO ("100") en el JSON y
id_producto es NUMERICO (100) en productos.csv. Se prueba el join antes y despues de traducir tipos.
"""
import json
from pathlib import Path

import pandas as pd

SOURCE = Path(__file__).resolve().parent.parent / "data" / "source"

productos = pd.read_csv(SOURCE / "productos.csv")           # pandas infiere id_producto = entero
with open(SOURCE / "lineas_pedido.json", encoding="utf-8") as f:
    lineas = pd.json_normalize(json.load(f), record_path="lineas", meta=["id_pedido"])

print("Tipo de productos.id_producto :", productos["id_producto"].dtype)
print("Tipo de lineas.producto_id    :", lineas["producto_id"].dtype)

print("\n--- INTENTO 1: join directo (sin traducir tipos) ---")
try:
    r = lineas.merge(productos, left_on="producto_id", right_on="id_producto", how="inner")
    print("Filas que hicieron match:", len(r))
except Exception as e:
    print("EL JOIN SE ROMPIO ->", type(e).__name__)
    print(str(e)[:150])

print("\n--- INTENTO 2: traduciendo el tipo (texto -> entero) ---")
lineas["producto_id_int"] = pd.to_numeric(lineas["producto_id"].astype(str).str.strip(),
                                          errors="coerce").astype("Int64")
r = lineas.merge(productos, left_on="producto_id_int", right_on="id_producto", how="left")
con_match = r["id_producto"].notna().sum()
print(f"Lineas totales: {len(lineas)} | con producto: {con_match} | "
      f"sin producto (iran a la DLQ en la Fase 3): {len(lineas) - con_match}")
