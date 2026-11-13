"""
Genera los archivos FUENTE con "caos controlado" para el proyecto E-Commerce.

Crea en data/source/:
  - clientes.csv          (plano)
  - productos.csv         (plano, id_producto NUMERICO)
  - pedidos.csv           (plano)
  - lineas_pedido.json    (jerarquico: cada pedido trae su lista de lineas, producto_id como TEXTO)
  - inventario.json       (jerarquico: producto -> almacenes -> movimientos, producto_id como TEXTO)

Caos inyectado: nulos, outliers, edades negativas, tipos mezclados, fechas invalidas,
duplicados, pedidos sin lineas y el "Join roto" (producto_id texto vs numerico).
"""
import csv
import json
import random
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)  # misma semilla = mismos datos cada vez (reproducible)

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "source"
OUT.mkdir(parents=True, exist_ok=True)

N_CLIENTES = 500
N_PRODUCTOS = 100
N_PEDIDOS = 2000
PRIMER_PRODUCTO = 100          # los productos van de 100 a 199
CHAOS_RATE = 0.08              # 8% de registros corruptos (el enunciado pide minimo 5%)
BASE = datetime(2026, 11, 10, 23, 0, 0)

caos = Counter()               # cuenta cuanto caos se inyecto


def hay_caos():
    return random.random() < CHAOS_RATE


def fecha_aleatoria(dias_atras):
    return BASE - timedelta(days=random.randint(0, dias_atras),
                            minutes=random.randint(0, 1439))


# ---------------------------------------------------------------- CLIENTES
def generar_clientes():
    nombres = ["Ana", "Luis", "Maria", "Jose", "Sofia", "Diego", "Valeria", "Carlos",
               "Fernanda", "Jorge", "Camila", "Pedro", "Lucia", "Miguel", "Paola"]
    apellidos = ["Garcia", "Lopez", "Martinez", "Hernandez", "Perez", "Sanchez", "Ramirez"]
    canales = ["organico", "pago", "referido"]
    filas = []
    for i in range(1, N_CLIENTES + 1):
        nombre = f"{random.choice(nombres)} {random.choice(apellidos)}"
        fila = {
            "id_cliente": i,
            "nombre": nombre,
            "correo": f"{nombre.split()[0].lower()}{i}@correo.com",
            "fecha_registro": fecha_aleatoria(700).strftime("%Y-%m-%d"),
            "edad": int(min(70, max(18, random.gauss(32, 9)))),   # sesgada hacia la juventud
            "tipo_cliente": random.choice(["registrado", "registrado", "invitado"]),
            "canal_adquisicion": random.choice(canales),
        }
        if hay_caos():
            tipo = random.choice(["edad_negativa", "edad_texto", "edad_outlier", "edad_nula",
                                  "correo_nulo", "correo_invalido", "fecha_invalida", "duplicado"])
            caos["clientes:" + tipo] += 1
            if tipo == "edad_negativa":
                fila["edad"] = random.randint(-10, -1)
            elif tipo == "edad_texto":
                fila["edad"] = "veinte"
            elif tipo == "edad_outlier":
                fila["edad"] = random.randint(150, 999)
            elif tipo == "edad_nula":
                fila["edad"] = ""
            elif tipo == "correo_nulo":
                fila["correo"] = ""
            elif tipo == "correo_invalido":
                fila["correo"] = "correo_sin_arroba.com"
            elif tipo == "fecha_invalida":
                fila["fecha_registro"] = "2026-13-45"
            elif tipo == "duplicado" and filas:
                filas.append(dict(filas[-1]))   # repite el registro anterior
        filas.append(fila)
    return filas


# --------------------------------------------------------------- PRODUCTOS
def generar_productos():
    catalogo = {
        "Electronica": ["Audio", "Computo", "Celulares"],
        "Hogar": ["Cocina", "Decoracion", "Limpieza"],
        "Ropa": ["Hombre", "Mujer", "Infantil"],
        "Deportes": ["Fitness", "Ciclismo", "Outdoor"],
    }
    marcas = ["Aurora", "Nova", "Zenit", "Kappa", "Orbita"]
    filas = []
    for i in range(N_PRODUCTOS):
        categoria = random.choice(list(catalogo))
        fila = {
            "id_producto": PRIMER_PRODUCTO + i,          # NUMERICO (100, 101, ...)
            "nombre": f"Producto {PRIMER_PRODUCTO + i}",
            "categoria": categoria,
            "subcategoria": random.choice(catalogo[categoria]),
            "precio_lista": round(random.lognormvariate(5.2, 0.7), 2),  # sesgada a la derecha
            "marca": random.choice(marcas),
            "estado": random.choice(["activo", "activo", "activo", "descontinuado"]),
        }
        if hay_caos():
            tipo = random.choice(["precio_nulo", "precio_negativo", "precio_outlier",
                                  "precio_texto", "estado_invalido", "categoria_nula"])
            caos["productos:" + tipo] += 1
            if tipo == "precio_nulo":
                fila["precio_lista"] = ""
            elif tipo == "precio_negativo":
                fila["precio_lista"] = -abs(fila["precio_lista"])
            elif tipo == "precio_outlier":
                fila["precio_lista"] = 9999999
            elif tipo == "precio_texto":
                fila["precio_lista"] = "gratis"
            elif tipo == "estado_invalido":
                fila["estado"] = "???"
            elif tipo == "categoria_nula":
                fila["categoria"] = ""
        filas.append(fila)
    return filas


# ------------------------------------------------- PEDIDOS + LINEAS (JSON)
def generar_pedidos(productos):
    precios = {}
    for p in productos:
        try:
            precios[p["id_producto"]] = float(p["precio_lista"])
        except (TypeError, ValueError):
            precios[p["id_producto"]] = 100.0
    pedidos, lineas_json = [], []
    for pid in range(1, N_PEDIDOS + 1):
        lineas, total = [], 0.0
        for n in range(1, random.randint(1, 4) + 1):
            prod = random.randint(PRIMER_PRODUCTO, PRIMER_PRODUCTO + N_PRODUCTOS - 1)
            cantidad = random.randint(1, 5)
            precio = abs(precios[prod]) if precios[prod] < 100000 else 100.0
            descuento = random.choice([0, 0, 0, 0.05, 0.1, 0.2])
            total += cantidad * precio * (1 - descuento)
            lineas.append({
                "numero_linea": n,
                "producto_id": str(prod),          # TEXTO "100"  <-- Join roto
                "cantidad": cantidad,
                "precio_unitario": round(precio, 2),
                "descuento": descuento,
            })
        pedido = {
            "id_pedido": pid,
            "id_cliente": random.randint(1, N_CLIENTES),
            "fecha_hora": fecha_aleatoria(90).strftime("%Y-%m-%d %H:%M:%S"),
            "monto_total": round(total, 2),
            "estado": random.choice(["pendiente", "pagado", "pagado", "enviado", "cancelado"]),
            "metodo_pago": random.choice(["tarjeta", "transferencia", "efectivo", "paypal"]),
            "canal_venta": random.choice(["web", "app movil", "tienda fisica"]),
        }
        if hay_caos():
            tipo = random.choice(["monto_negativo", "monto_nulo", "monto_texto", "fecha_invalida",
                                  "cliente_inexistente", "estado_invalido", "sin_lineas",
                                  "cantidad_cero", "producto_inexistente", "precio_linea_nulo",
                                  "producto_con_espacios"])
            caos["pedidos:" + tipo] += 1
            if tipo == "monto_negativo":
                pedido["monto_total"] = -abs(pedido["monto_total"])
            elif tipo == "monto_nulo":
                pedido["monto_total"] = ""
            elif tipo == "monto_texto":
                pedido["monto_total"] = "mil pesos"
            elif tipo == "fecha_invalida":
                pedido["fecha_hora"] = "31/02/2026 25:61"
            elif tipo == "cliente_inexistente":
                pedido["id_cliente"] = 99999
            elif tipo == "estado_invalido":
                pedido["estado"] = "devuelto??"
            elif tipo == "sin_lineas":
                lineas = []                                  # viola la regla R2
            elif tipo == "cantidad_cero":
                lineas[0]["cantidad"] = random.choice([0, -3])
            elif tipo == "producto_inexistente":
                lineas[0]["producto_id"] = "999"
            elif tipo == "precio_linea_nulo":
                lineas[0]["precio_unitario"] = None
            elif tipo == "producto_con_espacios":
                lineas[0]["producto_id"] = " " + lineas[0]["producto_id"] + " "
        pedidos.append(pedido)
        lineas_json.append({"id_pedido": pid, "lineas": lineas})
    return pedidos, lineas_json


# --------------------------------------------------------- INVENTARIO (JSON)
def generar_inventario():
    almacenes = ["GDL", "CDMX", "MTY"]
    motivos = {"entrada": "reposicion", "salida": "venta", "ajuste": "merma"}
    datos = []
    mov_id = 1
    for i in range(N_PRODUCTOS):
        prod = PRIMER_PRODUCTO + i
        regs = []
        for alm in random.sample(almacenes, random.randint(1, 2)):
            movs = []
            for _ in range(random.randint(1, 4)):
                tipo = random.choice(["entrada", "salida", "ajuste"])
                movs.append({
                    "id_movimiento": f"M-{mov_id}",
                    "tipo": tipo,
                    "cantidad": random.randint(1, 80),
                    "fecha_hora": fecha_aleatoria(60).strftime("%Y-%m-%dT%H:%M:%S"),
                    "motivo": motivos[tipo],
                })
                mov_id += 1
            reg = {
                "almacen": alm,
                "stock_disponible": random.randint(0, 300),
                "stock_reservado": random.randint(0, 20),
                "ultima_actualizacion": fecha_aleatoria(10).strftime("%Y-%m-%dT%H:%M:%S"),
                "movimientos": movs,
            }
            if hay_caos():
                tipo = random.choice(["stock_negativo", "stock_texto", "movimiento_tipo_invalido",
                                      "movimiento_cantidad_nula"])
                caos["inventario:" + tipo] += 1
                if tipo == "stock_negativo":
                    reg["stock_disponible"] = -random.randint(1, 50)
                elif tipo == "stock_texto":
                    reg["stock_disponible"] = "N/D"
                elif tipo == "movimiento_tipo_invalido":
                    reg["movimientos"][0]["tipo"] = "teletransporte"
                elif tipo == "movimiento_cantidad_nula":
                    reg["movimientos"][0]["cantidad"] = None
            regs.append(reg)
        datos.append({"producto_id": str(prod), "almacenes": regs})   # TEXTO "100"
    return datos


def escribir_csv(nombre, filas):
    with open(OUT / nombre, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(filas[0].keys()))
        w.writeheader()
        w.writerows(filas)


def escribir_json(nombre, datos):
    with open(OUT / nombre, "w", encoding="utf-8") as f:
        json.dump(datos, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    clientes = generar_clientes()
    productos = generar_productos()
    pedidos, lineas = generar_pedidos(productos)
    inventario = generar_inventario()

    escribir_csv("clientes.csv", clientes)
    escribir_csv("productos.csv", productos)
    escribir_csv("pedidos.csv", pedidos)
    escribir_json("lineas_pedido.json", lineas)
    escribir_json("inventario.json", inventario)

    print("Archivos generados en", OUT)
    print(f"\n{'Archivo':<22}{'Registros':>10}")
    print(f"{'clientes.csv':<22}{len(clientes):>10}")
    print(f"{'productos.csv':<22}{len(productos):>10}")
    print(f"{'pedidos.csv':<22}{len(pedidos):>10}")
    print(f"{'lineas_pedido.json':<22}{len(lineas):>10}")
    print(f"{'inventario.json':<22}{len(inventario):>10}")
    print("\nCaos inyectado (por tipo):")
    for k, v in sorted(caos.items()):
        print(f"  {k:<40}{v:>4}")
    total_caos = sum(caos.values())
    total_reg = len(clientes) + len(productos) + len(pedidos) + len(inventario)
    print(f"\nTotal de registros con caos: {total_caos} de {total_reg} "
          f"({100 * total_caos / total_reg:.1f}%)")
