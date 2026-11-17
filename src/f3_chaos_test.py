"""
FASE 3 - PRUEBA DE CAOS (Chaos Test): inyecta registros corruptos EN VIVO y comprueba que el pipeline
no se detiene, manda los errores a la DLQ, imputa los faltantes y actualiza la capa Parquet.

Como funciona:
  0. Corre primero el pipeline normal para partir de un estado conocido (linea base).
  1. Copia el batch crudo mas reciente a data/raw/batch_id=chaos_test y le agrega registros malos
     (tipos erroneos, edades negativas, nulos, pedidos sin lineas, eventos incompletos...).
  2. Corre el pipeline REAL con esos datos:
        f2_load_raw -> f2_transform -> f3_validate -> f3_impute -> f3_export_clean
     y verifica que ningun paso falle (codigo de salida 0).
  3. Comprueba cada caso: ¿cayo en la DLQ con la regla esperada? ¿los nulos se imputaron?
     ¿los registros buenos de control pasaron limpios?
  4. Por defecto RESTAURA todo (borra los datos inyectados y vuelve a correr el pipeline normal).
     Con --mantener deja los datos del caos en el almacen y en Parquet (util para la demo).

Uso:
    python src/f3_chaos_test.py
    python src/f3_chaos_test.py --mantener
"""
import argparse
import csv
import json
import shutil
import subprocess
import sys
from datetime import datetime

from f3_comun import ROOT, SRC, conectar

RAW = ROOT / "data" / "raw"
CAOS_BATCH = RAW / "batch_id=chaos_test"
CAOS_EVENTOS = RAW / "eventos_stream" / "eventos_chaos_test.jsonl"

PASOS = ["f2_load_raw.py", "f2_transform.py", "f3_validate.py", "f3_impute.py", "f3_export_clean.py"]

# ----------------------------------------------------------------------------- casos
# (id legible, entidad, clave en la DLQ, regla esperada, accion esperada o None si NO debe ir a la DLQ)
CASOS = [
    # clientes
    ("Cliente con edad negativa (-5)",          "cliente", "9001", "N02", "CORREGIDO"),
    ("Cliente con edad 'abc' (tipo erroneo)",   "cliente", "9002", "E02", "CORREGIDO"),
    ("Cliente con edad vacia (nulo)",           "cliente", "9003", None,  None),
    ("Cliente con edad 250 (extrema)",          "cliente", "9004", "N02", "CORREGIDO"),
    ("Cliente con id 'XYZ' (no entero)",        "cliente", "XYZ",  "E01", "RECHAZADO"),
    ("Cliente con correo sin arroba",           "cliente", "9006", "N03", "CORREGIDO"),
    ("Cliente repetido (mismo id 9001)",        "cliente", "9001", "N01", "RECHAZADO"),
    ("Cliente menor de edad (17)",              "cliente", "9008", "N02", "CORREGIDO"),
    ("Cliente de control (valido)",             "cliente", "9010", None,  None),
    # productos
    ("Producto con precio 'gratis'",            "producto", "9501", "E05", "CORREGIDO"),
    ("Producto con precio negativo",            "producto", "9502", "N05", "CORREGIDO"),
    ("Producto con precio extremo (5,000,000)", "producto", "9503", None,  None),
    ("Producto con estado 'raro'",              "producto", "9504", "N06", "CORREGIDO"),
    # pedidos
    ("Pedido con monto negativo",               "pedido", "9101", "N08", "RECHAZADO"),
    ("Pedido sin lineas (R2)",                  "pedido", "9102", "N10", "RECHAZADO"),
    ("Pedido de un cliente inexistente (R1)",   "pedido", "9103", "I01", "RECHAZADO"),
    ("Pedido con monto 'mucho' (tipo erroneo)", "pedido", "9104", "E09", "RECHAZADO"),
    ("Pedido con una linea de cantidad 0",      "pedido", "9105", "I02", "RECHAZADO"),
    ("Linea con cantidad 0 (R5)",               "linea_pedido", "9105-1", "N12", "RECHAZADO"),
    ("Linea con producto 'abc'",                "linea_pedido", "9106-1", "E11", "RECHAZADO"),
    ("Pedido de control (valido)",              "pedido", "9107", None,  None),
    ("Pedido con monto distinto a la suma",     "pedido", "9108", "N11", "RECHAZADO"),
    ("Pedido con estado 'perdido'",             "pedido", "9109", "N09", "RECHAZADO"),
    ("Pedido con fecha 'ayer'",                 "pedido", "9110", "E08", "RECHAZADO"),
    # inventario y movimientos
    ("Inventario con stock negativo (R7)",      "inventario", "100|ZZZ", "N16", "RECHAZADO"),
    ("Inventario con stock 'N/D'",              "inventario", "101|QRO", "E16", "RECHAZADO"),
    ("Movimiento de tipo 'teletransporte'",     "movimiento_inventario", "M-CHAOS-1", "N20", "RECHAZADO"),
    ("Movimiento con cantidad nula",            "movimiento_inventario", "M-CHAOS-2", "E20", "RECHAZADO"),
    # eventos
    ("Evento 'vista' sin producto (R4)",        "evento_comportamiento", "CHAOS-V1", "N23", "RECHAZADO"),
    ("Evento sin cliente ni sesion (R3)",       "evento_comportamiento", "CHAOS-V2", "N24", "RECHAZADO"),
    ("Evento con tipo 'baile'",                 "evento_comportamiento", "CHAOS-V3", "N22", "RECHAZADO"),
    ("Evento con event_time 'ayer'",            "evento_comportamiento", "CHAOS-V4", "E26", "RECHAZADO"),
    ("Evento de un cliente inexistente",        "evento_comportamiento", "CHAOS-V5", "I07", "RECHAZADO"),
    ("Evento de control (valido)",              "evento_comportamiento", "CHAOS-V6", None,  None),
]

# Imputaciones que deben ocurrir: (tabla, clave, columna)
IMPUTADOS = [("clean_cliente", 9001, "edad"), ("clean_cliente", 9002, "edad"),
             ("clean_cliente", 9003, "edad"), ("clean_cliente", 9004, "edad"),
             ("clean_cliente", 9008, "edad"),
             ("clean_producto", 9501, "precio_lista"), ("clean_producto", 9502, "precio_lista"),
             ("clean_producto", 9503, "precio_lista")]
# Registros que SI deben quedar en la capa limpia
LIMPIOS = [("clean_cliente", "id_cliente", 9010), ("clean_cliente", "id_cliente", 9001),
           ("clean_producto", "id_producto", 9501), ("clean_pedido", "id_pedido", 9107),
           ("clean_evento_comportamiento", "id_evento", "CHAOS-V6")]
# Registros que NO deben quedar en la capa limpia
FUERA = [("clean_pedido", "id_pedido", 9101), ("clean_pedido", "id_pedido", 9102),
         ("clean_pedido", "id_pedido", 9105), ("clean_linea_pedido", "id_pedido", 9105),
         ("clean_evento_comportamiento", "id_evento", "CHAOS-V1")]


def limpiar_restos():
    """Borra lo que haya dejado una prueba anterior hecha con --mantener."""
    shutil.rmtree(CAOS_BATCH, ignore_errors=True)
    CAOS_EVENTOS.unlink(missing_ok=True)


def ultimo_batch_normal():
    carpetas = [c for c in sorted(RAW.glob("batch_id=*")) if c.name != CAOS_BATCH.name]
    if not carpetas:
        sys.exit("ERROR: no hay un batch crudo. Ejecuta primero f1_ingest_batch.py")
    return carpetas[-1]


def agregar_csv(ruta, filas):
    with open(ruta, newline="", encoding="utf-8") as f:
        columnas = csv.DictReader(f).fieldnames
    with open(ruta, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columnas)
        for fila in filas:
            base = {c: "" for c in columnas}
            base.update(fila)
            base.update({"_batch_id": "chaos_test", "_ingest_ts": datetime.now().isoformat(timespec="seconds"),
                         "_source_file": "chaos_test"})
            w.writerow(base)


def agregar_json(ruta, registros):
    with open(ruta, encoding="utf-8") as f:
        env = json.load(f)
    env["registros"].extend(registros)
    with open(ruta, "w", encoding="utf-8") as f:
        json.dump(env, f, ensure_ascii=False)


def cl(i, edad, correo=None, fecha="2026-01-15"):
    return {"id_cliente": str(i), "nombre": f"Cliente Caos {i}", "correo": correo or f"caos{i}@correo.com",
            "fecha_registro": fecha, "edad": str(edad), "tipo_cliente": "registrado",
            "canal_adquisicion": "organico"}


def pr(i, precio, estado="activo"):
    return {"id_producto": str(i), "nombre": f"Producto Caos {i}", "categoria": "Hogar",
            "subcategoria": "Cocina", "precio_lista": str(precio), "marca": "Nova", "estado": estado}


def pe(i, cliente, monto, estado="pagado", fecha="2026-11-05 10:00:00"):
    return {"id_pedido": str(i), "id_cliente": str(cliente), "fecha_hora": fecha,
            "monto_total": str(monto), "estado": estado, "metodo_pago": "tarjeta",
            "canal_venta": "web"}


def ln(n, prod, cant, precio, desc=0):
    return {"numero_linea": n, "producto_id": prod, "cantidad": cant, "precio_unitario": precio,
            "descuento": desc}


def inyectar():
    batch = ultimo_batch_normal()
    if CAOS_BATCH.exists():
        shutil.rmtree(CAOS_BATCH)
    shutil.copytree(batch, CAOS_BATCH)
    agregar_csv(CAOS_BATCH / "clientes.csv", [
        cl(9001, -5), cl(9002, "abc"), cl(9003, ""), cl(9004, 250), cl("XYZ", 30),
        cl(9006, 30, correo="sin_arroba"), cl(9001, -5), cl(9008, 17), cl(9010, 30)])
    agregar_csv(CAOS_BATCH / "productos.csv", [
        pr(9501, "gratis"), pr(9502, -10), pr(9503, 5000000), pr(9504, 100, estado="raro")])
    agregar_csv(CAOS_BATCH / "pedidos.csv", [
        pe(9101, 9010, -50), pe(9102, 9010, 100), pe(9103, 99999, 100), pe(9104, 9010, "mucho"),
        pe(9105, 9010, 100), pe(9106, 9010, 100), pe(9107, 9010, 400), pe(9108, 9010, 999),
        pe(9109, 9010, 400, estado="perdido"), pe(9110, 9010, 400, fecha="ayer")])
    agregar_json(CAOS_BATCH / "lineas_pedido.json", [
        {"id_pedido": 9101, "lineas": [ln(1, "100", 1, 50)]},
        {"id_pedido": 9102, "lineas": []},
        {"id_pedido": 9103, "lineas": [ln(1, "100", 1, 100)]},
        {"id_pedido": 9104, "lineas": [ln(1, "100", 1, 100)]},
        {"id_pedido": 9105, "lineas": [ln(1, "100", 0, 100)]},
        {"id_pedido": 9106, "lineas": [ln(1, "abc", 1, 100)]},
        {"id_pedido": 9107, "lineas": [ln(1, " 100 ", 2, 200)]},          # id con espacios: se traduce
        {"id_pedido": 9108, "lineas": [ln(1, "100", 2, 200)]},            # suma 400, el pedido dice 999
        {"id_pedido": 9109, "lineas": [ln(1, "100", 2, 200)]},
        {"id_pedido": 9110, "lineas": [ln(1, "100", 2, 200)]}])
    hoy = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    agregar_json(CAOS_BATCH / "inventario.json", [
        {"producto_id": "100", "almacenes": [
            {"almacen": "ZZZ", "stock_disponible": -5, "stock_reservado": 1, "ultima_actualizacion": hoy,
             "movimientos": []}]},
        {"producto_id": "101", "almacenes": [
            {"almacen": "QRO", "stock_disponible": "N/D", "stock_reservado": 1, "ultima_actualizacion": hoy,
             "movimientos": []}]},
        {"producto_id": "102", "almacenes": [
            {"almacen": "MEX", "stock_disponible": 10, "stock_reservado": 1, "ultima_actualizacion": hoy,
             "movimientos": [
                 {"id_movimiento": "M-CHAOS-1", "tipo": "teletransporte", "cantidad": 3,
                  "fecha_hora": hoy, "motivo": "caos"},
                 {"id_movimiento": "M-CHAOS-2", "tipo": "entrada", "cantidad": None,
                  "fecha_hora": hoy, "motivo": "caos"},
                 {"id_movimiento": "M-CHAOS-3", "tipo": "entrada", "cantidad": 5,
                  "fecha_hora": hoy, "motivo": "reposicion"}]}]}])
    t = datetime.now().isoformat(timespec="seconds")

    def ev(i, tipo="vista", cliente="9010", producto="100", sesion="S-caos", event_time=t):
        return json.dumps({"id_evento": i, "id_cliente": cliente, "producto_id": producto,
                           "tipo_evento": tipo, "event_time": event_time, "sesion": sesion,
                           "dispositivo": "móvil", "fuente_trafico": "directo"}, ensure_ascii=False)
    CAOS_EVENTOS.parent.mkdir(parents=True, exist_ok=True)
    CAOS_EVENTOS.write_text("\n".join([
        ev("CHAOS-V1", producto=None),
        ev("CHAOS-V2", cliente=None, sesion=None),
        ev("CHAOS-V3", tipo="baile"),
        ev("CHAOS-V4", event_time="ayer"),
        ev("CHAOS-V5", cliente="999999"),
        ev("CHAOS-V6")]) + "\n", encoding="utf-8")


def correr_pipeline(titulo):
    print(f"\n--- {titulo} ---")
    ok = True
    for paso in PASOS:
        r = subprocess.run([sys.executable, str(SRC / paso)], cwd=ROOT, capture_output=True, text=True)
        ok = ok and r.returncode == 0
        print(f"  {paso:<22}{'OK (codigo 0)' if r.returncode == 0 else 'FALLO (codigo ' + str(r.returncode) + ')'}")
        if r.returncode != 0:
            print((r.stdout + r.stderr)[-600:])
    return ok


def metricas():
    conn = conectar(foreign_keys=False)
    m = {"dlq": conn.execute("SELECT COUNT(*) FROM dlq").fetchone()[0],
         "imputaciones": conn.execute("SELECT COUNT(*) FROM imputacion_log").fetchone()[0]}
    for t in ("clean_cliente", "clean_producto", "clean_pedido", "clean_linea_pedido",
              "clean_inventario", "clean_movimiento_inventario", "clean_evento_comportamiento"):
        m[t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    conn.close()
    return m


def verificar():
    conn = conectar(foreign_keys=False)
    fallas = 0
    print(f"\n{'Caso inyectado':<44}{'Esperado':<22}{'Resultado'}")
    for nombre, entidad, clave, regla, accion in CASOS:
        filas = conn.execute("SELECT regla, accion FROM dlq WHERE entidad = ? AND clave = ?",
                             (entidad, clave)).fetchall()
        if regla is None:
            ok = len(filas) == 0
            esperado = "sin DLQ"
        else:
            ok = any(r.startswith(regla) and a == accion for r, a in filas)
            esperado = f"{regla} {accion}"
        fallas += 0 if ok else 1
        print(f"{nombre:<44}{esperado:<22}{'PASA' if ok else 'FALLA'}")

    print("\nImputaciones esperadas (el faltante se rellena y queda marcado):")
    for tabla, clave, col in IMPUTADOS:
        pk = "id_cliente" if tabla == "clean_cliente" else "id_producto"
        flag = "edad_imputada" if col == "edad" else "precio_imputado"
        fila = conn.execute(f"SELECT {col}, {flag} FROM {tabla} WHERE {pk} = ?", (clave,)).fetchone()
        ok = fila is not None and fila[0] is not None and fila[1] == 1
        fallas += 0 if ok else 1
        print(f"  {tabla}.{col} de {clave}: valor {fila[0] if fila else 'NO EXISTE'}  "
              f"{'PASA' if ok else 'FALLA'}")

    print("\nRegistros que deben quedar en la capa limpia:")
    for tabla, pk, valor in LIMPIOS:
        n = conn.execute(f"SELECT COUNT(*) FROM {tabla} WHERE {pk} = ?", (valor,)).fetchone()[0]
        fallas += 0 if n == 1 else 1
        print(f"  {tabla} {pk}={valor}: {'PASA' if n == 1 else 'FALLA'}")
    print("Registros que NO deben quedar en la capa limpia:")
    for tabla, pk, valor in FUERA:
        n = conn.execute(f"SELECT COUNT(*) FROM {tabla} WHERE {pk} = ?", (valor,)).fetchone()[0]
        fallas += 0 if n == 0 else 1
        print(f"  {tabla} {pk}={valor}: {'PASA' if n == 0 else 'FALLA'}")
    conn.close()
    return fallas


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mantener", action="store_true",
                   help="deja los datos del caos en el almacen y en Parquet (para la demo)")
    args = p.parse_args()

    limpiar_restos()
    if not correr_pipeline("Linea base: pipeline normal, sin datos inyectados"):
        sys.exit("ERROR: el pipeline normal ya falla antes de inyectar. Corrigelo antes de la prueba de caos.")
    print("\nPRUEBA DE CAOS | estado ANTES de inyectar")
    antes = metricas()
    print("  " + " | ".join(f"{k.replace('clean_', '')}={v}" for k, v in antes.items()))

    inyectar()
    n_casos = len(CASOS)
    print(f"\nSe inyectaron registros malos en {CAOS_BATCH.name} ({n_casos} casos a verificar).")
    pipeline_ok = correr_pipeline("Pipeline con datos corruptos (no debe detenerse)")
    despues = metricas()
    print("\nEstado DESPUES de inyectar:")
    print("  " + " | ".join(f"{k.replace('clean_', '')}={v}" for k, v in despues.items()))
    print(f"  Filas nuevas en la DLQ: {despues['dlq'] - antes['dlq']} | "
          f"imputaciones nuevas: {despues['imputaciones'] - antes['imputaciones']}")
    fallas = verificar()

    if args.mantener:
        print(f"\nSe MANTIENEN los datos del caos. Para volver a lo normal borra {CAOS_BATCH} y "
              f"{CAOS_EVENTOS} y ejecuta de nuevo el pipeline.")
    else:
        shutil.rmtree(CAOS_BATCH, ignore_errors=True)
        CAOS_EVENTOS.unlink(missing_ok=True)
        restaurado = correr_pipeline("Restaurando: pipeline normal sin datos inyectados")
        final = metricas()
        igual = final == antes
        print(f"\nEstado restaurado igual al de antes: {'SI' if igual else 'NO'}")
        pipeline_ok = pipeline_ok and restaurado and igual

    print("\n=== RESULTADO ===")
    print(f"  El pipeline no se detuvo ante datos corruptos: {'SI' if pipeline_ok else 'NO'}")
    print(f"  Casos con resultado distinto al esperado: {fallas}")
    return 0 if pipeline_ok and fallas == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
