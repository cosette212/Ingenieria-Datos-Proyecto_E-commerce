"""
FASE 3 - VERIFICADOR DE LA CADENA COMPLETA (Fase 1 -> Fase 2 -> Fase 3).

Comprueba con numeros que cada fase usa EXACTAMENTE lo que produjo la anterior:
  Fase 1: archivos fuente == manifiesto del batch == tablas crudas
  Fase 2: contabilidad cruda -> staging -> modelo 3FN, integridad referencial, Parquet de la Fase 2
  Fase 3: lo evaluado == lo crudo, aceptados + rechazados == evaluados, lo rechazado en la Fase 2
          tampoco esta en la capa limpia, la capa limpia es un subconjunto de la 3FN, sin nulos
          en lo imputado y Parquet de la Fase 3 igual al almacen.

Uso (con el almacen en estado normal, sin datos de la prueba de caos):
    python src/f3_verificar_cadena.py
Termina con codigo 1 si alguna comprobacion falla.
"""
import csv
import hashlib
import json
import sys

from f3_comun import CLEAN_DIR, ENTIDADES, ROOT, conectar, existe_tabla

SOURCE = ROOT / "data" / "source"
RAW = ROOT / "data" / "raw"
CURATED = ROOT / "data" / "curated"

resultados = []


def chk(nombre, ok, detalle=""):
    resultados.append(ok)
    print(f"  [{'PASA ' if ok else 'FALLA'}] {nombre}" + (f"  ->  {detalle}" if detalle else ""))


def aviso(texto):
    print(f"  [AVISO] {texto}")


def uno(conn, sql, params=()):
    return conn.execute(sql, params).fetchone()[0]


def contar_csv(ruta):
    with open(ruta, newline="", encoding="utf-8") as f:
        return sum(1 for _ in csv.DictReader(f))


def contar_json(ruta):
    with open(ruta, encoding="utf-8") as f:
        return len(json.load(f))


def sha256(ruta):
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        for bloque in iter(lambda: f.read(8192), b""):
            h.update(bloque)
    return h.hexdigest()


def contar_parquet(ruta):
    import pandas as pd
    return len(pd.read_parquet(ruta))


def hay_pyarrow():
    try:
        import pandas  # noqa: F401
        import pyarrow  # noqa: F401
        return True
    except ImportError:
        return False


def fase1(conn):
    print("\n=== FASE 1: fuente -> ingesta -> zona cruda ===")
    lote = conn.execute("SELECT batch_id FROM meta_carga").fetchone()[0] if existe_tabla(conn, "meta_carga") else None
    chk("El almacen tiene registrado el batch que cargo f2_load_raw.py", lote is not None, f"batch {lote}")
    if lote is None:
        return None
    carpeta = RAW / f"batch_id={lote}"
    chk("La carpeta de ese batch existe en data/raw", carpeta.is_dir(), carpeta.name)
    if not carpeta.is_dir():
        return None
    if lote == "chaos_test":
        aviso("El almacen tiene cargado el batch de la prueba de caos. Se omite la comparacion con data/source. "
              "Restaura con f3_chaos_test.py (sin --mantener) o recarga el batch normal.")
        return carpeta
    manifiesto = json.loads((carpeta / "manifest.json").read_text(encoding="utf-8"))
    chk("El manifiesto del batch no reporta errores", manifiesto["archivos_con_error"] == 0,
        f"archivos con error: {manifiesto['archivos_con_error']}")
    archivos = {a["archivo"]: a for a in manifiesto["archivos"]}
    for nombre in ("clientes.csv", "productos.csv", "pedidos.csv", "lineas_pedido.json", "inventario.json"):
        ruta = SOURCE / nombre
        n = contar_csv(ruta) if nombre.endswith(".csv") else contar_json(ruta)
        chk(f"{nombre}: filas en data/source == registros del manifiesto",
            archivos[nombre]["registros"] == n, f"{n} contra {archivos[nombre]['registros']}")
        chk(f"{nombre}: huella SHA-256 igual (la fuente no cambio despues de ingerir)",
            archivos[nombre]["sha256"] == sha256(ruta))
    return carpeta


def fase1_stream(conn):
    archivos = sorted((RAW / "eventos_stream").glob("*.jsonl")) if (RAW / "eventos_stream").is_dir() else []
    lineas = 0
    for a in archivos:
        with open(a, encoding="utf-8") as f:
            lineas += sum(1 for linea in f if linea.strip())
    chk("Eventos de data/raw/eventos_stream == filas de raw_eventos", lineas == uno(conn, "SELECT COUNT(*) FROM raw_eventos"),
        f"{lineas} contra {uno(conn, 'SELECT COUNT(*) FROM raw_eventos')}")
    dlq1 = ROOT / "data" / "dlq"
    existen = [n for n in ("eventos_rezagados.jsonl", "eventos_corruptos.jsonl") if (dlq1 / n).exists()]
    if existen:
        chk("La DLQ del stream (Fase 1) existe en data/dlq", True, ", ".join(existen))
    else:
        aviso("No hay DLQ del stream en data/dlq (no hubo eventos tardios ni corruptos en tu corrida)")


def fase2(conn, carpeta):
    print("\n=== FASE 2: crudo -> staging -> modelo 3FN ===")
    if carpeta is not None and (carpeta / "manifest.json").exists() and carpeta.name != "batch_id=chaos_test":
        manifiesto = json.loads((carpeta / "manifest.json").read_text(encoding="utf-8"))
        archivos = {a["archivo"]: a["registros"] for a in manifiesto["archivos"]}
        for archivo, tabla in (("clientes.csv", "raw_clientes"), ("productos.csv", "raw_productos"),
                               ("pedidos.csv", "raw_pedidos"), ("lineas_pedido.json", "raw_lineas_pedido"),
                               ("inventario.json", "raw_inventario")):
            n = uno(conn, f"SELECT COUNT(*) FROM {tabla}")
            chk(f"{tabla} == registros del manifiesto de la Fase 1", n == archivos[archivo],
                f"{n} contra {archivos[archivo]}")
    rech = lambda t: uno(conn, "SELECT COUNT(*) FROM rechazado_integridad WHERE tabla_destino = ?", (t,))
    pares = [
        ("cliente", "SELECT COUNT(DISTINCT id_cliente) FROM stg_clientes WHERE id_cliente IS NOT NULL",
         "SELECT COUNT(*) FROM cliente", 0),
        ("producto", "SELECT COUNT(DISTINCT id_producto) FROM stg_productos WHERE id_producto IS NOT NULL",
         "SELECT COUNT(*) FROM producto", 0),
        ("pedido", "SELECT COUNT(DISTINCT id_pedido) FROM stg_pedidos WHERE id_pedido IS NOT NULL",
         "SELECT COUNT(*) FROM pedido", rech("pedido")),
        ("linea_pedido", "SELECT COUNT(*) FROM stg_lineas_pedido", "SELECT COUNT(*) FROM linea_pedido",
         rech("linea_pedido")),
        ("evento_comportamiento", "SELECT COUNT(*) FROM stg_eventos",
         "SELECT COUNT(*) FROM evento_comportamiento", rech("evento_comportamiento")),
    ]
    for nombre, sql_stg, sql_final, rechazados in pares:
        stg, final = uno(conn, sql_stg), uno(conn, sql_final)
        chk(f"{nombre}: staging == modelo 3FN + rechazados de integridad",
            stg == final + rechazados, f"{stg} = {final} + {rechazados}")
    huerfanos = len(conn.execute("PRAGMA foreign_key_check").fetchall())
    chk("PRAGMA foreign_key_check sin referencias rotas", huerfanos == 0, f"referencias rotas: {huerfanos}")
    if hay_pyarrow() and CURATED.is_dir():
        for tabla in ("cliente", "producto", "pedido", "linea_pedido"):
            ruta = CURATED / f"{tabla}.parquet"
            if ruta.exists():
                n_p, n_t = contar_parquet(ruta), uno(conn, f"SELECT COUNT(*) FROM {tabla}")
                chk(f"Parquet de la Fase 2 ({tabla}) == tabla del almacen", n_p == n_t,
                    f"{n_p} contra {n_t}" + ("" if n_p == n_t else " -> ejecuta f2_export_parquet.py"))
    else:
        aviso("Parquet de la Fase 2 omitido (falta pyarrow o data/curated)")


def fase3(conn):
    print("\n=== FASE 3: lo que llega, lo que sale y como se conecta con la Fase 2 ===")
    raw = {"cliente": "raw_clientes", "producto": "raw_productos", "pedido": "raw_pedidos",
           "evento_comportamiento": "raw_eventos"}
    stg = {"linea_pedido": "stg_lineas_pedido", "inventario": "stg_inventario",
           "movimiento_inventario": "stg_movimientos"}
    for entidad, vista, clean, _ in ENTIDADES:
        evaluados = uno(conn, f"SELECT COUNT(*) FROM {vista}")
        origen = raw.get(entidad) or stg.get(entidad)
        n_origen = uno(conn, f"SELECT COUNT(*) FROM {origen}")
        etiqueta = "cruda" if entidad in raw else "de staging (mismo desanidado)"
        chk(f"{entidad}: evaluados por la Fase 3 == filas de la tabla {etiqueta} {origen}",
            evaluados == n_origen, f"{evaluados} contra {n_origen}")
        aceptados = uno(conn, f"SELECT COUNT(*) FROM {clean}")
        rechazados = uno(conn, f"SELECT COUNT(*) FROM {vista} WHERE rechazar = 1")
        chk(f"{entidad}: evaluados == aceptados + rechazados", evaluados == aceptados + rechazados,
            f"{evaluados} = {aceptados} + {rechazados}")
        if rechazados:
            en_dlq = uno(conn, "SELECT COUNT(*) FROM dlq WHERE entidad = ? AND accion = 'RECHAZADO'", (entidad,))
            chk(f"{entidad}: cada rechazo tiene al menos una fila en la DLQ", en_dlq >= rechazados,
                f"{en_dlq} filas de DLQ para {rechazados} rechazos")

    print()
    subconjuntos = [
        ("clean_cliente", "id_cliente", "cliente", "id_cliente"),
        ("clean_producto", "id_producto", "producto", "id_producto"),
        ("clean_pedido", "id_pedido", "pedido", "id_pedido"),
        ("clean_evento_comportamiento", "id_evento", "evento_comportamiento", "id_evento"),
    ]
    for c_tabla, c_col, f_tabla, f_col in subconjuntos:
        fuera = uno(conn, f"SELECT COUNT(*) FROM {c_tabla} WHERE {c_col} NOT IN (SELECT {f_col} FROM {f_tabla})")
        chk(f"{c_tabla} es subconjunto de {f_tabla} (Fase 2)", fuera == 0, f"fuera del modelo 3FN: {fuera}")
    fuera = uno(conn, "SELECT COUNT(*) FROM clean_linea_pedido c WHERE NOT EXISTS (SELECT 1 FROM linea_pedido l "
                      "WHERE l.id_pedido = c.id_pedido AND l.numero_linea = c.numero_linea)")
    chk("clean_linea_pedido es subconjunto de linea_pedido (Fase 2)", fuera == 0, f"fuera del modelo 3FN: {fuera}")
    fuera = uno(conn, "SELECT COUNT(*) FROM clean_inventario c WHERE NOT EXISTS (SELECT 1 FROM inventario i "
                      "WHERE i.id_producto = c.id_producto AND i.almacen = c.almacen)")
    chk("clean_inventario es subconjunto de inventario (Fase 2)", fuera == 0, f"fuera del modelo 3FN: {fuera}")
    fuera = uno(conn, "SELECT COUNT(*) FROM clean_movimiento_inventario WHERE id_movimiento NOT IN "
                      "(SELECT id_movimiento FROM movimiento_inventario)")
    chk("clean_movimiento_inventario es subconjunto de movimiento_inventario (Fase 2)", fuera == 0,
        f"fuera del modelo 3FN: {fuera}")

    print()
    for tabla, pk, col in (("pedido", "id_pedido", None), ("linea_pedido", None, None)):
        pass
    r2_ped = uno(conn, "SELECT COUNT(*) FROM rechazado_integridad r WHERE tabla_destino = 'pedido' AND "
                       "CAST(clave AS INTEGER) IN (SELECT id_pedido FROM clean_pedido)")
    chk("Los pedidos que la Fase 2 rechazo por integridad tampoco estan en la capa limpia", r2_ped == 0,
        f"presentes en la capa limpia: {r2_ped}")
    r2_lin = uno(conn, "SELECT COUNT(*) FROM rechazado_integridad r WHERE tabla_destino = 'linea_pedido' AND "
                       "EXISTS (SELECT 1 FROM clean_linea_pedido c WHERE c.id_pedido || '-' || c.numero_linea = r.clave)")
    chk("Las lineas que la Fase 2 rechazo por integridad tampoco estan en la capa limpia", r2_lin == 0,
        f"presentes en la capa limpia: {r2_lin}")
    r2_ev = uno(conn, "SELECT COUNT(*) FROM rechazado_integridad r WHERE tabla_destino = 'evento_comportamiento' "
                      "AND clave IN (SELECT id_evento FROM clean_evento_comportamiento)")
    chk("Los eventos que la Fase 2 rechazo por integridad tampoco estan en la capa limpia", r2_ev == 0,
        f"presentes en la capa limpia: {r2_ev}")

    print()
    huerfanos = [f for f in conn.execute("PRAGMA foreign_key_check").fetchall() if str(f[0]).startswith("clean_")]
    chk("Capa limpia: PRAGMA foreign_key_check sin referencias rotas", len(huerfanos) == 0,
        f"referencias rotas: {len(huerfanos)}")
    sub = uno(conn, "SELECT COUNT(*) FROM clean_producto WHERE id_subcategoria IS NOT NULL AND id_subcategoria "
                    "NOT IN (SELECT id_subcategoria FROM subcategoria)")
    chk("clean_producto.id_subcategoria apunta a una subcategoria de la Fase 2", sub == 0,
        f"subcategorias inexistentes: {sub}")
    for tabla, col, flag in (("clean_cliente", "edad", "edad_imputada"),
                             ("clean_producto", "precio_lista", "precio_imputado")):
        nulos = uno(conn, f"SELECT COUNT(*) FROM {tabla} WHERE {col} IS NULL")
        marcados = uno(conn, f"SELECT COUNT(*) FROM {tabla} WHERE {flag} = 1")
        logs = uno(conn, "SELECT COUNT(*) FROM imputacion_log WHERE tabla = ? AND columna = ?", (tabla, col))
        chk(f"{tabla}.{col}: sin nulos despues de imputar", nulos == 0, f"nulos: {nulos}")
        chk(f"{tabla}.{col}: filas marcadas como imputadas == filas de imputacion_log",
            marcados == logs, f"{marcados} contra {logs}")
    if hay_pyarrow() and CLEAN_DIR.is_dir():
        for nombre, tabla in (("cliente", "clean_cliente"), ("pedido", "clean_pedido"),
                              ("linea_pedido", "clean_linea_pedido"), ("dlq", "dlq")):
            ruta = CLEAN_DIR / f"{nombre}.parquet"
            if ruta.exists():
                n_p, n_t = contar_parquet(ruta), uno(conn, f"SELECT COUNT(*) FROM {tabla}")
                chk(f"Parquet de la Fase 3 ({nombre}) == tabla del almacen", n_p == n_t,
                    f"{n_p} contra {n_t}" + ("" if n_p == n_t else " -> ejecuta f3_export_clean.py"))
            else:
                chk(f"Existe data/clean/{nombre}.parquet", False, "ejecuta f3_export_clean.py")
    else:
        aviso("Parquet de la Fase 3 omitido (falta pyarrow o data/clean)")


def main():
    conn = conectar(foreign_keys=False)
    necesarias = ["raw_clientes", "stg_clientes", "cliente", "rechazado_integridad", "dlq",
                  "clean_cliente", "imputacion_log"]
    faltan = [t for t in necesarias if not existe_tabla(conn, t)]
    if faltan:
        sys.exit("ERROR: faltan tablas (" + ", ".join(faltan) + "). Ejecuta el pipeline en orden: "
                 "f2_load_raw, f2_transform, f3_validate, f3_impute")
    carpeta = fase1(conn)
    fase1_stream(conn)
    fase2(conn, carpeta)
    fase3(conn)
    conn.close()
    print("\n=== RESUMEN ===")
    print(f"  Comprobaciones que pasan: {sum(resultados)} de {len(resultados)}")
    return 0 if all(resultados) else 1


if __name__ == "__main__":
    sys.exit(main())
