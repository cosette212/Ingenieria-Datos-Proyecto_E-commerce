"""
FASE 2 - Paso 3: VALIDACION de la fase.

Demuestra con evidencia (para tu Blueprint y tu defensa):
  1. 1FN  : no quedan listas anidadas ni JSON dentro de ninguna celda.
  2. Join roto: ids texto vs entero, y cuantas lineas rescata la traduccion de tipos.
  3. Tipos : las llaves son enteros en todas las tablas finales.
  4. Integridad referencial: PRAGMA foreign_key_check sin errores.
  5. 3FN  : la dependencia subcategoria -> categoria se cumple y se separo en tablas propias.

Uso:
    python src/f2_validate_phase2.py
Termina con codigo 1 si alguna prueba falla.
"""
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WAREHOUSE = ROOT / "data" / "warehouse" / "tienda_dwh.db"

TABLAS_FINALES = ["categoria", "subcategoria", "producto", "cliente", "pedido",
                  "linea_pedido", "inventario", "movimiento_inventario",
                  "evento_comportamiento"]

resultados = []


def prueba(nombre, ok, detalle=""):
    estado = "PASA " if ok else "FALLA"
    resultados.append(ok)
    print(f"  [{estado}] {nombre}" + (f"  ->  {detalle}" if detalle else ""))


def uno(conn, sql):
    return conn.execute(sql).fetchone()[0]


def main():
    if not WAREHOUSE.exists():
        sys.exit("ERROR: no existe el almacen. Ejecuta load_raw.py y transform.py primero.")
    conn = sqlite3.connect(WAREHOUSE)

    print("\n=== 1. PRIMERA FORMA NORMAL (valores atomicos) ===")
    con_lista = uno(conn, "SELECT COUNT(*) FROM raw_lineas_pedido "
                          "WHERE json_array_length(registro_json, '$.lineas') > 0")
    print(f"  Antes: {con_lista} pedidos traian una LISTA anidada de lineas dentro de una celda.")
    n_lineas = uno(conn, "SELECT COUNT(*) FROM stg_lineas_pedido")
    print(f"  Despues del desanidado: {n_lineas} filas atomicas (una por linea).")
    anidados = 0
    for tabla in TABLAS_FINALES:
        columnas = [c[1] for c in conn.execute(f"PRAGMA table_info({tabla})") if c[2] == "TEXT"]
        for col in columnas:
            anidados += uno(conn, f"SELECT COUNT(*) FROM {tabla} "
                                  f"WHERE {col} LIKE '[%' OR {col} LIKE '{{%'")
    prueba("Ninguna celda de las tablas finales contiene listas u objetos JSON",
           anidados == 0, f"celdas con listas/JSON: {anidados}")

    print("\n=== 2. JOIN ROTO (texto vs numerico) ===")
    tipo_texto = uno(conn, "SELECT COUNT(*) FROM stg_lineas_pedido "
                           "WHERE typeof(producto_id_texto) = 'text'")
    tipo_int = uno(conn, "SELECT COUNT(*) FROM producto WHERE typeof(id_producto) = 'integer'")
    print(f"  Origen: producto_id llega como TEXTO en {tipo_texto} lineas del JSON; "
          f"en la tabla relacional producto es ENTERO ({tipo_int} filas).")
    con_espacios = uno(conn, "SELECT COUNT(*) FROM stg_lineas_pedido "
                             "WHERE producto_id_texto IS NOT NULL "
                             "AND producto_id_texto <> TRIM(producto_id_texto)")
    ingenuo = uno(conn, "SELECT COUNT(*) FROM stg_lineas_pedido l JOIN producto p "
                        "ON l.producto_id_texto = CAST(p.id_producto AS TEXT)")
    traducido = uno(conn, "SELECT COUNT(*) FROM stg_lineas_pedido l JOIN producto p "
                          "ON l.id_producto = p.id_producto")
    print(f"  Join comparando el texto tal cual:   {ingenuo} lineas con producto")
    print(f"  Join con tipo traducido a entero:    {traducido} lineas con producto")
    print(f"  Lineas que la traduccion rescata (ids con espacios, ej. ' 105 '): {con_espacios}")
    prueba("La traduccion de tipos mejora el join", traducido > ingenuo,
           f"{ingenuo} -> {traducido} (+{traducido - ingenuo})")

    print("\n=== 3. TIPOS DE LAS LLAVES ===")
    for tabla, col in (("producto", "id_producto"), ("cliente", "id_cliente"),
                       ("pedido", "id_pedido"), ("linea_pedido", "id_producto"),
                       ("pedido", "id_cliente")):
        tipos = [r[0] for r in conn.execute(f"SELECT DISTINCT typeof({col}) FROM {tabla}")]
        prueba(f"{tabla}.{col} es siempre entero", tipos == ["integer"], f"tipos encontrados: {tipos}")

    print("\n=== 4. INTEGRIDAD REFERENCIAL ===")
    huerfanos = conn.execute("PRAGMA foreign_key_check").fetchall()
    prueba("PRAGMA foreign_key_check no encuentra referencias rotas",
           len(huerfanos) == 0, f"referencias rotas: {len(huerfanos)}")
    rechazados = uno(conn, "SELECT COUNT(*) FROM rechazado_integridad")
    print(f"  Registros que NO entraron por romper la integridad (quedan auditables): {rechazados}")

    print("\n=== 5. TERCERA FORMA NORMAL (sin dependencias transitivas) ===")
    violan = uno(conn, "SELECT COUNT(*) FROM (SELECT subcategoria FROM stg_productos "
                       "WHERE categoria IS NOT NULL AND subcategoria IS NOT NULL "
                       "GROUP BY subcategoria HAVING COUNT(DISTINCT categoria) > 1)")
    prueba("Cada subcategoria pertenece a una sola categoria (subcategoria -> categoria)",
           violan == 0, f"subcategorias con mas de una categoria: {violan}")
    n_cat = uno(conn, "SELECT COUNT(*) FROM categoria")
    n_sub = uno(conn, "SELECT COUNT(*) FROM subcategoria")
    n_prod = uno(conn, "SELECT COUNT(*) FROM producto")
    print(f"  La categoria ahora se guarda {n_cat} veces (una por categoria), "
          f"no {n_prod} veces (una por producto).")
    prueba("producto ya no repite el nombre de categoria ni de subcategoria",
           "categoria" not in [c[1] for c in conn.execute("PRAGMA table_info(producto)")]
           and "subcategoria" not in [c[1] for c in conn.execute("PRAGMA table_info(producto)")],
           f"{n_cat} categorias, {n_sub} subcategorias")
    sin_cat = uno(conn, "SELECT COUNT(*) FROM stg_productos WHERE categoria IS NULL")
    recuperadas = uno(conn, "SELECT COUNT(*) FROM stg_productos s JOIN producto p "
                            "ON p.id_producto = s.id_producto "
                            "WHERE s.categoria IS NULL AND p.id_subcategoria IS NOT NULL")
    print(f"  Productos que venian SIN categoria en la fuente: {sin_cat}; "
          f"recuperaron su categoria a traves de la subcategoria: {recuperadas}")

    print("\n=== RESUMEN ===")
    print(f"  Pruebas que pasan: {sum(resultados)} de {len(resultados)}")
    conn.close()
    return 0 if all(resultados) else 1


if __name__ == "__main__":
    sys.exit(main())
