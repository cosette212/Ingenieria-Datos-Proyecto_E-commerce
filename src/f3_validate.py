"""
FASE 3 - Paso 1: ESCUDO DE VALIDACION en doble capa + DEAD LETTER QUEUE (DLQ).

Ejecuta sql/03_validacion_dlq.sql dentro del almacen:
  Capa 1 (ESQUEMA)  : cada valor respeta su tipo tecnico.
  Capa 2 (NEGOCIO)  : cada valor respeta las reglas de negocio (R1 a R7 y otras).
  Capa 3 (INTEGRIDAD): las referencias existen en la capa limpia.
Lo que falla va a la tabla dlq (con el registro original en JSON); lo que pasa se carga en clean_*.
El flujo NO se detiene por registros malos.

Uso:
    python src/f3_validate.py
"""
import sys

from f3_comun import ENTIDADES, SQL_DIR, ahora_id, conectar, exigir_tablas


# entidades que corresponden a una fila de un CSV o a un registro raiz de un JSON de origen
ORIGEN = {"cliente", "producto", "pedido", "inventario"}


def main():
    conn = conectar()
    exigir_tablas(conn, ["raw_clientes", "raw_productos", "raw_pedidos", "raw_lineas_pedido",
                         "raw_inventario", "raw_eventos"], "python src/f2_load_raw.py")
    exigir_tablas(conn, ["categoria", "subcategoria"], "python src/f2_transform.py")

    ejecucion = ahora_id()
    print(f"Escudo de validacion | ejecucion {ejecucion}")
    try:
        conn.executescript((SQL_DIR / "03_validacion_dlq.sql").read_text(encoding="utf-8"))
        conn.execute("UPDATE dlq SET ejecucion_id = ? WHERE ejecucion_id IS NULL", (ejecucion,))
        conn.commit()
    except Exception as e:
        conn.close()
        sys.exit(f"ERROR ejecutando 03_validacion_dlq.sql: {e}")

    print(f"\n{'Entidad':<24}{'Evaluados':>10}{'Aceptados':>11}{'Rechazados':>12}{'Corregidos':>12}"
          f"{'Prob. propio':>14}")
    t_eval = t_acep = t_rech = t_corr = 0
    origen_eval = origen_propio = 0          # solo registros de origen (ver nota abajo)
    balance_ok = True
    for entidad, vista, clean, propio in ENTIDADES:
        evaluados = conn.execute(f"SELECT COUNT(*) FROM {vista}").fetchone()[0]
        rechazados = conn.execute(f"SELECT COUNT(*) FROM {vista} WHERE rechazar = 1").fetchone()[0]
        aceptados = conn.execute(f"SELECT COUNT(*) FROM {clean}").fetchone()[0]
        corregidos = conn.execute("SELECT COUNT(DISTINCT clave) FROM dlq "
                                  "WHERE entidad = ? AND accion = 'CORREGIDO'", (entidad,)).fetchone()[0]
        con_problema_propio = conn.execute(
            f"SELECT COUNT(*) FROM {vista} WHERE {propio}").fetchone()[0]
        marca = "" if evaluados == aceptados + rechazados else "  <-- NO CUADRA"
        balance_ok = balance_ok and evaluados == aceptados + rechazados
        pct = 100.0 * con_problema_propio / evaluados if evaluados else 0.0
        print(f"{entidad:<24}{evaluados:>10}{aceptados:>11}{rechazados:>12}{corregidos:>12}"
              f"{con_problema_propio:>8} ({pct:4.1f}%){marca}")
        t_eval += evaluados
        t_acep += aceptados
        t_rech += rechazados
        t_corr += corregidos
        if entidad in ORIGEN:
            origen_eval += evaluados
            origen_propio += con_problema_propio
    print(f"{'TOTAL':<24}{t_eval:>10}{t_acep:>11}{t_rech:>12}{t_corr:>12}")
    print("\nBalance (evaluados = aceptados + rechazados):", "OK" if balance_ok else "FALLA")

    print("\nDLQ por capa y accion (una fila de la DLQ por cada regla violada):")
    for capa, accion, n in conn.execute("SELECT capa, accion, COUNT(*) FROM dlq "
                                        "GROUP BY capa, accion ORDER BY capa, accion"):
        print(f"  {capa:<12}{accion:<11}{n:>6}")
    total_dlq = conn.execute("SELECT COUNT(*) FROM dlq").fetchone()[0]
    print(f"  {'TOTAL':<23}{total_dlq:>6}")

    print("\nReglas que mas se dispararon:")
    for regla, entidad, n in conn.execute("SELECT regla, entidad, COUNT(*) AS n FROM dlq "
                                          "GROUP BY regla, entidad ORDER BY n DESC, regla LIMIT 12"):
        print(f"  {regla:<42}{entidad:<24}{n:>5}")

    # Las lineas y los movimientos van anidados dentro de un pedido o un inventario: son elementos
    # de un registro de origen, no registros de origen. Los eventos tienen su propia DLQ en la Fase 1.
    tasa = 100.0 * origen_propio / origen_eval if origen_eval else 0.0
    print(f"\nRegistros de origen (clientes, productos, pedidos, inventario) con problema propio: "
          f"{origen_propio} de {origen_eval} ({tasa:.1f}%)")
    print("Requisito del enunciado (al menos 5% de registros corruptos):",
          "CUMPLE" if tasa >= 5 else "NO ALCANZA -> ejecuta f3_chaos_test.py para inyectar mas")
    conn.close()
    print("\nVALIDACION terminada. Siguiente paso: python src/f3_impute.py")
    return 0 if balance_ok else 1


if __name__ == "__main__":
    sys.exit(main())
