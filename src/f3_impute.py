"""
FASE 3 - Paso 2: IMPUTACION ESTADISTICA de nulos y outliers, y comparacion media vs mediana.

Columnas que se imputan (las unicas numericas descriptivas con nulos y valores extremos):
  clean_cliente.edad            (entero; rango de negocio 18-100)
  clean_producto.precio_lista   (decimal, distribucion ASIMETRICA a la derecha)

Para cada columna:
  1. Detecta OUTLIERS con el criterio de Tukey (3 x IQR). En el precio se aplica sobre el
     logaritmo, porque una distribucion asimetrica marcaria como "outlier" valores normales.
  2. Los outliers se vuelven NULL y TODOS los NULL se rellenan con la MEDIANA de los valores validos.
  3. Cada imputacion queda registrada en imputacion_log y marcada en la tabla (edad_imputada, precio_imputado).
  4. COMPARACION: se esconde 20 % de los valores conocidos y se imputan con media y con mediana,
     con datos contaminados y con datos limpios, para medir el error (MAE y RMSE).

Uso:
    python src/f3_impute.py
"""
import sys

import numpy as np
import pandas as pd

from f3_comun import DOCS_F3, ahora_id, conectar, exigir_tablas

K_TUKEY = 3.0           # 3 x IQR = "outliers extremos"
FRACCION_PRUEBA = 0.20
SEMILLA = 42

OBJETIVOS = [
    {"tabla": "clean_cliente", "clave": "id_cliente", "columna": "edad", "flag": "edad_imputada",
     "entidad": "cliente", "reglas": ("E02", "N02"),
     "vista": "v3_cliente", "clave_vista": "id_cliente", "raw_txt": "edad_raw",
     "origen": "rn = 1 AND id_cliente IS NOT NULL", "invalido": "(edad < 18 OR edad > 100)",
     "entero": True, "log_escala": False,
     "descripcion": "edad del cliente (entero, rango de negocio 18-100)"},
    {"tabla": "clean_producto", "clave": "id_producto", "columna": "precio_lista",
     "flag": "precio_imputado", "entidad": "producto", "reglas": ("E05", "N05"),
     "vista": "v3_producto", "clave_vista": "id_producto", "raw_txt": "precio_raw",
     "origen": "rn = 1 AND id_producto IS NOT NULL", "invalido": "precio_lista <= 0",
     "entero": False, "log_escala": True,
     "descripcion": "precio de lista (decimal, asimetrico a la derecha)"},
]


def fences(valores, log_escala):
    """Limites de Tukey. Con log_escala se calculan sobre el logaritmo (valores > 0)."""
    x = np.log(valores) if log_escala else valores
    q1, q3 = np.quantile(x, [0.25, 0.75])
    iqr = q3 - q1
    return q1 - K_TUKEY * iqr, q3 + K_TUKEY * iqr


def comparar(train, malos, entero):
    """Backtest: esconde 20 % de los valores buenos y mide el error de cada estrategia."""
    rng = np.random.default_rng(SEMILLA)
    vals = np.asarray(train, dtype=float)
    orden = rng.permutation(len(vals))
    n_prueba = max(1, int(round(len(vals) * FRACCION_PRUEBA)))
    prueba = vals[orden[:n_prueba]]
    ajuste = vals[orden[n_prueba:]]
    contaminado = np.concatenate([ajuste, np.asarray(malos, dtype=float)])
    estrategias = [
        ("Media, con datos contaminados", contaminado.mean()),
        ("Mediana, con datos contaminados", float(np.median(contaminado))),
        ("Media, con datos limpios", ajuste.mean()),
        ("Mediana, con datos limpios", float(np.median(ajuste))),
    ]
    filas = []
    for nombre, valor in estrategias:
        valor = int(round(valor)) if entero else round(float(valor), 2)
        error = prueba - valor
        filas.append((nombre, valor, float(np.mean(np.abs(error))),
                      float(np.sqrt(np.mean(error ** 2)))))
    return filas, n_prueba, len(ajuste)


def imputar(conn, o, ejecucion):
    tabla, clave, col, flag = o["tabla"], o["clave"], o["columna"], o["flag"]
    filas = conn.execute(f"SELECT {clave}, {col}, {flag} FROM {tabla} ORDER BY {clave}").fetchall()
    df = pd.DataFrame(filas, columns=["clave", "valor", "imputado"])
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

    base = df[(df["imputado"] == 0) & df["valor"].notna()]
    if base.empty:
        print(f"  AVISO: {tabla}.{col} no tiene valores validos para calcular la mediana")
        return None

    usar_log = o["log_escala"] and bool((base["valor"] > 0).all())
    lo, hi = fences(base["valor"].to_numpy(float), usar_log)
    x = np.log(base["valor"]) if usar_log else base["valor"]
    es_outlier = (x < lo) | (x > hi)
    outliers = base[es_outlier]
    train = base[~es_outlier]["valor"]

    mediana = float(train.median())
    relleno = int(round(mediana)) if o["entero"] else round(mediana, 2)

    # valores que llegaron mal de origen y que las reglas de validacion pusieron en NULL
    malos_reglas = [r[0] for r in conn.execute(
        f"SELECT {col} FROM {o['vista']} WHERE {o['origen']} AND {col} IS NOT NULL "
        f"AND {o['invalido']}")]
    malos = malos_reglas + [float(v) for v in outliers["valor"]]

    # bitacora: por que cada valor llego a ser NULL
    filtros = " OR ".join(f"regla LIKE '{r}%'" for r in o["reglas"])
    por_regla = {c: r for c, r in conn.execute(
        f"SELECT clave, regla FROM dlq WHERE entidad = ? AND accion = 'CORREGIDO' AND ({filtros})",
        (o["entidad"],))}
    texto_raw = {str(c): t for c, t in conn.execute(
        f"SELECT {o['clave_vista']}, {o['raw_txt']} FROM {o['vista']} WHERE {o['origen']}")}

    a_imputar = []
    for _, fila in df[(df["valor"].isna()) & (df["imputado"] == 0)].iterrows():
        k = str(int(fila["clave"]))
        motivo = f"regla_{por_regla[k][:3]}" if k in por_regla else "nulo_en_origen"
        a_imputar.append((int(fila["clave"]), motivo, texto_raw.get(k)))
    for _, fila in outliers.iterrows():
        a_imputar.append((int(fila["clave"]), "outlier_tukey_3xIQR", f"{fila['valor']:.2f}"))

    for k, motivo, original in a_imputar:
        conn.execute(f"UPDATE {tabla} SET {col} = ?, {flag} = 1 WHERE {clave} = ?", (relleno, k))
        conn.execute("INSERT INTO imputacion_log (ejecucion_id, tabla, clave, columna, motivo, "
                     "valor_original, valor_imputado, metodo) VALUES (?, ?, ?, ?, ?, ?, ?, 'mediana')",
                     (ejecucion, tabla, str(k), col, motivo, original, float(relleno)))
    conn.commit()

    restantes = conn.execute(f"SELECT COUNT(*) FROM {tabla} WHERE {col} IS NULL").fetchone()[0]
    resumen = {"objetivo": o, "n_base": len(base), "n_train": len(train),
               "n_outliers": len(outliers), "n_malos_reglas": len(malos_reglas),
               "n_imputados": len(a_imputar), "relleno": relleno, "restantes": restantes,
               "media_limpia": float(train.mean()), "mediana_limpia": mediana,
               "asimetria": float(train.skew()) if len(train) > 2 else float("nan"),
               "media_contaminada": float(np.mean(list(train) + malos)),
               "mediana_contaminada": float(np.median(list(train) + malos)),
               "minimo": float(train.min()), "maximo": float(train.max()),
               "limites": (float(np.exp(lo)) if usar_log else float(lo),
                           float(np.exp(hi)) if usar_log else float(hi)),
               "motivos": pd.Series([m for _, m, _ in a_imputar]).value_counts().to_dict()
               if a_imputar else {}}
    if len(train) >= 5:
        resumen["comparacion"] = comparar(train, malos, o["entero"])
    return resumen


def imprimir(r):
    o = r["objetivo"]
    print(f"\n=== {o['tabla']}.{o['columna']}: {o['descripcion']} ===")
    print(f"  Valores validos de origen: {r['n_base']} | outliers detectados: {r['n_outliers']} | "
          f"valores invalidos por reglas: {r['n_malos_reglas']}")
    print(f"  Asimetria (skew) de los valores buenos: {r['asimetria']:.2f}  "
          f"({'asimetrica a la derecha' if r['asimetria'] > 0.5 else 'casi simetrica'})")
    print(f"  Con datos limpios       -> media {r['media_limpia']:,.2f} | mediana {r['mediana_limpia']:,.2f}")
    print(f"  Con datos contaminados  -> media {r['media_contaminada']:,.2f} | "
          f"mediana {r['mediana_contaminada']:,.2f}")
    print(f"  Imputados: {r['n_imputados']} con la mediana = {r['relleno']:,} | motivos: {r['motivos']}")
    print(f"  NULL restantes en la columna: {r['restantes']}")
    if "comparacion" in r:
        filas, n_prueba, n_ajuste = r["comparacion"]
        print(f"\n  Comparacion (se esconden {n_prueba} valores conocidos y se imputan; ajuste con {n_ajuste}):")
        print(f"  {'Estrategia':<34}{'Valor usado':>14}{'MAE':>14}{'RMSE':>16}")
        for nombre, valor, mae, rmse in filas:
            print(f"  {nombre:<34}{valor:>14,.2f}{mae:>14,.2f}{rmse:>16,.2f}")


def escribir_reporte(resumenes, ejecucion):
    DOCS_F3.mkdir(parents=True, exist_ok=True)
    L = ["# Imputacion estadistica: media vs mediana", "",
         f"- Ejecucion: {ejecucion}",
         f"- Outliers: criterio de Tukey con {K_TUKEY:g} x IQR (en escala logaritmica para el precio)",
         f"- Comparacion: se esconde {int(FRACCION_PRUEBA * 100)} % de los valores buenos (semilla {SEMILLA}) "
         "y se imputan con cada estrategia; MAE = error absoluto medio, RMSE = raiz del error cuadratico medio.",
         "- *Contaminados* = valores buenos + los que llegaron invalidos o extremos de origen. "
         "*Limpios* = solo los valores buenos.", ""]
    for r in resumenes:
        o = r["objetivo"]
        L += [f"## {o['tabla']}.{o['columna']}", "", f"{o['descripcion'].capitalize()}.", "",
              "| Medida | Valor |", "|---|---:|",
              f"| Valores validos de origen | {r['n_base']} |",
              f"| Outliers detectados | {r['n_outliers']} |",
              f"| Valores invalidos puestos en NULL por las reglas | {r['n_malos_reglas']} |",
              f"| Asimetria (skew) | {r['asimetria']:.2f} |",
              f"| Media / mediana con datos limpios | {r['media_limpia']:,.2f} / {r['mediana_limpia']:,.2f} |",
              f"| Media / mediana con datos contaminados | {r['media_contaminada']:,.2f} / "
              f"{r['mediana_contaminada']:,.2f} |",
              f"| Valores imputados | {r['n_imputados']} (mediana = {r['relleno']:,}) |", ""]
        if "comparacion" in r:
            filas, n_prueba, n_ajuste = r["comparacion"]
            L += [f"Prueba con {n_prueba} valores escondidos (ajuste con {n_ajuste}):", "",
                  "| Estrategia | Valor usado | MAE | RMSE |", "|---|---:|---:|---:|"]
            for nombre, valor, mae, rmse in filas:
                L.append(f"| {nombre} | {valor:,.2f} | {mae:,.2f} | {rmse:,.2f} |")
            mae = {n: m for n, _, m, _ in filas}
            rmse = {n: e for n, _, _, e in filas}
            mc, dc = mae["Media, con datos contaminados"], mae["Mediana, con datos contaminados"]
            L += ["", "Lectura:", ""]
            if dc > 0 and mc / dc >= 2:
                L.append(f"- Con datos contaminados la media se equivoca {mc / dc:,.1f} veces mas que la "
                         f"mediana (MAE {mc:,.2f} contra {dc:,.2f}).")
            elif mc > dc * 1.02:
                L.append(f"- Con datos contaminados la media se equivoca algo mas que la mediana "
                         f"(MAE {mc:,.2f} contra {dc:,.2f}): hay pocos valores invalidos y no pesan mucho.")
            else:
                L.append(f"- Con datos contaminados media y mediana empatan (MAE {mc:,.2f} contra {dc:,.2f}).")
            for metrica, tabla in (("MAE", mae), ("RMSE", rmse)):
                m_media, m_mediana = (tabla["Media, con datos limpios"], tabla["Mediana, con datos limpios"])
                if abs(m_media - m_mediana) <= 0.01 * max(m_media, m_mediana):
                    L.append(f"- Con datos limpios, por {metrica} empatan ({m_mediana:,.2f} la mediana, "
                             f"{m_media:,.2f} la media).")
                else:
                    ganador = "mediana" if m_mediana < m_media else "media"
                    L.append(f"- Con datos limpios, por {metrica} gana la {ganador} ({m_mediana:,.2f} la "
                             f"mediana, {m_media:,.2f} la media).")
            L += ["", "La media minimiza el error cuadratico (RMSE), pero un solo valor extremo la arruina; "
                      "la mediana casi no se mueve. Por eso la mediana es la opcion segura.", ""]
    (DOCS_F3 / "imputacion_comparacion.md").write_text("\n".join(L) + "\n", encoding="utf-8")


def main():
    conn = conectar()
    exigir_tablas(conn, ["clean_cliente", "clean_producto", "dlq", "imputacion_log"],
                  "python src/f3_validate.py")
    ejecucion = ahora_id()
    print(f"Imputacion estadistica | ejecucion {ejecucion}")
    resumenes = []
    for o in OBJETIVOS:
        r = imputar(conn, o, ejecucion)
        if r:
            imprimir(r)
            resumenes.append(r)
    escribir_reporte(resumenes, ejecucion)
    pendientes = sum(r["restantes"] for r in resumenes)
    total = conn.execute("SELECT COUNT(*) FROM imputacion_log").fetchone()[0]
    print(f"\nImputaciones registradas en imputacion_log: {total}")
    print(f"Reporte guardado en {DOCS_F3 / 'imputacion_comparacion.md'}")
    conn.close()
    print("\nIMPUTACION terminada. Siguiente paso: python src/f3_export_clean.py")
    return 0 if pendientes == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
