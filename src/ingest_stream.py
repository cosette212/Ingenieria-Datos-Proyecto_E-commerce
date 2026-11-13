"""
Ingesta STREAM (micro-batch) con WATERMARKING.

Vigila data/stream_in/ y procesa cada archivo nuevo como un micro-lote.

Watermark = (mayor event_time visto hasta ahora) - (retraso permitido).
  - event_time >= watermark  -> evento ACEPTADO   -> data/raw/eventos_stream/
  - event_time <  watermark  -> evento REZAGADO   -> data/dlq/eventos_rezagados.jsonl
  - JSON roto / sin fecha    -> evento CORRUPTO   -> data/dlq/eventos_corruptos.jsonl
El flujo NUNCA se detiene por un evento malo.

Uso:
    python src/ingest_stream.py --lateness 60 --max-idle 15
"""
import argparse
import json
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ENTRADA = ROOT / "data" / "stream_in"
PROCESADOS = ROOT / "data" / "stream_processed"
RAW_STREAM = ROOT / "data" / "raw" / "eventos_stream"
DLQ = ROOT / "data" / "dlq"


def anexar(ruta, registros):
    ruta.parent.mkdir(parents=True, exist_ok=True)
    with open(ruta, "a", encoding="utf-8") as f:
        for r in registros:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def procesar_microlote(archivo, num, estado, lateness):
    aceptados, rezagados, corruptos = [], [], []
    watermark_previo = estado["watermark"]       # se fija al INICIO del micro-lote
    mayor_event_time = estado["max_event_time"]
    ahora = datetime.now().isoformat(timespec="seconds")

    for linea in archivo.read_text(encoding="utf-8").splitlines():
        if not linea.strip():
            continue
        try:
            evento = json.loads(linea)
            event_time = datetime.fromisoformat(evento["event_time"])
        except (json.JSONDecodeError, KeyError, ValueError, TypeError) as e:
            corruptos.append({"linea_original": linea, "motivo": f"{type(e).__name__}: {e}",
                              "archivo": archivo.name, "detectado": ahora})
            continue

        if watermark_previo is not None and event_time < watermark_previo:
            segundos = (watermark_previo - event_time).total_seconds()
            rezagados.append({**evento, "motivo": "evento_tardio_bajo_watermark",
                              "segundos_bajo_watermark": round(segundos),
                              "watermark": watermark_previo.isoformat(timespec="seconds"),
                              "detectado": ahora})
        else:
            aceptados.append({**evento, "_ingest_ts": ahora, "_microlote": num})
            if mayor_event_time is None or event_time > mayor_event_time:
                mayor_event_time = event_time

    # se actualiza el watermark DESPUES de procesar el micro-lote
    estado["max_event_time"] = mayor_event_time
    if mayor_event_time is not None:
        estado["watermark"] = mayor_event_time - timedelta(seconds=lateness)

    fecha = datetime.now().strftime("%Y%m%d")
    anexar(RAW_STREAM / f"eventos_{fecha}.jsonl", aceptados)
    anexar(DLQ / "eventos_rezagados.jsonl", rezagados)
    anexar(DLQ / "eventos_corruptos.jsonl", corruptos)
    return len(aceptados), len(rezagados), len(corruptos)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--lateness", type=int, default=60,
                   help="retraso permitido en segundos (tamano del watermark)")
    p.add_argument("--poll", type=float, default=1.0, help="cada cuantos segundos revisa la carpeta")
    p.add_argument("--max-idle", type=int, default=15,
                   help="termina si pasan estos segundos sin archivos nuevos")
    args = p.parse_args()

    for carpeta in (ENTRADA, PROCESADOS):
        carpeta.mkdir(parents=True, exist_ok=True)

    estado = {"watermark": None, "max_event_time": None}
    totales = [0, 0, 0]
    num = 0
    ultimo_archivo = time.time()
    print(f"Ingesta stream iniciada | retraso permitido: {args.lateness}s | Ctrl+C para detener")

    try:
        while True:
            pendientes = sorted(ENTRADA.glob("*.jsonl"))   # los .tmp se ignoran
            for archivo in pendientes:
                num += 1
                a, r, c = procesar_microlote(archivo, num, estado, args.lateness)
                totales = [totales[0] + a, totales[1] + r, totales[2] + c]
                shutil.move(str(archivo), PROCESADOS / archivo.name)
                wm = estado["watermark"].strftime("%H:%M:%S") if estado["watermark"] else "-"
                print(f"[micro-lote {num:>3}] aceptados={a} rezagados={r} corruptos={c} "
                      f"| watermark={wm}")
                ultimo_archivo = time.time()
            if time.time() - ultimo_archivo > args.max_idle:
                print(f"Sin archivos nuevos por {args.max_idle}s. Cerrando.")
                break
            time.sleep(args.poll)
    except KeyboardInterrupt:
        print("\nDetenido manualmente.")

    total = sum(totales)
    print("\n===== RESUMEN =====")
    print(f"Eventos leidos:     {total}")
    print(f"Aceptados:          {totales[0]}")
    print(f"Rezagados (DLQ):    {totales[1]}")
    print(f"Corruptos (DLQ):    {totales[2]}")


if __name__ == "__main__":
    main()
