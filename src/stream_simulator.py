"""
Simulador de STREAM: genera eventos de comportamiento en tiempo real.

Cada pocos segundos escribe un pequeno archivo (micro-lote) en data/stream_in/.
Algunos eventos llegan TARDE (event_time viejo respecto a la hora de llegada) y algunos
vienen CORRUPTOS, para probar el watermark y la Dead Letter Queue.

Uso:
    python src/stream_simulator.py --duration 40
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SALIDA = ROOT / "data" / "stream_in"

TIPOS = ["vista", "añadir_al_carrito", "checkout", "compra"]
DISPOSITIVOS = ["móvil", "escritorio", "tableta"]
FUENTES = ["orgánico", "pago", "directo"]


def generar_evento():
    ahora = datetime.now()
    r = random.random()
    if r < 0.78:
        retraso = random.uniform(0, 3)           # llega casi en tiempo real
    elif r < 0.92:
        retraso = random.uniform(10, 50)         # llega con retraso tolerable
    else:
        retraso = random.uniform(90, 600)        # llega MUY tarde (fallo de red)
    tipo = random.choice(TIPOS)
    evento = {
        "id_evento": uuid.uuid4().hex[:12],
        "id_cliente": str(random.randint(1, 500)) if random.random() > 0.3 else None,
        "producto_id": None if tipo == "checkout" else str(random.randint(100, 199)),  # TEXTO
        "tipo_evento": tipo,
        "event_time": (ahora - timedelta(seconds=retraso)).isoformat(timespec="seconds"),
        "arrival_time": ahora.isoformat(timespec="seconds"),
        "sesion": "S-" + uuid.uuid4().hex[:6],
        "dispositivo": random.choice(DISPOSITIVOS),
        "fuente_trafico": random.choice(FUENTES),
    }
    return evento


def linea_para_escribir(evento):
    """3% de las lineas se corrompen a proposito."""
    if random.random() < 0.03:
        tipo = random.choice(["json_roto", "sin_event_time", "fecha_basura"])
        if tipo == "json_roto":
            return '{"id_evento": "xx", "tipo_evento": "vis'
        if tipo == "sin_event_time":
            evento = dict(evento)
            evento.pop("event_time")
        else:
            evento = dict(evento, event_time="ayer por la tarde")
    return json.dumps(evento, ensure_ascii=False)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--duration", type=int, default=40, help="segundos que corre el simulador")
    p.add_argument("--interval", type=float, default=2.0, help="segundos entre micro-lotes")
    p.add_argument("--events", type=int, default=8, help="eventos por micro-lote")
    args = p.parse_args()

    SALIDA.mkdir(parents=True, exist_ok=True)
    fin = time.time() + args.duration
    n = 0
    print(f"Simulador iniciado: {args.duration}s, un archivo cada {args.interval}s")
    while time.time() < fin:
        n += 1
        lineas = [linea_para_escribir(generar_evento()) for _ in range(args.events)]
        nombre = f"events_{datetime.now().strftime('%H%M%S')}_{n:04d}.jsonl"
        tmp = SALIDA / (nombre + ".tmp")
        tmp.write_text("\n".join(lineas) + "\n", encoding="utf-8")
        tmp.rename(SALIDA / nombre)              # se renombra al final: nadie lee archivos a medias
        print(f"  escrito {nombre} ({len(lineas)} eventos)")
        time.sleep(args.interval)
    print(f"Simulador terminado: {n} archivos generados")


if __name__ == "__main__":
    main()
