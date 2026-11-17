"""
Fase 4: Orquestación y Resiliencia Operativa con Prefect.

Automatiza de punta a punta las Fases 1, 2 y 3 respetando:
  - Dependencias estrictas en el DAG (Batch + Stream -> ELT/Parquet -> Calidad/DLQ).
  - Priorización por costo de cómputo (Batch nocturno vs. Stream micro-batch).
  - Política de Retry automático con espera ante fallos temporales de conexión.

Uso:
    python src/f4_orchestrator.py                  # Ejecución completa normal
    python src/f4_orchestrator.py --simular-fallo  # Demuestra Retry ante fallo temporal
    python src/f4_orchestrator.py --solo-stream    # Ejecuta solo micro-batch de eventos
"""
import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from prefect import flow, get_run_logger, task
from prefect.tasks import task_input_hash

ROOT = Path(__file__).resolve().parent.parent
PYTHON = sys.executable

# Estado en memoria para simular un fallo temporal en el primer intento
_INTENTOS_SIMULADOS = {}


def ejecutar_script(script_relativo: str, args: list[str] | None = None, simular_fallo_id: str | None = None):
    """Ejecuta un script Python del proyecto y lanza excepción si retorna código != 0."""
    logger = get_run_logger()
    args = args or []

    # Simulación controlada de fallo temporal de red/almacén en el intento #1
    if simular_fallo_id:
        intento_actual = _INTENTOS_SIMULADOS.get(simular_fallo_id, 0) + 1
        _INTENTOS_SIMULADOS[simular_fallo_id] = intento_actual
        if intento_actual == 1:
            logger.warning(
                f"[CAOS OPERATIVO] Fallo temporal de conexión simulado en '{script_relativo}' "
                f"(intento {intento_actual}). Activando política Retry..."
            )
            raise ConnectionError(f"Timeout temporal al conectar con el almacén destino en {script_relativo}")

    ruta_script = ROOT / script_relativo
    cmd = [PYTHON, str(ruta_script), *args]
    logger.info(f"Ejecutando: {' '.join([script_relativo, *args])}")

    inicio = time.perf_counter()
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"

    res = subprocess.run(
        cmd,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )
    duracion = round(time.perf_counter() - inicio, 2)

    if res.stdout:
        for linea in res.stdout.strip().splitlines()[-8:]:
            logger.info(f"  | {linea}")

    if res.returncode != 0:
        logger.error(f"Error en {script_relativo} (código {res.returncode}) tras {duracion}s")
        if res.stderr:
            logger.error(res.stderr.strip())
        raise RuntimeError(f"Fallo en tarea {script_relativo} (exit code {res.returncode})")

    logger.info(f"Completado: {script_relativo} en {duracion}s")
    return {"script": script_relativo, "duracion_s": duracion, "status": "OK"}


# ==============================================================================
# TAREAS DEL DAG (Política Retry: hasta 3 reintentos con espera de 5 segundos)
# ==============================================================================

@task(
    name="F1.1 - Ingesta Batch Nocturna",
    description="Extrae CSV/JSON hacia data/raw/. Prioridad BAJA (ventana nocturna FinOps).",
    retries=3,
    retry_delay_seconds=5,
    tags=["fase-1", "batch", "prioridad-baja"],
)
def tarea_f1_ingest_batch():
    return ejecutar_script("src/f1_ingest_batch.py")


@task(
    name="F1.2 - Simulador Eventos Stream",
    description="Genera micro-lotes en tiempo real con eventos retrasados y corruptos.",
    retries=2,
    retry_delay_seconds=3,
    tags=["fase-1", "stream", "prioridad-alta"],
)
def tarea_f1_stream_simulator(duracion: int = 6):
    return ejecutar_script("src/f1_stream_simulator.py", ["--duration", str(duracion)])


@task(
    name="F1.3 - Ingesta Stream con Watermark",
    description="Consume micro-lotes aplicando watermark (60s) y desvía rezagados/corruptos a DLQ.",
    retries=3,
    retry_delay_seconds=5,
    tags=["fase-1", "stream", "prioridad-alta"],
)
def tarea_f1_ingest_stream(lateness: int = 60, max_idle: int = 3):
    return ejecutar_script(
        "src/f1_ingest_stream.py",
        ["--lateness", str(lateness), "--max-idle", str(max_idle)],
    )


@task(
    name="F2.1 - Carga Raw a Almacén ELT",
    description="Carga datos crudos Batch y Stream en SQLite.",
    retries=3,
    retry_delay_seconds=5,
    tags=["fase-2", "elt", "prioridad-media"],
)
def tarea_f2_load_raw(simular_fallo: bool = False):
    fallo_id = "f2_load_raw" if simular_fallo else None
    return ejecutar_script("src/f2_load_raw.py", simular_fallo_id=fallo_id)


@task(
    name="F2.2 - Transformación y Normalización 3FN",
    description="Desanida JSON, resuelve Join Roto (texto->entero) y normaliza 1FN->3FN.",
    retries=3,
    retry_delay_seconds=5,
    tags=["fase-2", "elt", "prioridad-media"],
)
def tarea_f2_transform():
    return ejecutar_script("src/f2_transform.py")


@task(
    name="F2.3 - Validación Integridad Fase 2",
    description="Verifica llaves PK/FK y recuperación de categorías en modelo 3FN.",
    retries=2,
    retry_delay_seconds=3,
    tags=["fase-2", "qa"],
)
def tarea_f2_validate():
    return ejecutar_script("src/f2_validate_phase2.py")


@task(
    name="F2.4 - Exportación Capa Analítica Parquet",
    description="Exporta las 10 tablas 3FN a formato columnar Apache Parquet.",
    retries=3,
    retry_delay_seconds=5,
    tags=["fase-2", "parquet"],
)
def tarea_f2_export_parquet():
    return ejecutar_script("src/f2_export_parquet.py")


@task(
    name="F3.1 - Validación Doble Capa y DLQ",
    description="Filtra errores de esquema y reglas de negocio hacia Dead Letter Queue.",
    retries=3,
    retry_delay_seconds=5,
    tags=["fase-3", "calidad", "dlq"],
)
def tarea_f3_validate():
    return ejecutar_script("src/f3_validate.py")


@task(
    name="F3.2 - Imputación Estadística (Mediana)",
    description="Imputa nulos y outliers por criterio de Tukey registrando bitácora.",
    retries=2,
    retry_delay_seconds=5,
    tags=["fase-3", "calidad", "imputacion"],
)
def tarea_f3_impute():
    return ejecutar_script("src/f3_impute.py")


@task(
    name="F3.3 - Publicación Capa Limpia Parquet",
    description="Exporta tablas limpias a data/clean/ y genera dlq_auditoria.csv.",
    retries=3,
    retry_delay_seconds=5,
    tags=["fase-3", "parquet"],
)
def tarea_f3_export_clean():
    return ejecutar_script("src/f3_export_clean.py")


@task(
    name="F3.4 - Verificación End-to-End de Cadena",
    description="Audita que los datos atravesaron íntegros las Fases 1, 2 y 3.",
    retries=2,
    retry_delay_seconds=3,
    tags=["fase-3", "auditoria"],
)
def tarea_f3_verificar_cadena():
    return ejecutar_script("src/f3_verificar_cadena.py")


# ==============================================================================
# FLUJOS ORQUESTADOS (DAGS)
# ==============================================================================

@flow(
    name="ecommerce-stream-microbatch",
    description="Flujo de alta prioridad (cada 15 min): captura y filtra eventos en tiempo real con watermark.",
    log_prints=True,
)
def flujo_stream_microbatch(duracion_simulador: int = 6):
    t_sim = tarea_f1_stream_simulator(duracion=duracion_simulador)
    t_ing = tarea_f1_ingest_stream(lateness=60, max_idle=3, wait_for=[t_sim])
    return {"stream_simulator": t_sim, "stream_ingest": t_ing}


@flow(
    name="ecommerce-pipeline-end-to-end",
    description="DAG maestro híbrido (Batch + Stream -> ELT 3FN -> Escudo Calidad/DLQ -> Parquet).",
    log_prints=True,
)
def flujo_pipeline_completo(simular_fallo: bool = False, duracion_stream: int = 6):
    logger = get_run_logger()
    inicio_global = time.perf_counter()
    logger.info("=== INICIANDO PIPELINE ORQUESTADO E-COMMERCE (FASE 4) ===")

    # 1. Fase 1: Ingesta Híbrida (Batch nocturno + Micro-batch Stream)
    res_batch = tarea_f1_ingest_batch()
    res_sim = tarea_f1_stream_simulator(duracion=duracion_stream)
    res_stream = tarea_f1_ingest_stream(lateness=60, max_idle=3, wait_for=[res_sim])

    # 2. Fase 2: ELT, Normalización 3FN y Parquet (depende de Batch y Stream)
    res_load = tarea_f2_load_raw(
        simular_fallo=simular_fallo,
        wait_for=[res_batch, res_stream],
    )
    res_trans = tarea_f2_transform(wait_for=[res_load])
    res_val2 = tarea_f2_validate(wait_for=[res_trans])
    res_parq = tarea_f2_export_parquet(wait_for=[res_val2])

    # 3. Fase 3: Escudo de Calidad, DLQ e Imputación (depende de Fase 2)
    res_val3 = tarea_f3_validate(wait_for=[res_parq])
    res_imp = tarea_f3_impute(wait_for=[res_val3])
    res_clean = tarea_f3_export_clean(wait_for=[res_imp])
    res_audit = tarea_f3_verificar_cadena(wait_for=[res_clean])

    total_s = round(time.perf_counter() - inicio_global, 2)
    logger.info(f"=== PIPELINE COMPLETADO EXITOSAMENTE EN {total_s}s ===")
    return {"duracion_total_s": total_s, "verificacion_final": res_audit}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Orquestador Fase 4 - Pipeline E-commerce")
    parser.add_argument(
        "--simular-fallo",
        action="store_true",
        help="Simula un fallo temporal de conexión en f2_load_raw para probar la política Retry",
    )
    parser.add_argument(
        "--solo-stream",
        action="store_true",
        help="Ejecuta únicamente el micro-batch de eventos en tiempo real",
    )
    parser.add_argument(
        "--duracion-stream",
        type=int,
        default=6,
        help="Segundos de generación de eventos del simulador stream (default: 6)",
    )
    args = parser.parse_args()

    if args.solo_stream:
        flujo_stream_microbatch(duracion_simulador=args.duracion_stream)
    else:
        flujo_pipeline_completo(
            simular_fallo=args.simular_fallo,
            duracion_stream=args.duracion_stream,
        )