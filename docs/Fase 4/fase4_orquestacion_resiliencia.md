# Fase 4 · Orquestación y Resiliencia Operativa

## 1. Herramienta de Orquestación: Prefect 3
Para automatizar el pipeline *end-to-end* se implementó **Prefect 3** (`src/f4_orchestrator.py`), modelando el flujo híbrido como un Grafo Acíclico Dirigido (DAG) con trazabilidad por tarea, aislamiento de fallos y observabilidad en tiempo real mediante servidor local (`http://127.0.0.1:4200`)[cite: 1, 5].

![Dashboard general en Prefect](../screenshots/screenshots%20fase%204/Captura%203.2Toma%20la%20captura%20final%20de%20los%20despliegues%20programados.png)

---

## 2. Arquitectura del DAG y Dependencias
El flujo principal (`ecommerce-pipeline-end-to-end`) coordina 11 tareas respetando estrictamente las dependencias entre fases[cite: 3, 5]:

1. **Fase 1 · Ingesta Híbrida (Ramas paralelas iniciales):**
   * `F1.1 - Ingesta Batch Nocturna` (`src/f1_ingest_batch.py`)[cite: 3]
   * `F1.2 - Simulador Eventos Stream` (`src/f1_stream_simulator.py`) $\rightarrow$ `F1.3 - Ingesta Stream con Watermark` (`src/f1_ingest_stream.py`)[cite: 3]
2. **Fase 2 · ELT, Normalización 3FN y Parquet (Convergencia tras Batch + Stream):**
   * `F2.1 - Carga Raw a Almacén ELT` (`src/f2_load_raw.py`)[cite: 3]
   * `F2.2 - Transformación y Normalización 3FN` (`src/f2_transform.py`)[cite: 3]
   * `F2.3 - Validación Integridad Fase 2` (`src/f2_validate_phase2.py`)[cite: 3]
   * `F2.4 - Exportación Capa Analítica Parquet` (`src/f2_export_parquet.py`)[cite: 3]
3. **Fase 3 · Escudo de Calidad, DLQ e Imputación:**
   * `F3.1 - Validación Doble Capa y DLQ` (`src/f3_validate.py`)[cite: 3]
   * `F3.2 - Imputación Estadística (Mediana)` (`src/f3_impute.py`)[cite: 3]
   * `F3.3 - Publicación Capa Limpia Parquet` (`src/f3_export_clean.py`)[cite: 3]
   * `F3.4 - Verificación End-to-End de Cadena` (`src/f3_verificar_cadena.py`) — **69/69 comprobaciones aprobadas**[cite: 1, 3].

![Grafo DAG completo y dependencias](../screenshots/screenshots%20fase%204/Captura%201%20Grafo%20DAG%20completo%20y%20dependencias.png)

---

## 3. Horarios Programados y Priorización por Costo (FinOps)
Configurados en `prefect.yaml` mediante dos despliegues independientes asociados al work pool `local-process-pool`[cite: 4, 5]:

| Despliegue | Flujo (`Entrypoint`) | Horario (`cron`) | Prioridad | Estrategia FinOps |
|---|---|---|---|---|
| `batch-nocturno-completo` | `src/f4_orchestrator.py:flujo_pipeline_completo`[cite: 5] | `0 2 * * *` (02:00 AM)[cite: 5] | Baja (`batch`)[cite: 5] | Ejecuta la carga pesada y reconstrucción 3FN/Parquet en ventana valle nocturna, liberando recursos de cómputo al finalizar. |
| `stream-microbatch-continuo` | `src/f4_orchestrator.py:flujo_stream_microbatch`[cite: 4] | `*/15 * * * *` (cada 15 min)[cite: 4] | Alta (`stream`)[cite: 4] | Ingiere únicamente micro-lotes ligeros con *watermarking* de 60s para mantener frescos los eventos sin reprocesar el histórico. |

### Despliegue Batch Nocturno (`02:00 AM`)
![Despliegue batch nocturno 2 am](../screenshots/screenshots%20fase%204/Captura%203%20Toma%20la%20captura%20final%20de%20los%20despliegues%20programados%20a%20las%202%20am.png)

### Despliegue Stream Micro-batch (`cada 15 min`)
![Despliegue stream cada 15 min](../screenshots/screenshots%20fase%204/Captura%203.1%20Toma%20la%20captura%20final%20de%20los%20despliegues%20programados%20cada%2015%20min.png)

---

## 4. Resiliencia Operativa: Política `Retry`
Todas las tareas están configuradas con `@task(retries=3, retry_delay_seconds=5)`.

* **Prueba de Caos Operativo:** Ejecutando `python src/f4_orchestrator.py --simular-fallo`, se inyecta un `ConnectionError` controlado en el primer intento de `F2.1 - Carga Raw a Almacén ELT`[cite: 2].
* **Comportamiento observado:** Prefect captura la excepción, transiciona la tarea a estado `Retrying`, espera 5 segundos y relanza automáticamente la ejecución en el segundo intento hasta completar las 69 verificaciones finales sin intervención manual[cite: 1, 2].

![Evidencia política Retry - parte 1](../screenshots/screenshots%20fase%204/Captura%202.1%20Evidencia%20de%20la%20politica%20Retry%20ante%20fallo%20temporal.png)

![Evidencia política Retry - parte 2](../screenshots/screenshots%20fase%204/Captura%202.2%20Evidencia%20de%20la%20politica%20Retry%20ante%20fallo%20temporal.png)