# 🛒 Arquitectura de Datos Híbrida y Resiliente para E-commerce

Proyecto final de **Fundamentos de Ingeniería de Datos** · Fecha: 30 de noviembre de 2026

> Pipeline de datos *end-to-end* (Batch + Stream) para un caso de uso de **E-commerce**, diseñado para sobrevivir al "caos operativo": datos corruptos, tipos mezclados, eventos retrasados y consultas analíticas lentas.

**Estado actual:** Fases 1, 2 y 3 completadas · Fases 4 y 5 en desarrollo.

---

## 👥 Equipo

| Integrante | Rol |
|---|---|
| Regina Cosette Ordoñez Pinedo | |
| Juan Pablo Huerta Gonzalez | |
| Francisco de Jesus Gonzalez Lopez | |
| Gaudencio Isaac Gutierrez Mendez | |

**Docente:** Guadalupe Jeanette González Díaz

**Institución:** Universidad de Guadalajara

**Campus:** Centro Universitario de Guadalajara (CUGDL)

---

## 📌 Contexto y problema

Una tienda en línea (real o simulada) genera datos de pedidos, clientes, productos y eventos de navegación que llegan **sucios, tarde y en formatos distintos**: nulos, valores extremos, identificadores con tipos mezclados (`"100"` vs `100`) y eventos retrasados por fallos de red. Esto provoca cruces rotos, reportes poco confiables y consultas lentas.

Este proyecto construye una arquitectura que ingiere, valida, limpia, transforma y publica esos datos en un formato analítico, **sin detenerse ante registros inválidos**.

---

## 🏗️ Arquitectura

```
 ┌─────────────┐    ┌─────────────────────────┐
 │ CSV / JSON  │───▶│ Ingesta BATCH (nocturna)│──┐
 └─────────────┘    └─────────────────────────┘  │   ┌────────────────────┐   ┌────────────────┐
                                                 ├──▶│ Validación (doble  │──▶│ Normalización  │
 ┌─────────────┐    ┌─────────────────────────┐  │   │ capa) + DLQ        │   │ 1FN → 3FN      │
 │ Eventos     │───▶│ Ingesta STREAM          │──┘   └─────────┬──────────┘   └───────┬────────┘
 │ (clics,     │    │ (micro-batch+watermark) │                │ inválidos            │
 │  compras)   │    └─────────────────────────┘                ▼                      ▼
 └─────────────┘                                        ┌────────────┐      ┌──────────────────┐
                                                        │ Tabla DLQ  │      │ Capa analítica   │
                                                        │ (auditoría)│      │ Apache Parquet   │
                                                        └────────────┘      └──────────────────┘
```

**Paradigma adoptado: ELT.** Los datos crudos se cargan primero en el almacén (SQLite) y las transformaciones se ejecutan allí con SQL, de modo que se pueden repetir sin volver al origen.

| Capa | Ubicación | Contenido |
|---|---|---|
| Fuente | `data/source/` | CSV y JSON con caos controlado |
| Cruda (raw) | `data/raw/` y tablas `raw_*` | Copia fiel de lo ingerido, todo como texto |
| Staging | tablas `stg_*` | Tipos correctos y JSON desanidado (1FN) |
| Modelo 3FN | almacén SQLite | 9 tablas normalizadas + `rechazado_integridad` |
| Analítica (Fase 2) | `data/curated/` | Un archivo Apache Parquet por tabla, sin limpiar |
| Limpia (Fase 3) | tablas `clean_*` y `data/clean/` | Datos validados e imputados en Parquet, más la DLQ y la bitácora de imputación |

> 📎 Detalle en la documentación: [Arquitectura de ingesta](docs/Fase1/arquitectura_ingesta.md) · [Modelo conceptual](docs/Fase1/modelo_conceptual.md) ([PDF](docs/Fase1/modelo_conceptual_ecommerce.drawio.pdf)) · [ELT, normalización y Parquet](docs/Fase%202/fase2_elt_normalizacion_parquet.md) · [Benchmark CSV vs Parquet](docs/Fase%202/benchmark_resultado.md) · [Validación, DLQ e imputación](docs/Fase3/fase3_validacion_dlq_imputacion.md) · [Comparación de imputación](docs/Fase3/imputacion_comparacion.md)

---

## 📂 Estructura del repositorio

```
.
├── data/
│   ├── source/                    # Datos fuente con "caos controlado"
│   ├── curated/                   # Capa analítica de la Fase 2 en Parquet (una por tabla)
│   ├── clean/                     # Capa limpia de la Fase 3 en Parquet, DLQ y auditoría
│   └── sample/                    # Muestra de 100 filas en CSV, JSON y Parquet
├── docs/
│   ├── Fase1/                     # Arquitectura de ingesta y modelo conceptual
│   ├── Fase 2/                    # ELT, normalización, Parquet y benchmark
│   ├── Fase3/                     # Validación, DLQ e imputación
│   └── screenshots/               # Capturas de evidencia por fase
├── sql/
│   ├── 01_staging.sql             # Tipos correctos y desanidado de JSON (1FN)
│   ├── 02_modelo_3fn.sql          # Modelo normalizado con llaves PK/FK
│   └── 03_validacion_dlq.sql      # Reglas de validación y Dead Letter Queue
├── src/
│   ├── f1_*.py                    # Fase 1: generación de datos, ingesta batch/stream, join roto
│   ├── f2_*.py                    # Fase 2: carga, transformación, validación, Parquet, benchmark
│   └── f3_*.py                    # Fase 3: validación, imputación, exportación, chaos test
├── requirements.txt               # pandas, pyarrow
└── README.md
```

---

## 🧰 Tecnologías

- **Python 3.11+**
- **pandas** – limpieza, transformación e imputación
- **pyarrow** – lectura/escritura de Apache Parquet
- **SQLite** – almacén destino del flujo ELT
- Orquestador: *pendiente de definir* (Fase 4)

---

## 🚀 Cómo ejecutarlo

```bash
# 1. Clonar el repositorio
git clone https://github.com/cosette212/Ingenieria-Datos-Proyecto_E-commerce.git
cd Ingenieria-Datos-Proyecto_E-commerce

# 2. (Opcional) Crear entorno virtual
python -m venv venv
source venv/bin/activate        # En Windows: venv\Scripts\activate

# 3. Instalar dependencias
pip install -r requirements.txt

# --- Fase 1: datos e ingesta ---
python src/f1_generate_chaos_data.py
python src/f1_ingest_batch.py
python src/f1_stream_simulator.py
python src/f1_ingest_stream.py
python src/f1_demo_join_roto.py

# --- Fase 2: ELT, normalización y Parquet ---
python src/f2_load_raw.py
python src/f2_transform.py
python src/f2_validate_phase2.py
python src/f2_export_parquet.py
python src/f2_benchmark_parquet.py

# --- Fase 3: validación, DLQ e imputación ---
python src/f3_validate.py
python src/f3_impute.py
python src/f3_export_clean.py
python src/f3_verificar_cadena.py
python src/f3_chaos_test.py
```

---

## ✅ Fases del proyecto

### Fase 1 · Modelado conceptual e ingesta híbrida ✔️
- [x] Modelo conceptual independiente de la tecnología
- [x] **Vía Batch:** extracción programada (cierre nocturno) desde CSV y JSON
- [x] **Vía Stream / micro-batch:** flujo de eventos con **watermarking** para eventos tardíos

### Fase 2 · ETL/ELT, normalización y almacenamiento analítico ✔️
- [x] Paradigma **ELT** elegido y justificado
- [x] Normalización **1FN → 3FN** con llaves primarias y foráneas (integridad referencial sin errores)
- [x] **Reto del Join Roto:** traducción de tipos en identificadores (`"100"` texto ↔ `100` entero)
- [x] Exportación a **Apache Parquet** con prueba comparativa frente a 365 CSV: consultas **17x a 23x más rápidas** y **10x menos espacio** en disco (3.0 MB vs 29.8 MB)

### Fase 3 · Escudo de validación y limpieza inteligente ✔️
- [x] Validación en doble capa: **esquema** (tipos técnicos) y **reglas de negocio** (p. ej. montos positivos, edades 18–100), más verificación de integridad referencial
- [x] **Dead Letter Queue (DLQ):** el escudo detecta el 7.3 % de los registros de origen con problemas (mínimo requerido: 5 %), sin detener el flujo
- [x] Imputación de nulos y outliers con **mediana** (criterio de Tukey), comparada contra la media; **KNN** se descartó y se justifica en la documentación
- [x] **Chaos Test** (`f3_chaos_test.py`) y verificación de la cadena completa (`f3_verificar_cadena.py`)

### Fase 4 · Orquestación y resiliencia
- [ ] Orquestador con dependencias entre tareas y horarios
- [ ] Política de **Retry** con espera ante fallos temporales

### Fase 5 · Gobernanza, linaje y versionado
- [ ] Diagrama de **Data Lineage** (tablas y columnas) + análisis de impacto
- [ ] **Catálogo de datos:** vista de referencia (negocio) y vista operativa (metadatos y accesos)
- [ ] Versionado semántico `MAJOR.MINOR.PATCH` (release `v1.0.0`)

---

## 🌪️ Chaos Test

Durante la demostración se inyectan en vivo registros con:

| Anomalía | Comportamiento esperado |
|---|---|
| Tipos erróneos (texto en campo numérico) | Rechazado por validación de esquema → **DLQ** |
| Valores negativos (edades, montos) | Rechazado por regla de negocio → **DLQ** |
| Nulos | Imputados (mediana / KNN) |
| Eventos tardíos | Gestionados con watermark |
| Fallo temporal de conexión | Reintento automático (Retry) |

El pipeline **no se detiene** y actualiza la capa Parquet con los datos válidos. Se ejecuta con `python src/f3_chaos_test.py` (usa `--mantener` para dejar los datos del caos en el almacén durante la demo). El reintento automático (Retry) queda para la Fase 4.

---

## 🔍 Gobernanza y linaje

- **Data Lineage y análisis de impacto:** *pendiente (Fase 5)*
- **Catálogo de datos (referencia y operativo):** *pendiente (Fase 5)*
- **Pregunta guía:** *Si mañana se renombra o elimina la columna X en el origen, ¿qué tablas intermedias y reportes finales se rompen?*

---

## 🏷️ Versionado

Se sigue [Versionado Semántico](https://semver.org/lang/es/):

| Versión | Descripción |
|---|---|
| `v0.1.0` | Modelado conceptual (Fase 1) |
| `v0.2.0` | Ingesta Híbrida (Fase 1) |
| `v0.3.0` | ELT, normalización y almacenamiento en Parquet (Fase 2) |
| `v0.4.0` | Validación, DLQ e imputación estadística (Fase 3) |
| `v1.0.0` | Entrega final del proyecto |

---

## 📦 Entregables

- [ ] Repositorio versionado (`v1.0.0`) con código, configuración del orquestador y muestras en CSV, JSON y Parquet
- [ ] Blueprint de Arquitectura y Gobernanza (modelo conceptual, arquitectura, linaje, catálogo y costos Batch vs. Stream)
- [ ] Demo en vivo o video **Chaos Test** (5 min)

---

## 📄 Licencia

Proyecto académico sin fines comerciales.
