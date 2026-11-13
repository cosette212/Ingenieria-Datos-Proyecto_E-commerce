# 🛒 Arquitectura de Datos Híbrida y Resiliente para E-commerce

Proyecto final de **Fundamentos de Ingeniería de Datos** · Fecha: 30 de noviembre de 2026

> Pipeline de datos *end-to-end* (Batch + Stream) para un caso de uso de **E-commerce**, diseñado para sobrevivir al "caos operativo": datos corruptos, tipos mezclados, eventos retrasados y consultas analíticas lentas.

---

## 👥 Equipo

| Integrante | Rol |
|---|---|
| <!-- Nombre completo --> | <!-- Rol --> |
| <!-- Nombre completo --> | <!-- Rol --> |

**Docente:** Guadalupe Jeanette González Díaz
**Institución:** Universidad de Guadalajara

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

> 📎 Modelo conceptual: [`docs/modelo_conceptual.md`](docs/modelo_conceptual.md) · Diagrama: [`docs/fase1/modelo_conceptual_ecommerce.drawio.png`](docs/fase1/modelo_conceptual_ecommerce.drawio.png) ([PDF](docs/fase1/modelo_conceptual_ecommerce.drawio.pdf))

**Paradigma adoptado:** **ELT** — los datos crudos se cargan primero en el destino y las transformaciones se ejecutan allí. La justificación completa está en [`docs/arquitectura_ingesta.md`](docs/arquitectura_ingesta.md).

---

## 📂 Estructura del repositorio

```
.
├── data/source/                   # Datos fuente con "caos controlado"
│   ├── clientes.csv
│   ├── pedidos.csv
│   ├── productos.csv
│   ├── inventario.json
│   └── lineas_pedido.json
├── docs/
│   ├── arquitectura_ingesta.md    # Arquitectura de ingesta y paradigma ELT
│   ├── modelo_conceptual.md       # Modelo conceptual (Fase 1)
│   ├── fase1/                     # Diagrama conceptual (.drawio.pdf / .png / .xml)
│   └── screenshots/               # Capturas de evidencia
├── src/
│   ├── generate_chaos_data.py     # Genera datos con nulos, outliers y tipos mixtos
│   ├── ingest_batch.py            # Ingesta por lotes (CSV / JSON)
│   ├── ingest_stream.py           # Ingesta de flujo continuo (micro-batch)
│   ├── stream_simulator.py        # Simulador de eventos en tiempo real
│   └── demo_join_roto.py          # Demo del reto del Join Roto
├── .gitignore
├── requirements.txt               # pandas, pyarrow
└── README.md
```

### Datos fuente

| Archivo | Formato | Contenido |
|---|---|---|
| `clientes.csv` | CSV | Clientes de la tienda |
| `pedidos.csv` | CSV | Encabezados de pedidos |
| `productos.csv` | CSV | Catálogo de productos |
| `lineas_pedido.json` | JSON | Líneas de cada pedido (estructura jerárquica) |
| `inventario.json` | JSON | Existencias de inventario |

---

## 🧰 Tecnologías

- **Python 3.11+**
- **pandas** – limpieza, transformación e imputación
- **pyarrow** – lectura/escritura de Apache Parquet
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

# 4. Generar los datos con caos controlado
python src/generate_chaos_data.py

# 5. Ingesta Batch
python src/ingest_batch.py

# 6. Ingesta Stream (simulador de eventos + consumidor)
python src/stream_simulator.py
python src/ingest_stream.py

# 7. Demo del Join Roto
python src/demo_join_roto.py
```

---

## ✅ Fases del proyecto

### Fase 1 · Modelado conceptual e ingesta híbrida
- [x] Modelo conceptual independiente de la tecnología ([`docs/modelo_conceptual.md`](docs/modelo_conceptual.md))
- [x] **Vía Batch** (`src/ingest_batch.py`): extracción programada (cierre nocturno) desde CSV y JSON; justificación de costos frente a un servidor 24/7
- [x] **Vía Stream / micro-batch** (`src/ingest_stream.py`, `src/stream_simulator.py`): flujo de eventos con **watermarking** para eventos tardíos

### Fase 2 · ETL/ELT, normalización y almacenamiento analítico
- [x] Paradigma **ELT** elegido y justificado ([`docs/arquitectura_ingesta.md`](docs/arquitectura_ingesta.md))
- [ ] Normalización 1FN → 3FN (valores atómicos, sin dependencias transitivas)
- [x] **Reto del Join Roto** (`src/demo_join_roto.py`): traducción de tipos en identificadores (`"100"` texto ↔ `100` entero)
- [ ] Exportación a **Apache Parquet** con prueba comparativa de rendimiento vs. múltiples CSV

### Fase 3 · Escudo de validación y limpieza inteligente
- [ ] Validación por **esquema** (tipos técnicos)
- [ ] Validación por **reglas de negocio** (p. ej. montos positivos, edades > 18)
- [ ] **Dead Letter Queue (DLQ):** ≥ 5 % de registros corruptos aislados sin detener el flujo
- [ ] Imputación de nulos y outliers: comparación **mediana vs. KNN**

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

El pipeline **no se detiene** y actualiza la capa Parquet con los datos válidos.

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
| `v0.1.0` | Modelado conceptual e Ingesta Híbrida |
| `v1.0.0` | Entrega final del proyecto |

---

## 📦 Entregables

- [ ] Repositorio versionado (`v1.0.0`) con código, configuración del orquestador y muestras en CSV, JSON y Parquet
- [ ] Blueprint de Arquitectura y Gobernanza (modelo conceptual, arquitectura, linaje, catálogo y costos Batch vs. Stream)
- [ ] Demo en vivo o video **Chaos Test** (5 min)

---

## 📄 Licencia

Proyecto académico sin fines comerciales.
