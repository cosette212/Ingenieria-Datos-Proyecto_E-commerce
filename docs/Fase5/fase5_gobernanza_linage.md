#  Fase 5 · Gobernanza, Linaje y Versionado

Proyecto: **Arquitectura de Datos Híbrida y Resiliente para E-commerce**  
Versión: `v1.0.0`

---

## 1. Diagrama de Data Lineage y Análisis de Impacto

### Flujo de Linaje
El linaje de datos rastrea el ciclo de vida completo:
1. **Ingesta:** Archivos crudos en `data/source/` y eventos de streaming.
2. **Capa Cruda (`raw_*`):** Copia fiel en formato texto (`TEXT`) para evitar fallos de tipos.
3. **Capa Staging (`stg_*`):** Transformación inicial (1FN), desanidado JSON y resolución del *Join Roto* de IDs.
4. **Modelo 3FN:** 9 tablas relacionales normalizadas en SQLite con llaves PK/FK.
5. **Capa Limpia / Analítica (`data/clean/` y `data/curated/`):** Exportación a Apache Parquet con validaciones y manejo de errores mediante la **DLQ**.

### Análisis de Impacto
> *Si se renombra o elimina la columna `precio_unitario` en el origen:*
* **Staging:** Las consultas SQL fallan o devuelven nulos.
* **Modelo 3FN:** Pérdida de integridad en cálculos financieros.
* **Escudo de Validación:** El 100% de los registros fallan y van directo a la **DLQ**.
* **Capa Analítica:** Reportes en Parquet incompletos o vacíos.

---

## 2. Catálogo de Datos

* **Vista de Referencia (Negocio):** Control de dominios críticos (`clientes`, `productos`, `pedidos`, `eventos_stream`).
* **Vista Operativa (Técnica):** Metadatos de almacenamiento (SQLite y Parquet), particionamiento por fecha y políticas de acceso `read-only` / `read-write`.

---

## 3. Versionado Semántico
Release final consolidada bajo el estándar **SemVer** en la etiqueta `v1.0.0`.