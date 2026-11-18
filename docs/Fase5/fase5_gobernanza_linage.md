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

**Escenario:** Se renombra o elimina la columna `precio_unitario` del archivo de origen `lineas_pedido.json`.

| Capa | Impacto observado |
|---|---|
| Ingesta y Raw | La carga continúa sin errores porque los registros JSON se almacenan como texto. |
| Staging | `json_extract` devuelve `NULL` para `precio_unitario`. |
| Modelo 3FN | Las 4,907 líneas conservan sus registros, pero su precio queda en `NULL`. |
| Escudo de Validación | Se rechazan 4,956 líneas (regla E13) y 1,989 pedidos relacionados (regla I02), enviándolos a la DLQ. |
| Capa Limpia | Las tablas `clean_linea_pedido` y `clean_pedido` quedan vacías. |
| Otras entidades | Clientes, productos y otras entidades no se ven afectados por este cambio. |
| Capa Analítica | La capa `data/curated/` conserva las líneas con precio nulo, mientras que los Parquet limpios de pedidos y líneas quedan vacíos. |

**Conclusión:** La arquitectura permite detectar y aislar los efectos del cambio sin detener la ingesta. Sin embargo, la pérdida del campo afecta directamente a las líneas de pedido y a los pedidos relacionados, demostrando la importancia del linaje de datos y las reglas de validación.

---

## 2. Catálogo de Datos

* **Vista de Referencia (Negocio):** Control de dominios críticos (`clientes`, `productos`, `pedidos`, `eventos_stream`).
* **Vista Operativa (Técnica):** Metadatos de almacenamiento (SQLite y Parquet), particionamiento por fecha y políticas de acceso `read-only` / `read-write`.

---

## 3. Versionado Semántico
Release final consolidada bajo el estándar **SemVer** en la etiqueta `v1.0.0`.