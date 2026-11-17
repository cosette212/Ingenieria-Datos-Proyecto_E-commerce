# Fase 3 — Escudo de validación, DLQ e imputación estadística

Versión: v0.4.0 · Integrantes: Gaudencio Isaac Gutiérrez Méndez, Juan Pablo Huerta Gonzalez, Francisco de Jesus Gonzalez Lopez y Regina Cosette Ordoñez Pinedo

## 1. Dónde encaja en el pipeline

| Orden | Script | Entrada | Salida | Fase |
|---|---|---|---|---|
| 1 | `f1_generate_chaos_data.py` | — | `data/source/` | 1 |
| 2 | `f1_ingest_batch.py` | `data/source/` | `data/raw/batch_id=.../` | 1 |
| 2 | `f1_stream_simulator.py` + `f1_ingest_stream.py` | — | `data/raw/eventos_stream/` y DLQ del stream | 1 |
| 3 | `f2_load_raw.py` | `data/raw/` | tablas `raw_*` | 2 |
| 4 | `f2_transform.py` | `raw_*` | `stg_*`, modelo 3FN, `rechazado_integridad` | 2 |
| 5 | `f3_validate.py` | `raw_*`, `categoria`, `subcategoria` | `dlq`, `clean_*` | 3 |
| 6 | `f3_impute.py` | `clean_*`, `dlq` | `clean_*` imputado, `imputacion_log` | 3 |
| 7 | `f3_export_clean.py` | `clean_*`, `dlq` | `data/clean/*.parquet` | 3 |

Capas de datos: **fuente → raw → staging → modelo 3FN (Fase 2) → capa limpia `clean_*` (Fase 3) → Parquet**.

**Por qué la Fase 3 lee `raw_*` y no `stg_*`:** el staging de la Fase 2 ya convirtió los textos inválidos en `NULL` (`'veinte'`, `'mil pesos'`) y perdió la evidencia. La DLQ necesita el valor original. Se reutilizan los mismos casts seguros (`to_int`, `to_real`, `to_date`, `to_ts`, `to_text`, importados de `f2_transform.py`) para que ambas fases interpreten los tipos igual. De la Fase 2 también se reutilizan las tablas `categoria` y `subcategoria`.

**Dos DLQ con papeles distintos:**
- *DLQ del stream* (Fase 1, archivos en `data/dlq/`): eventos tardíos bajo el watermark y líneas que no son JSON.
- *DLQ de validación* (Fase 3, tabla `dlq`): todo lo que rompe una regla de esquema, de negocio o de integridad.

## 2. Validación en doble capa

| Capa | Pregunta que responde | Ejemplo |
|---|---|---|
| **E: Esquema** | ¿El valor respeta su tipo técnico? | `edad = 'veinte'` no es entero |
| **N: Negocio** | ¿El valor respeta las reglas del negocio? | edad entre 18 y 100, montos mayores a 0 |
| **I: Integridad** | ¿Lo que referencia existe en la capa limpia? | el cliente del pedido existe |

Catálogo de reglas (`*` = **CORREGIDO**; el resto = **RECHAZADO**):

| Entidad | Esquema (E) | Negocio (N) | Integridad (I) |
|---|---|---|---|
| cliente | E01 id · E02 edad entera* · E03 fecha* | N01 duplicado · N02 edad 18–100* · N03 correo* | — |
| producto | E04 id · E05 precio numérico* | N04 duplicado · N05 precio > 0* · N06 estado* | — |
| pedido | E06 id · E07 cliente · E08 fecha · E09 monto | N07 duplicado · N08 monto > 0 · N09 estado · N10 sin líneas · N11 monto = suma de líneas | I01 cliente existe · I02 línea inválida |
| línea de pedido | E10 número · E11 producto · E12 cantidad · E13 precio | N12 cantidad > 0 · N13 precio > 0 · N14 descuento 0–1 · N15 duplicada | I03 producto existe · I04 pedido padre |
| inventario | E14 producto · E15 almacén · E16 stock · E17 reservado · E18 fecha* | N16 stock ≥ 0 · N17 reservado ≥ 0 · N18 duplicado | I05 producto existe |
| movimiento | E19 id · E20 cantidad · E21 fecha | N19 cantidad > 0 · N20 tipo · N21 duplicado | I06 inventario existe |
| evento | E22 JSON · E23 id · E24 cliente · E25 producto · E26 fecha | N22 tipo · N23 vista/carrito con producto · N24 sin cliente con sesión · N25 duplicado | I07 cliente existe · I08 producto existe |

**Dos acciones posibles:**
- **RECHAZADO:** el registro no entra a la capa limpia. Queda en la DLQ con su JSON original.
- **CORREGIDO:** el registro entra, el atributo defectuoso se pone en `NULL` y se imputa después. Queda registrado en la DLQ.

**Por qué dos acciones:**
- Los **hechos** (pedidos, líneas, inventario, movimientos, eventos) se rechazan. Un pedido es una unidad: si una de sus líneas es inválida, se rechaza el pedido completo (I02) y sus otras líneas quedan en la DLQ como I04.
- Los **atributos descriptivos de los maestros** (edad, correo, fecha de registro, precio de lista, estado) se corrigen. Cada producto aparece en unas 49 líneas de pedido; rechazar un producto por un precio mal escrito eliminaría unas 49 ventas válidas.
- Un valor **vacío no es una violación**: no va a la DLQ; si es numérico se imputa.

## 3. Trazabilidad con las reglas del modelo conceptual

| Regla del modelo | Regla implementada | Si se viola |
|---|---|---|
| R1 Todo pedido pertenece a un cliente | I01 | pedido rechazado |
| R2 Todo pedido tiene al menos una línea | N10 | pedido rechazado |
| R3 Visitante anónimo se identifica por sesión | N24 | evento rechazado |
| R4 Vista y carrito exigen producto | N23 | evento rechazado |
| R5 Línea con producto del catálogo y cantidad > 0 | I03 y N12 | línea y pedido rechazados |
| R6 Producto con inventario, uno por producto y almacén | I05 y N18 | registro de inventario rechazado |
| R7 Stock no negativo | N16 | registro de inventario rechazado |

**Limitación de R7:** solo se valida la parte "no negativo". La igualdad "stock = suma de movimientos" no se puede comprobar con estos datos sintéticos, porque el generador asigna el stock de forma independiente de los movimientos. Con datos reales se agregaría una regla de conciliación.

## 4. Dead Letter Queue (tabla `dlq`)

| Columna | Contenido |
|---|---|
| `entidad`, `clave` | qué registro es (por ejemplo `pedido`, `9101`) |
| `capa`, `regla`, `accion` | capa (esquema, negocio, integridad), código de la regla y acción |
| `detalle` | qué llegó mal (por ejemplo `edad="veinte" no es entera -> NULL (se imputa)`) |
| `registro_json` | el registro original tal como llegó |
| `ejecucion_id`, `registrado_en` | cuándo se detectó |

Una fila de la DLQ por cada regla violada. Un registro puede tener varias. Para auditar sin SQL: `data/clean/dlq_auditoria.csv` (se abre en Excel).

**Evidencia** (pega aquí tu salida de `f3_validate.py`):

| Entidad | Evaluados | Aceptados | Rechazados | Corregidos |
|---|---:|---:|---:|---:|
| cliente | 505 | 500 | 5 | 22 |
| producto | 100 | 100 | 0 | 1 |
| pedido | 2000 | 1832 | 168 | 0 |
| línea de pedido | 4956 | 4564 | 392 | 0 |
| inventario | 147 | 141 | 6 | 0 |
| movimiento de inventario | 344 | 324 | 20 | 0 |
| evento de comportamiento | [__] | [__] | [__] | [__] |

Balance: evaluados = aceptados + rechazados en todas las entidades.

*Nota de reproducibilidad: Las cifras de clientes, productos, pedidos, líneas, inventario y movimientos son deterministas (el generador usa semilla). El stream usa `--seed 42` para que la fila de eventos también se repita.*

## 5. Corrupción inyectada contra corrupción detectada

El generador de la Fase 1 inyectó **241 registros corruptos de 2 705 (8.9 %)**. El escudo los encuentra o los explica todos:

| Origen | Inyectados | Detectados como violación | No son violación |
|---|---:|---:|---|
| clientes | 36 | 27 (22 corregidos y 5 duplicados rechazados) | 9: edad vacía (3) y correo vacío (6) |
| productos | 6 | 1 | 5: categoría vacía (2, se recupera por la subcategoría), precio vacío (1) y precios extremos (2, se imputan) |
| pedidos | 188 | 168 | 20: ids de producto con espacios, válidos después de traducir el tipo |
| inventario y movimientos | 11 | 11 | 0 |
| **Total** | **241** | **207** | **34** |

Sobre los registros de origen (clientes, productos, pedidos e inventario) el escudo detecta **202 de 2 752 (7.3 %)** con problema propio, y 5 más en movimientos anidados. **Cumple el requisito de al menos 5 %.**

## 6. Imputación estadística

| Columna | Qué se imputa | Método |
|---|---|---|
| `clean_cliente.edad` | vacíos, valores corregidos por reglas | mediana (entero) |
| `clean_producto.precio_lista` | vacíos, valores corregidos por reglas, outliers | mediana |

**Outliers:** criterio de Tukey con 3 × IQR. En el precio se aplica sobre el logaritmo, porque una distribución asimétrica marcaría como "outliers" valores normales de la cola.

**Comparación** (se esconde el 20 % de los valores buenos, se imputan con cada estrategia y se mide el error; semilla 42). Detalle completo en `docs/Fase3/imputacion_comparacion.md`.

| Precio de lista (asimetría 1.47) | Valor usado | MAE | RMSE |
|---|---:|---:|---:|
| Media, con datos contaminados | 253 389.83 | 253 168.33 | 253 168.37 |
| Mediana, con datos contaminados | 185.27 | 101.29 | 145.29 |
| Media, con datos limpios | 231.15 | 111.26 | 141.03 |
| Mediana, con datos limpios | 176.92 | 100.85 | 147.60 |

| Edad (asimetría 0.32) | Valor usado | MAE | RMSE |
|---|---:|---:|---:|
| Media, con datos contaminados | 35.00 | 7.75 | 9.44 |
| Mediana, con datos contaminados | 32.00 | 7.39 | 9.13 |
| Media, con datos limpios | 32.00 | 7.39 | 9.13 |
| Mediana, con datos limpios | 32.00 | 7.39 | 9.13 |

**Justificación de la mediana:**
- Con datos contaminados la media del precio se equivoca unas 2 500 veces más que la mediana: un solo precio de 9 999 999 la arrastra a 204 306 (la mediana pasa de 175.50 a 181.09).
- En una distribución asimétrica la media queda por encima de la mediana (229.24 contra 175.50) porque la cola derecha la jala; la mediana representa mejor al producto típico.
- **Matiz honesto:** con datos limpios la media gana por RMSE (141.03 contra 147.60), porque la media minimiza el error cuadrático. La mediana gana por MAE. Pero basta un valor extremo para arruinar la media, y los datos reales siempre traen alguno.
- En la edad, casi simétrica y con pocos valores inválidos, ambas dan lo mismo (32). La mediana se mantiene por consistencia.
- **Por qué no KNN:** las columnas de esta tienda no se relacionan entre sí (el precio no depende de la marca ni de la categoría en los datos generados), así que KNN no aportaría información, añadiría una dependencia y sería más difícil de explicar. Queda como extensión.

Cada valor imputado queda en `imputacion_log` (tabla, clave, columna, motivo, valor original, valor imputado, método) y marcado en la capa limpia (`edad_imputada`, `precio_imputado`).

## 7. Prueba de caos (`f3_chaos_test.py`)

Inyecta 34 casos en un batch crudo nuevo y corre el pipeline real (`f2_load_raw`, `f2_transform`, `f3_validate`, `f3_impute`, `f3_export_clean`). Después comprueba 52 cosas: cada caso en la DLQ con su regla, cada imputación, los registros de control que deben pasar limpios y los que no deben pasar.

| Grupo | Casos |
|---|---|
| Tipos erróneos | edad `'abc'`, id `'XYZ'`, precio `'gratis'`, monto `'mucho'`, evento con fecha `'ayer'` |
| Edades | negativa (−5), menor de edad (17), extrema (250) |
| Nulos | edad vacía (se imputa) |
| Reglas de negocio | pedido sin líneas, monto negativo, monto distinto a la suma, cantidad 0, stock negativo, movimiento de tipo inválido, eventos sin producto o sin cliente ni sesión |
| Integridad | pedido de un cliente inexistente, evento de un cliente inexistente |
| Controles | registros válidos que deben pasar sin tocar la DLQ |

Por defecto restaura el estado normal al terminar. Con `--mantener` deja los datos del caos en el almacén y en Parquet (útil para la demo).

## 8. Verificación de la cadena (`f3_verificar_cadena.py`)

69 comprobaciones numéricas que prueban que cada fase usa lo que produjo la anterior: fuente contra manifiesto y huella SHA-256 (Fase 1), cuadre crudo → staging → 3FN e integridad referencial (Fase 2), y evaluados contra crudo, aceptados más rechazados, lo rechazado en la Fase 2 ausente de la capa limpia, capa limpia contenida en la 3FN, sin nulos tras imputar y Parquet igual al almacén (Fase 3).

## 9. Decisiones y límites

- **Reglas en SQL** (`sql/03_validacion_dlq.sql`): son auditables y se ejecutan dentro del almacén, igual que la Fase 2 (ELT). La estadística (mediana, Tukey, comparación) se hace en Python con pandas.
- **`clean_producto.id_subcategoria` no tiene FOREIGN KEY** hacia `subcategoria`: así la Fase 2 puede recargarse sin que la Fase 3 la bloquee. La integridad se garantiza por construcción y la verifica el script.
- **`data/curated/` (Fase 2) no se toca:** queda como evidencia de la capa normalizada. La capa limpia final es `data/clean/`.
- **Siempre se ejecuta en orden.** Al recargar la Fase 2, hay que volver a correr `f3_validate`, `f3_impute` y `f3_export_clean`.

## 10. Cómo reproducir

```
python src/f2_load_raw.py
python src/f2_transform.py
python src/f3_validate.py
python src/f3_impute.py
python src/f3_export_clean.py
python src/f3_verificar_cadena.py
python src/f3_chaos_test.py
```
