# Arquitectura de Ingesta — E-Commerce (Fase 1)

Versión: v0.2.0 · Integrantes: Gaudencio Isaac Gutiérrez Méndez, Juan Pablo Huerta, Francisco de Jesus Gonzalez Lopez y Regina Cosette Ordoñez Pinedo

## 1. Paradigma elegido: ELT

La arquitectura opera bajo **ELT** (Extract, Load, Transform):

1. **Extract:** se leen los archivos fuente (CSV y JSON) y los eventos del stream.
2. **Load:** se aterrizan **tal cual, sin limpiar**, en la zona cruda `data/raw/` (todo como texto, con metadatos `_batch_id`, `_ingest_ts`, `_source_file`).
3. **Transform:** la limpieza, la traducción de tipos, la normalización y las validaciones se hacen **después**, ya dentro del almacén analítico (Fase 2 y Fase 3).

**Por qué ELT y no ETL:**
- Los datos crudos quedan intactos: si una regla de limpieza estaba mal, se puede volver a transformar sin volver a pedir los datos al origen.
- Los datos corruptos no se pierden: se conservan para auditoría y alimentan la DLQ.
- La ingesta es simple y rápida, y no se rompe por un dato mal formado (todo entra como texto).
- La transformación aprovecha la capacidad de cómputo del almacén destino y se puede ejecutar de nuevo cuando cambien las reglas.

## 2. Mapa de ingesta

| Vía | Origen | Frecuencia | Destino crudo | Script |
|---|---|---|---|---|
| Batch | `clientes.csv`, `productos.csv`, `pedidos.csv`, `lineas_pedido.json`, `inventario.json` | Cierre nocturno (23:00) | `data/raw/batch_id=.../` | `src/ingest_batch.py` |
| Stream (micro-batch) | Eventos de comportamiento (JSON lines) | Continua (archivo cada ~2 s) | `data/raw/eventos_stream/` | `src/ingest_stream.py` |

Datos de prueba generados por `src/generate_chaos_data.py` (semilla fija, reproducibles).

## 3. Trazabilidad: fuente → modelo conceptual

| Archivo fuente | Entidad del modelo | Campos fuente que alimentan sus atributos |
|---|---|---|
| `clientes.csv` | CLIENTE | `id_cliente` (código), `nombre`, `correo`, `fecha_registro`, `edad`, `tipo_cliente`, `canal_adquisicion` |
| `productos.csv` | PRODUCTO | `id_producto` (código), `nombre`, `categoria`, `subcategoria`, `precio_lista`, `marca`, `estado` |
| `pedidos.csv` | PEDIDO | `id_pedido` (código), `fecha_hora`, `monto_total`, `estado`, `metodo_pago`, `canal_venta`; `id_cliente` expresa la relación CLIENTE realiza PEDIDO |
| `lineas_pedido.json` | LÍNEA DE PEDIDO | `numero_linea`, `cantidad`, `precio_unitario`, `descuento`; `id_pedido` y `producto_id` expresan las relaciones con PEDIDO y PRODUCTO |
| `inventario.json` | INVENTARIO | `almacen`, `stock_disponible`, `stock_reservado`, `ultima_actualizacion`; `producto_id` expresa la relación con PRODUCTO |
| `inventario.json` (lista `movimientos`) | MOVIMIENTO DE INVENTARIO | `id_movimiento`, `tipo`, `cantidad`, `fecha_hora`, `motivo` |
| Stream de eventos | EVENTO DE COMPORTAMIENTO | `id_evento`, `tipo_evento`, `event_time` (fecha y hora), `sesion`, `dispositivo`, `fuente_trafico`; `id_cliente` y `producto_id` expresan las relaciones opcionales |

Nota: el "Código de inventario" no viene en la fuente; se generará como clave sustituta en la Fase 2.

## 4. Caos controlado inyectado en los datos

Se inyecta aproximadamente **9 % de registros corruptos** (el enunciado pide mínimo 5 %): nulos, outliers (edad 150–999, precio 9 999 999), edades negativas, tipos mezclados (`"veinte"`, `"gratis"`, `"N/D"`), fechas inválidas, duplicados, pedidos sin líneas, productos inexistentes, cantidades en cero o negativas y stock negativo.

**Join roto deliberado:** `producto_id` llega como **texto** (`"100"`, a veces `" 100 "`) en los JSON, y `id_producto` es **numérico** (`100`) en `productos.csv`. Ejecutar `src/demo_join_roto.py` muestra que el join directo falla y que funciona después de traducir el tipo.

## 5. Watermarking en la vía stream

- **Watermark = mayor `event_time` visto − retraso permitido** (60 s en la demo).
- Evento con `event_time` igual o posterior al watermark → **aceptado**.
- Evento con `event_time` anterior al watermark → **rezagado**, va a `data/dlq/eventos_rezagados.jsonl` con el motivo y los segundos de retraso.
- JSON roto o sin fecha válida → **corrupto**, va a `data/dlq/eventos_corruptos.jsonl`.
- El flujo nunca se detiene por un evento malo.

Se eligió 60 s porque los fallos de red cortos (segundos) son comunes y conviene aceptarlos, mientras que esperar más tiempo retrasa los resultados y consume memoria.

## 6. Justificación de costos: Batch vs servidor 24/7

| | Servidor 24/7 | Batch nocturno |
|---|---|---|
| Horas de cómputo al mes | 24 × 30 = **720 h** | 1 × 30 = **30 h** |
| Costo con precio ilustrativo de 0.10 USD/h | 72.00 USD | 3.00 USD |
| Reducción | — | **≈ 95.8 %** menos horas de cómputo |

*Los precios son ilustrativos: reemplázalos por los de tu proveedor o por tu propio cálculo. La reducción de horas (30 / 720 = 4.2 %) no depende del precio.*

**Criterio de decisión:** el batch se usa para datos que se consultan al día siguiente (clientes, productos, pedidos, inventario), así que no justifican cómputo continuo. El stream se reserva para eventos de comportamiento, donde la frescura tiene valor (por ejemplo, detectar abandono de carrito). Aun así se implementa como micro-batch para reducir el costo frente a un stream puro.

## 7. Decisiones de diseño de la ingesta

- Todo se lee como texto en la zona cruda (`dtype=str`): no se adivinan tipos hasta la transformación.
- Cada corrida batch genera una carpeta nueva con `manifest.json` (conteos, SHA-256, estado, duración): permite auditar y reintentar.
- Un archivo fallido no detiene a los demás; el script termina con código 1 para que el orquestador (Fase 4) pueda activar el **Retry**.
- Los micro-lotes del stream se escriben como `.tmp` y se renombran al terminar, para no leer archivos a medias.
