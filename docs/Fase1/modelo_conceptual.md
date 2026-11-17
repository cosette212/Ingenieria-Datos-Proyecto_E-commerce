# Modelo Conceptual de Datos — E-Commerce

**Materia:** Ingeniería de Datos
**Proyecto:** Proyecto Final (E-Commerce)
**Integrantes:** Gaudencio Isaac Gutiérrez Méndez, Francisco de Jesus Gonzalez Lopez, Juan Pablo Huerta Gonzalez y Regina Cosette Ordoñez Pinedo
**Fecha:** 11 de noviembre de 2026
**Versión del modelo:** v0.1.0
**Fase:** 1 — Modelado conceptual

![Modelo conceptual E-Commerce](modelo_conceptual_ecommerce.png)

## 1. Contexto del negocio
Tienda en línea con catálogo de varias categorías (por ejemplo electrónica, hogar, ropa y deportes). Busca unificar los datos de pedidos históricos (batch) con el comportamiento de los usuarios en el sitio (streaming) para mejorar la conversión y evitar quiebres de stock.

## 2. Entidades y atributos
El identificador de cada entidad va en **negrita**.

- **CLIENTE:** persona que compra, registrada o como invitada (sin crear cuenta). Atributos: **código de cliente**, nombre, correo electrónico, fecha de registro, edad, tipo de cliente (registrado / invitado), canal de adquisición (orgánico, pago, referido).
- **PRODUCTO:** artículo a la venta. Atributos: **código de producto**, nombre, categoría, subcategoría, precio de lista, marca, estado (activo / descontinuado).
- **PEDIDO:** transacción de compra, el hecho central del negocio. Atributos: **código de pedido**, fecha y hora, monto total, estado (pendiente, pagado, enviado, cancelado), método de pago, canal de venta (web, app móvil, tienda física).
- **LÍNEA DE PEDIDO:** detalle de cada producto dentro de un pedido. Atributos: **número de línea** (se identifica junto con su pedido), cantidad, precio unitario al momento de la compra, descuento aplicado.
- **EVENTO DE COMPORTAMIENTO:** acción del usuario en el sitio, de alta frecuencia. Atributos: **código de evento**, tipo de evento (vista, añadir al carrito, checkout, compra), fecha y hora, sesión del usuario, dispositivo (móvil, escritorio, tableta), fuente de tráfico (orgánico, pago, directo).
- **INVENTARIO:** stock actual de cada producto en cada almacén. Atributos: **código de inventario**, almacén o ubicación, stock disponible, stock reservado, fecha de última actualización.
- **MOVIMIENTO DE INVENTARIO:** historial auditable de cambios de stock. Atributos: **código de movimiento**, tipo de movimiento (entrada, salida, ajuste), cantidad, fecha y hora, motivo (reposición, venta, devolución, merma).

## 3. Relaciones y cardinalidades

| Relación | Cardinalidad | Lectura |
|---|---|---|
| CLIENTE **realiza** PEDIDO | 1 a N | Un pedido pertenece a uno y solo un cliente. Un cliente realiza cero o muchos pedidos. |
| PEDIDO **contiene** LÍNEA DE PEDIDO | 1 a N | Una línea pertenece a un solo pedido. Un pedido contiene una o muchas líneas (mínimo una). |
| PRODUCTO **aparece en** LÍNEA DE PEDIDO | 1 a N | Una línea referencia a un solo producto. Un producto aparece en cero o muchas líneas. |
| CLIENTE **genera** EVENTO | 1 a N (opcional) | Un evento tiene cero o un cliente (el visitante anónimo no tiene). Un cliente genera cero o muchos eventos. |
| PRODUCTO **recibe** EVENTO | 1 a N (opcional) | Un evento tiene cero o un producto (el checkout no tiene). Un producto recibe cero o muchos eventos. |
| PRODUCTO **tiene** INVENTARIO | 1 a N | Un registro de inventario pertenece a un solo producto. Un producto tiene uno o muchos registros (uno por almacén). |
| INVENTARIO **registra** MOVIMIENTO | 1 a N | Un movimiento afecta a un solo inventario. Un inventario registra cero o muchos movimientos. |

## 4. Reglas de negocio
Son los post-its del diagrama. Cuando una regla se viola, el registro queda en la Dead Letter Queue (Fase 3): se rechaza o, en los atributos descriptivos de los maestros, se corrige e imputa, siempre con registro en la DLQ.

| # | Regla | Relación que rige |
|---|---|---|
| R1 | Todo pedido pertenece a un cliente. | CLIENTE realiza PEDIDO |
| R2 | Todo pedido debe tener al menos una línea de pedido. | PEDIDO contiene LÍNEA |
| R3 | Un visitante anónimo se identifica por sesión, no por código de cliente. | CLIENTE genera EVENTO |
| R4 | Un evento puede no tener producto (ej. checkout), pero las vistas y añadir al carrito sí deben tenerlo. | PRODUCTO recibe EVENTO |
| R5 | Toda línea referencia un producto del catálogo. La cantidad debe ser mayor a 0. El precio unitario puede diferir del precio de lista. | PRODUCTO aparece en LÍNEA |
| R6 | Todo producto tiene al menos un registro de inventario (solo uno por producto y almacén). | PRODUCTO tiene INVENTARIO |
| R7 | El stock disponible no puede ser negativo (salvo ajuste auditado). En principio equivale a la suma de los movimientos; con datos sintéticos esa igualdad no se puede comprobar (ver [Fase 3](../Fase3/fase3_validacion_dlq_imputacion.md)). | INVENTARIO registra MOVIMIENTO |

## 5. Decisiones de diseño
- **EVENTO separado de PEDIDO:** tienen distinta granularidad y frecuencia. El pedido es un hecho financiero consolidado; el evento es una acción de comportamiento. Mezclarlos impediría analizar el embudo (vista, carrito, checkout, compra).
- **INVENTARIO separado de MOVIMIENTO:** el inventario es el estado actual y el movimiento es el historial auditable.
- **Almacén como atributo de INVENTARIO, no como entidad:** por eso un producto puede tener varios registros de inventario (relación 1 a N).
- **Sin relación directa PRODUCTO – MOVIMIENTO:** se llega a través de INVENTARIO, para evitar redundancia.
- **Invitado y visitante anónimo:** CLIENTE incluye a quien compra sin cuenta (invitado), que sí tiene código. El visitante que solo navega no es CLIENTE y se identifica por la sesión; por eso el cliente es opcional en el evento.
- **Categoría y subcategoría como atributos de PRODUCTO:** es una decisión conceptual. En la Fase 2 se separan en tablas propias para cumplir la 3FN (ver `/Fase 2/fase2_elt_normalizacion_parquet.md`).
- **Monto total del pedido:** es un dato derivado de las líneas; la Fase 3 valida que coincida.

## 6. Trazabilidad con las fuentes
La correspondencia entre cada archivo fuente y estas entidades está en [arquitectura_ingesta.md](arquitectura_ingesta.md), sección 3.

## 7. Notación utilizada
Notación **Crow's Foot (pata de gallo)**, estándar para el modelado entidad-relación.

- `‖` uno y solo uno
- `○|` cero o uno
- `○<` cero o muchos
- `|<` uno o muchos

Archivos del diagrama en `docs/Fase1/`: `modelo_conceptual_ecommerce.drawio.xml` (editable) y `modelo_conceptual_ecommerce.drawio.png` (imagen).