-- =====================================================================
-- FASE 2 - TRANSFORM, parte 1: capa STAGING (tipos correctos)
-- Se ejecuta DENTRO del almacen (ELT). Convierte el texto crudo a tipos reales.
-- Las funciones to_int, to_real, to_date, to_ts y to_text son "casts seguros":
--   si el valor no se puede convertir devuelven NULL en vez de romper el flujo.
--   (la validacion de reglas de negocio y la imputacion son de la Fase 3)
-- =====================================================================

DROP TABLE IF EXISTS stg_clientes;
CREATE TABLE stg_clientes AS
SELECT
    to_int(id_cliente)          AS id_cliente,
    to_text(nombre)             AS nombre,
    to_text(correo)             AS correo,
    to_date(fecha_registro)     AS fecha_registro,
    to_int(edad)                AS edad,
    to_text(tipo_cliente)       AS tipo_cliente,
    to_text(canal_adquisicion)  AS canal_adquisicion
FROM raw_clientes;

DROP TABLE IF EXISTS stg_productos;
CREATE TABLE stg_productos AS
SELECT
    to_int(id_producto)         AS id_producto,
    to_text(nombre)             AS nombre,
    to_text(categoria)          AS categoria,
    to_text(subcategoria)       AS subcategoria,
    to_real(precio_lista)       AS precio_lista,
    to_text(marca)              AS marca,
    to_text(estado)             AS estado
FROM raw_productos;

DROP TABLE IF EXISTS stg_pedidos;
CREATE TABLE stg_pedidos AS
SELECT
    to_int(id_pedido)           AS id_pedido,
    to_int(id_cliente)          AS id_cliente,
    to_ts(fecha_hora)           AS fecha_hora,
    to_real(monto_total)        AS monto_total,
    to_text(estado)             AS estado,
    to_text(metodo_pago)        AS metodo_pago,
    to_text(canal_venta)        AS canal_venta
FROM raw_pedidos;

-- 1FN: la lista anidada "lineas" de cada pedido se convierte en una fila por linea.
-- JOIN ROTO: producto_id llega como TEXTO ("105", " 105 ") y se traduce a ENTERO.
DROP TABLE IF EXISTS stg_lineas_pedido;
CREATE TABLE stg_lineas_pedido AS
SELECT
    to_int(json_extract(r.registro_json, '$.id_pedido'))       AS id_pedido,
    to_int(json_extract(l.value, '$.numero_linea'))            AS numero_linea,
    json_extract(l.value, '$.producto_id')                     AS producto_id_texto,
    to_int(json_extract(l.value, '$.producto_id'))             AS id_producto,
    to_int(json_extract(l.value, '$.cantidad'))                AS cantidad,
    to_real(json_extract(l.value, '$.precio_unitario'))        AS precio_unitario,
    to_real(json_extract(l.value, '$.descuento'))              AS descuento
FROM raw_lineas_pedido AS r,
     json_each(r.registro_json, '$.lineas') AS l;

-- 1FN: inventario -> almacenes (una fila por producto y almacen)
DROP TABLE IF EXISTS stg_inventario;
CREATE TABLE stg_inventario AS
SELECT
    to_int(json_extract(r.registro_json, '$.producto_id'))     AS id_producto,
    to_text(json_extract(a.value, '$.almacen'))                AS almacen,
    to_int(json_extract(a.value, '$.stock_disponible'))        AS stock_disponible,
    to_int(json_extract(a.value, '$.stock_reservado'))         AS stock_reservado,
    to_ts(json_extract(a.value, '$.ultima_actualizacion'))     AS ultima_actualizacion
FROM raw_inventario AS r,
     json_each(r.registro_json, '$.almacenes') AS a;

-- 1FN: almacenes -> movimientos (segundo nivel de anidamiento)
DROP TABLE IF EXISTS stg_movimientos;
CREATE TABLE stg_movimientos AS
SELECT
    to_text(json_extract(m.value, '$.id_movimiento'))          AS id_movimiento,
    to_int(json_extract(r.registro_json, '$.producto_id'))     AS id_producto,
    to_text(json_extract(a.value, '$.almacen'))                AS almacen,
    to_text(json_extract(m.value, '$.tipo'))                   AS tipo_movimiento,
    to_int(json_extract(m.value, '$.cantidad'))                AS cantidad,
    to_ts(json_extract(m.value, '$.fecha_hora'))               AS fecha_hora,
    to_text(json_extract(m.value, '$.motivo'))                 AS motivo
FROM raw_inventario AS r,
     json_each(r.registro_json, '$.almacenes') AS a,
     json_each(a.value, '$.movimientos') AS m;

DROP TABLE IF EXISTS stg_eventos;
CREATE TABLE stg_eventos AS
SELECT
    to_text(json_extract(linea_json, '$.id_evento'))           AS id_evento,
    to_int(json_extract(linea_json, '$.id_cliente'))           AS id_cliente,
    to_int(json_extract(linea_json, '$.producto_id'))          AS id_producto,
    to_text(json_extract(linea_json, '$.tipo_evento'))         AS tipo_evento,
    to_ts(json_extract(linea_json, '$.event_time'))            AS fecha_hora_evento,
    to_text(json_extract(linea_json, '$.sesion'))              AS sesion,
    to_text(json_extract(linea_json, '$.dispositivo'))         AS dispositivo,
    to_text(json_extract(linea_json, '$.fuente_trafico'))      AS fuente_trafico
FROM raw_eventos
WHERE json_valid(linea_json);
