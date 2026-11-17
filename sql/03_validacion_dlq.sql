-- =====================================================================
-- FASE 3 - ESCUDO DE VALIDACION (doble capa) + DEAD LETTER QUEUE (DLQ)
--
-- Entrada : tablas raw_* (las deja f2_load_raw.py) + categoria/subcategoria (las deja f2_transform.py)
-- Salida  : dlq, imputacion_log y la CAPA LIMPIA clean_* (se llena ya validada)
--
-- Por que se lee raw_* y no stg_*: el staging de la Fase 2 ya convirtio los textos
-- invalidos en NULL y perdio la evidencia ('veinte', 'mil pesos'). Aqui se necesita el
-- texto original para saber QUE llego mal y guardarlo en la DLQ. Se usan los mismos casts
-- seguros (to_int, to_real, ...) para que ambas fases interpreten los tipos igual.
--
-- CAPAS DE VALIDACION
--   E = ESQUEMA   : el valor respeta su tipo tecnico (entero, decimal, fecha...)
--   N = NEGOCIO   : el valor respeta reglas de negocio (edad 18-100, montos > 0, dominios...)
--   I = INTEGRIDAD: las referencias existen en la capa limpia (cliente, producto, pedido...)
--
-- ACCION SEGUN LA REGLA
--   RECHAZADO : el registro NO entra a la capa limpia; queda en la DLQ con su JSON original.
--   CORREGIDO : el registro entra, pero el atributo defectuoso se pone en NULL y luego se
--               imputa (f3_impute.py). Aplica solo a atributos descriptivos de maestros
--               (edad, correo, precio de lista...), para no perder ventas por un dato menor.
--   Un valor VACIO no es violacion: no va a la DLQ, se imputa si es numerico.
-- =====================================================================

-- ---------------------------------------------------------------- limpieza previa
DROP VIEW  IF EXISTS v3_linea;
DROP VIEW  IF EXISTS v3_lineas_por_pedido;
DROP VIEW  IF EXISTS v3_linea_base;
DROP VIEW  IF EXISTS v3_pedido;
DROP VIEW  IF EXISTS v3_evento;
DROP VIEW  IF EXISTS v3_movimiento;
DROP VIEW  IF EXISTS v3_inventario;
DROP VIEW  IF EXISTS v3_producto;
DROP VIEW  IF EXISTS v3_cliente;

DROP TABLE IF EXISTS clean_movimiento_inventario;
DROP TABLE IF EXISTS clean_evento_comportamiento;
DROP TABLE IF EXISTS clean_inventario;
DROP TABLE IF EXISTS clean_linea_pedido;
DROP TABLE IF EXISTS clean_pedido;
DROP TABLE IF EXISTS clean_producto;
DROP TABLE IF EXISTS clean_cliente;
DROP TABLE IF EXISTS imputacion_log;
DROP TABLE IF EXISTS dlq;

-- ---------------------------------------------------------------- DLQ y bitacora
CREATE TABLE dlq (
    id_dlq         INTEGER PRIMARY KEY AUTOINCREMENT,
    ejecucion_id   TEXT,
    entidad        TEXT NOT NULL,
    clave          TEXT,
    capa           TEXT NOT NULL CHECK (capa IN ('esquema', 'negocio', 'integridad')),
    regla          TEXT NOT NULL,
    accion         TEXT NOT NULL CHECK (accion IN ('RECHAZADO', 'CORREGIDO')),
    detalle        TEXT,
    registro_json  TEXT,
    registrado_en  TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE imputacion_log (
    id_log          INTEGER PRIMARY KEY AUTOINCREMENT,
    ejecucion_id    TEXT,
    tabla           TEXT NOT NULL,
    clave           TEXT NOT NULL,
    columna         TEXT NOT NULL,
    motivo          TEXT NOT NULL,
    valor_original  TEXT,
    valor_imputado  REAL,
    metodo          TEXT NOT NULL,
    registrado_en   TEXT DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------- capa limpia (DDL)
CREATE TABLE clean_cliente (
    id_cliente         INTEGER PRIMARY KEY,
    nombre             TEXT,
    correo             TEXT,
    fecha_registro     TEXT,
    edad               INTEGER,
    tipo_cliente       TEXT,
    canal_adquisicion  TEXT,
    edad_imputada      INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE clean_producto (
    id_producto      INTEGER PRIMARY KEY,
    nombre           TEXT,
    id_subcategoria  INTEGER,   -- referencia logica a subcategoria (Fase 2). Sin FOREIGN KEY a proposito:
                                -- asi la Fase 2 puede recargarse (DROP TABLE subcategoria) sin que la Fase 3 lo bloquee.
    precio_lista     REAL,
    marca            TEXT,
    estado           TEXT,
    precio_imputado  INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE clean_pedido (
    id_pedido    INTEGER PRIMARY KEY,
    id_cliente   INTEGER NOT NULL REFERENCES clean_cliente (id_cliente),
    fecha_hora   TEXT NOT NULL,
    monto_total  REAL NOT NULL,
    estado       TEXT NOT NULL,
    metodo_pago  TEXT,
    canal_venta  TEXT
);

CREATE TABLE clean_linea_pedido (
    id_pedido        INTEGER NOT NULL REFERENCES clean_pedido (id_pedido),
    numero_linea     INTEGER NOT NULL,
    id_producto      INTEGER NOT NULL REFERENCES clean_producto (id_producto),
    cantidad         INTEGER NOT NULL,
    precio_unitario  REAL NOT NULL,
    descuento        REAL NOT NULL,
    PRIMARY KEY (id_pedido, numero_linea)
);

CREATE TABLE clean_inventario (
    id_inventario         INTEGER PRIMARY KEY AUTOINCREMENT,
    id_producto           INTEGER NOT NULL REFERENCES clean_producto (id_producto),
    almacen               TEXT NOT NULL,
    stock_disponible      INTEGER NOT NULL,
    stock_reservado       INTEGER NOT NULL,
    ultima_actualizacion  TEXT,
    UNIQUE (id_producto, almacen)
);

CREATE TABLE clean_movimiento_inventario (
    id_movimiento    TEXT PRIMARY KEY,
    id_inventario    INTEGER NOT NULL REFERENCES clean_inventario (id_inventario),
    tipo_movimiento  TEXT NOT NULL,
    cantidad         INTEGER NOT NULL,
    fecha_hora       TEXT NOT NULL,
    motivo           TEXT
);

CREATE TABLE clean_evento_comportamiento (
    id_evento          TEXT PRIMARY KEY,
    id_cliente         INTEGER REFERENCES clean_cliente (id_cliente),
    id_producto        INTEGER REFERENCES clean_producto (id_producto),
    tipo_evento        TEXT NOT NULL,
    fecha_hora_evento  TEXT NOT NULL,
    sesion             TEXT,
    dispositivo        TEXT,
    fuente_trafico     TEXT
);


-- ---------------------------------------------------------------- vistas de validacion


-- ======================= CLIENTE =======================
CREATE VIEW v3_cliente AS
WITH t AS (
    SELECT rowid AS rid,
           to_text(id_cliente)        AS id_raw,
           to_int(id_cliente)         AS id_cliente,
           to_text(nombre)            AS nombre,
           to_text(correo)            AS correo,
           to_text(fecha_registro)    AS fecha_raw,
           to_date(fecha_registro)    AS fecha_registro,
           to_text(edad)              AS edad_raw,
           to_int(edad)               AS edad,
           to_text(tipo_cliente)      AS tipo_cliente,
           to_text(canal_adquisicion) AS canal_adquisicion,
           json_object('id_cliente', id_cliente, 'nombre', nombre, 'correo', correo,
                       'fecha_registro', fecha_registro, 'edad', edad,
                       'tipo_cliente', tipo_cliente,
                       'canal_adquisicion', canal_adquisicion) AS registro_json
    FROM raw_clientes
),
g AS (
    SELECT t.*, ROW_NUMBER() OVER (PARTITION BY id_cliente ORDER BY rid) AS rn FROM t
),
f AS (
    SELECT g.*,
           CASE WHEN id_cliente IS NULL THEN 1 ELSE 0 END                                   AS e01,
           CASE WHEN edad_raw IS NOT NULL AND edad IS NULL THEN 1 ELSE 0 END                AS e02,
           CASE WHEN fecha_raw IS NOT NULL AND fecha_registro IS NULL THEN 1 ELSE 0 END     AS e03,
           CASE WHEN id_cliente IS NOT NULL AND rn > 1 THEN 1 ELSE 0 END                    AS n01,
           CASE WHEN edad IS NOT NULL AND (edad < 18 OR edad > 100) THEN 1 ELSE 0 END       AS n02,
           CASE WHEN correo IS NOT NULL AND correo NOT LIKE '%_@_%._%' THEN 1 ELSE 0 END    AS n03
    FROM g
)
SELECT f.*, CASE WHEN e01 + n01 > 0 THEN 1 ELSE 0 END AS rechazar FROM f;


-- ======================= PRODUCTO =======================
CREATE VIEW v3_producto AS
WITH t AS (
    SELECT rowid AS rid,
           to_text(id_producto)   AS id_raw,
           to_int(id_producto)    AS id_producto,
           to_text(nombre)        AS nombre,
           to_text(categoria)     AS categoria,
           to_text(subcategoria)  AS subcategoria,
           to_text(precio_lista)  AS precio_raw,
           to_real(precio_lista)  AS precio_lista,
           to_text(marca)         AS marca,
           to_text(estado)        AS estado,
           json_object('id_producto', id_producto, 'nombre', nombre, 'categoria', categoria,
                       'subcategoria', subcategoria, 'precio_lista', precio_lista,
                       'marca', marca, 'estado', estado) AS registro_json
    FROM raw_productos
),
g AS (
    SELECT t.*, ROW_NUMBER() OVER (PARTITION BY id_producto ORDER BY rid) AS rn FROM t
),
f AS (
    SELECT g.*,
           CASE WHEN id_producto IS NULL THEN 1 ELSE 0 END                                  AS e04,
           CASE WHEN precio_raw IS NOT NULL AND precio_lista IS NULL THEN 1 ELSE 0 END      AS e05,
           CASE WHEN id_producto IS NOT NULL AND rn > 1 THEN 1 ELSE 0 END                   AS n04,
           CASE WHEN precio_lista IS NOT NULL AND precio_lista <= 0 THEN 1 ELSE 0 END       AS n05,
           CASE WHEN estado IS NOT NULL
                 AND estado NOT IN ('activo', 'descontinuado') THEN 1 ELSE 0 END            AS n06
    FROM g
)
SELECT f.*, CASE WHEN e04 + n04 > 0 THEN 1 ELSE 0 END AS rechazar FROM f;


-- ======================= PEDIDO =======================
-- Tolerancia de 0.50 en N11: el monto de la fuente se calculo con precios sin redondear
-- y las lineas guardan el precio redondeado a 2 decimales.
CREATE VIEW v3_pedido AS
WITH t AS (
    SELECT rowid AS rid,
           to_text(id_pedido)    AS id_raw,
           to_int(id_pedido)     AS id_pedido,
           to_text(id_cliente)   AS cliente_raw,
           to_int(id_cliente)    AS id_cliente,
           to_text(fecha_hora)   AS fecha_raw,
           to_ts(fecha_hora)     AS fecha_hora,
           to_text(monto_total)  AS monto_raw,
           to_real(monto_total)  AS monto_total,
           to_text(estado)       AS estado,
           to_text(metodo_pago)  AS metodo_pago,
           to_text(canal_venta)  AS canal_venta,
           json_object('id_pedido', id_pedido, 'id_cliente', id_cliente, 'fecha_hora', fecha_hora,
                       'monto_total', monto_total, 'estado', estado, 'metodo_pago', metodo_pago,
                       'canal_venta', canal_venta) AS registro_json
    FROM raw_pedidos
),
g AS (
    SELECT t.*, ROW_NUMBER() OVER (PARTITION BY id_pedido ORDER BY rid) AS rn FROM t
),
h AS (
    SELECT g.*, lp.n_lineas, lp.n_invalidas, lp.suma AS suma_lineas
    FROM g LEFT JOIN v3_lineas_por_pedido AS lp ON lp.id_pedido = g.id_pedido
),
f AS (
    SELECT h.*,
           CASE WHEN id_pedido IS NULL THEN 1 ELSE 0 END                                     AS e06,
           CASE WHEN id_cliente IS NULL THEN 1 ELSE 0 END                                    AS e07,
           CASE WHEN fecha_hora IS NULL THEN 1 ELSE 0 END                                    AS e08,
           CASE WHEN monto_total IS NULL THEN 1 ELSE 0 END                                   AS e09,
           CASE WHEN id_pedido IS NOT NULL AND rn > 1 THEN 1 ELSE 0 END                      AS n07,
           CASE WHEN monto_total IS NOT NULL AND monto_total <= 0 THEN 1 ELSE 0 END          AS n08,
           CASE WHEN estado IS NULL
                  OR estado NOT IN ('pendiente', 'pagado', 'enviado', 'cancelado')
                THEN 1 ELSE 0 END                                                            AS n09,
           CASE WHEN id_pedido IS NOT NULL AND COALESCE(n_lineas, 0) = 0
                THEN 1 ELSE 0 END                                                            AS n10,
           CASE WHEN monto_total IS NOT NULL AND monto_total > 0
                 AND COALESCE(n_lineas, 0) > 0 AND COALESCE(n_invalidas, 0) = 0
                 AND ABS(monto_total - suma_lineas) > 0.5
                THEN 1 ELSE 0 END                                                            AS n11,
           CASE WHEN id_cliente IS NOT NULL
                 AND id_cliente NOT IN (SELECT id_cliente FROM clean_cliente)
                THEN 1 ELSE 0 END                                                            AS i01,
           CASE WHEN COALESCE(n_invalidas, 0) > 0 THEN 1 ELSE 0 END                          AS i02
    FROM h
)
SELECT f.*,
       CASE WHEN e06 + e07 + e08 + e09 + n07 + n08 + n09 + n10 + n11 + i01 + i02 > 0
            THEN 1 ELSE 0 END AS rechazar
FROM f;

-- ======================= LINEA DE PEDIDO (con el pedido padre) =======================
CREATE VIEW v3_linea AS
WITH x AS (
    SELECT b.*,
           CASE WHEN b.id_pedido IS NULL
                  OR b.id_pedido NOT IN (SELECT id_pedido FROM clean_pedido)
                THEN 1 ELSE 0 END AS i04
    FROM v3_linea_base AS b
)
SELECT x.*, CASE WHEN lin_invalida + i04 > 0 THEN 1 ELSE 0 END AS rechazar FROM x;


-- ======================= LINEA DE PEDIDO (sin mirar al pedido padre) =======================
-- 1FN: cada pedido trae su lista de lineas dentro del JSON; json_each la desanida.
CREATE VIEW v3_linea_base AS
WITH t AS (
    SELECT r.rowid AS rid,
           CAST(l.key AS INTEGER) AS idx,
           to_int(json_extract(r.registro_json, '$.id_pedido'))          AS id_pedido,
           to_int(json_extract(l.value, '$.numero_linea'))               AS numero_linea,
           to_text(json_extract(l.value, '$.producto_id'))               AS producto_raw,
           to_int(json_extract(l.value, '$.producto_id'))                AS id_producto,
           to_text(json_extract(l.value, '$.cantidad'))                  AS cantidad_raw,
           to_int(json_extract(l.value, '$.cantidad'))                   AS cantidad,
           to_text(json_extract(l.value, '$.precio_unitario'))           AS precio_raw,
           to_real(json_extract(l.value, '$.precio_unitario'))           AS precio_unitario,
           to_text(json_extract(l.value, '$.descuento'))                 AS descuento_raw,
           to_real(json_extract(l.value, '$.descuento'))                 AS descuento,
           json_object('id_pedido', json_extract(r.registro_json, '$.id_pedido'),
                       'numero_linea', json_extract(l.value, '$.numero_linea'),
                       'producto_id', json_extract(l.value, '$.producto_id'),
                       'cantidad', json_extract(l.value, '$.cantidad'),
                       'precio_unitario', json_extract(l.value, '$.precio_unitario'),
                       'descuento', json_extract(l.value, '$.descuento')) AS registro_json
    FROM raw_lineas_pedido AS r,
         json_each(r.registro_json, '$.lineas') AS l
),
g AS (
    SELECT t.*, ROW_NUMBER() OVER (PARTITION BY id_pedido, numero_linea ORDER BY rid, idx) AS rn FROM t
),
f AS (
    SELECT g.*,
           CASE WHEN numero_linea IS NULL THEN 1 ELSE 0 END                                  AS e10,
           CASE WHEN id_producto IS NULL THEN 1 ELSE 0 END                                   AS e11,
           CASE WHEN cantidad IS NULL THEN 1 ELSE 0 END                                      AS e12,
           CASE WHEN precio_unitario IS NULL THEN 1 ELSE 0 END                               AS e13,
           CASE WHEN cantidad IS NOT NULL AND cantidad <= 0 THEN 1 ELSE 0 END                AS n12,
           CASE WHEN precio_unitario IS NOT NULL AND precio_unitario <= 0 THEN 1 ELSE 0 END  AS n13,
           CASE WHEN descuento IS NULL OR descuento < 0 OR descuento > 1 THEN 1 ELSE 0 END   AS n14,
           CASE WHEN id_pedido IS NOT NULL AND numero_linea IS NOT NULL AND rn > 1
                THEN 1 ELSE 0 END                                                            AS n15,
           CASE WHEN id_producto IS NOT NULL
                 AND id_producto NOT IN (SELECT id_producto FROM clean_producto)
                THEN 1 ELSE 0 END                                                            AS i03
    FROM g
)
SELECT f.*,
       CASE WHEN e10 + e11 + e12 + e13 + n12 + n13 + n14 + n15 + i03 > 0 THEN 1 ELSE 0 END AS lin_invalida
FROM f;

CREATE VIEW v3_lineas_por_pedido AS
SELECT id_pedido,
       COUNT(*)           AS n_lineas,
       SUM(lin_invalida)  AS n_invalidas,
       SUM(CASE WHEN lin_invalida = 0 THEN cantidad * precio_unitario * (1 - descuento) END) AS suma
FROM v3_linea_base
WHERE id_pedido IS NOT NULL
GROUP BY id_pedido;


-- ======================= INVENTARIO =======================
-- 1FN: cada producto trae una lista de almacenes dentro del JSON.
CREATE VIEW v3_inventario AS
WITH t AS (
    SELECT r.rowid AS rid,
           CAST(a.key AS INTEGER) AS idx,
           to_text(json_extract(r.registro_json, '$.producto_id'))   AS producto_raw,
           to_int(json_extract(r.registro_json, '$.producto_id'))    AS id_producto,
           to_text(json_extract(a.value, '$.almacen'))               AS almacen,
           to_text(json_extract(a.value, '$.stock_disponible'))      AS stock_raw,
           to_int(json_extract(a.value, '$.stock_disponible'))       AS stock_disponible,
           to_text(json_extract(a.value, '$.stock_reservado'))       AS reservado_raw,
           to_int(json_extract(a.value, '$.stock_reservado'))        AS stock_reservado,
           to_text(json_extract(a.value, '$.ultima_actualizacion'))  AS ult_raw,
           to_ts(json_extract(a.value, '$.ultima_actualizacion'))    AS ultima_actualizacion,
           json_object('producto_id', json_extract(r.registro_json, '$.producto_id'),
                       'almacen', json_extract(a.value, '$.almacen'),
                       'stock_disponible', json_extract(a.value, '$.stock_disponible'),
                       'stock_reservado', json_extract(a.value, '$.stock_reservado'),
                       'ultima_actualizacion', json_extract(a.value, '$.ultima_actualizacion')) AS registro_json
    FROM raw_inventario AS r,
         json_each(r.registro_json, '$.almacenes') AS a
),
g AS (
    SELECT t.*, ROW_NUMBER() OVER (PARTITION BY id_producto, almacen ORDER BY rid, idx) AS rn FROM t
),
f AS (
    SELECT g.*,
           CASE WHEN id_producto IS NULL THEN 1 ELSE 0 END                                   AS e14,
           CASE WHEN almacen IS NULL THEN 1 ELSE 0 END                                       AS e15,
           CASE WHEN stock_disponible IS NULL THEN 1 ELSE 0 END                              AS e16,
           CASE WHEN stock_reservado IS NULL THEN 1 ELSE 0 END                               AS e17,
           CASE WHEN ult_raw IS NOT NULL AND ultima_actualizacion IS NULL THEN 1 ELSE 0 END  AS e18,
           CASE WHEN stock_disponible IS NOT NULL AND stock_disponible < 0 THEN 1 ELSE 0 END AS n16,
           CASE WHEN stock_reservado IS NOT NULL AND stock_reservado < 0 THEN 1 ELSE 0 END   AS n17,
           CASE WHEN id_producto IS NOT NULL AND almacen IS NOT NULL AND rn > 1
                THEN 1 ELSE 0 END                                                            AS n18,
           CASE WHEN id_producto IS NOT NULL
                 AND id_producto NOT IN (SELECT id_producto FROM clean_producto)
                THEN 1 ELSE 0 END                                                            AS i05
    FROM g
)
SELECT f.*,
       CASE WHEN e14 + e15 + e16 + e17 + n16 + n17 + n18 + i05 > 0 THEN 1 ELSE 0 END AS rechazar
FROM f;


-- ======================= MOVIMIENTO DE INVENTARIO =======================
-- 1FN (segundo nivel): cada almacen trae su lista de movimientos.
CREATE VIEW v3_movimiento AS
WITH t AS (
    SELECT r.rowid AS rid,
           CAST(a.key AS INTEGER) AS idx_a,
           CAST(m.key AS INTEGER) AS idx_m,
           to_int(json_extract(r.registro_json, '$.producto_id'))    AS id_producto,
           to_text(json_extract(a.value, '$.almacen'))               AS almacen,
           to_text(json_extract(m.value, '$.id_movimiento'))         AS id_movimiento,
           to_text(json_extract(m.value, '$.tipo'))                  AS tipo_movimiento,
           to_text(json_extract(m.value, '$.cantidad'))              AS cantidad_raw,
           to_int(json_extract(m.value, '$.cantidad'))               AS cantidad,
           to_text(json_extract(m.value, '$.fecha_hora'))            AS fecha_raw,
           to_ts(json_extract(m.value, '$.fecha_hora'))              AS fecha_hora,
           to_text(json_extract(m.value, '$.motivo'))                AS motivo,
           json_object('producto_id', json_extract(r.registro_json, '$.producto_id'),
                       'almacen', json_extract(a.value, '$.almacen'),
                       'id_movimiento', json_extract(m.value, '$.id_movimiento'),
                       'tipo', json_extract(m.value, '$.tipo'),
                       'cantidad', json_extract(m.value, '$.cantidad'),
                       'fecha_hora', json_extract(m.value, '$.fecha_hora'),
                       'motivo', json_extract(m.value, '$.motivo')) AS registro_json
    FROM raw_inventario AS r,
         json_each(r.registro_json, '$.almacenes') AS a,
         json_each(a.value, '$.movimientos') AS m
),
g AS (
    SELECT t.*, ROW_NUMBER() OVER (PARTITION BY id_movimiento ORDER BY rid, idx_a, idx_m) AS rn FROM t
),
f AS (
    SELECT g.*,
           CASE WHEN id_movimiento IS NULL THEN 1 ELSE 0 END                                 AS e19,
           CASE WHEN cantidad IS NULL THEN 1 ELSE 0 END                                      AS e20,
           CASE WHEN fecha_hora IS NULL THEN 1 ELSE 0 END                                    AS e21,
           CASE WHEN cantidad IS NOT NULL AND cantidad <= 0 THEN 1 ELSE 0 END                AS n19,
           CASE WHEN tipo_movimiento IS NULL
                  OR tipo_movimiento NOT IN ('entrada', 'salida', 'ajuste')
                THEN 1 ELSE 0 END                                                            AS n20,
           CASE WHEN id_movimiento IS NOT NULL AND rn > 1 THEN 1 ELSE 0 END                  AS n21,
           CASE WHEN NOT EXISTS (SELECT 1 FROM clean_inventario AS ci
                                 WHERE ci.id_producto = g.id_producto AND ci.almacen = g.almacen)
                THEN 1 ELSE 0 END                                                            AS i06
    FROM g
)
SELECT f.*,
       CASE WHEN e19 + e20 + e21 + n19 + n20 + n21 + i06 > 0 THEN 1 ELSE 0 END AS rechazar
FROM f;


-- ======================= EVENTO DE COMPORTAMIENTO (stream) =======================
CREATE VIEW v3_evento AS
WITH t AS (
    SELECT rowid AS rid,
           linea_json AS registro_json,
           json_valid(linea_json) AS json_ok,
           CASE WHEN json_valid(linea_json) THEN to_text(json_extract(linea_json, '$.id_evento')) END   AS id_evento,
           CASE WHEN json_valid(linea_json) THEN to_text(json_extract(linea_json, '$.id_cliente')) END  AS cliente_raw,
           CASE WHEN json_valid(linea_json) THEN to_int(json_extract(linea_json, '$.id_cliente')) END   AS id_cliente,
           CASE WHEN json_valid(linea_json) THEN to_text(json_extract(linea_json, '$.producto_id')) END AS producto_raw,
           CASE WHEN json_valid(linea_json) THEN to_int(json_extract(linea_json, '$.producto_id')) END  AS id_producto,
           CASE WHEN json_valid(linea_json) THEN to_text(json_extract(linea_json, '$.tipo_evento')) END AS tipo_evento,
           CASE WHEN json_valid(linea_json) THEN to_text(json_extract(linea_json, '$.event_time')) END  AS event_raw,
           CASE WHEN json_valid(linea_json) THEN to_ts(json_extract(linea_json, '$.event_time')) END    AS fecha_hora_evento,
           CASE WHEN json_valid(linea_json) THEN to_text(json_extract(linea_json, '$.sesion')) END      AS sesion,
           CASE WHEN json_valid(linea_json) THEN to_text(json_extract(linea_json, '$.dispositivo')) END AS dispositivo,
           CASE WHEN json_valid(linea_json) THEN to_text(json_extract(linea_json, '$.fuente_trafico')) END AS fuente_trafico
    FROM raw_eventos
),
g AS (
    SELECT t.*, ROW_NUMBER() OVER (PARTITION BY id_evento ORDER BY rid) AS rn FROM t
),
f AS (
    SELECT g.*,
           CASE WHEN json_ok = 0 THEN 1 ELSE 0 END                                           AS e22,
           CASE WHEN json_ok = 1 AND id_evento IS NULL THEN 1 ELSE 0 END                     AS e23,
           CASE WHEN json_ok = 1 AND cliente_raw IS NOT NULL AND id_cliente IS NULL
                THEN 1 ELSE 0 END                                                            AS e24,
           CASE WHEN json_ok = 1 AND producto_raw IS NOT NULL AND id_producto IS NULL
                THEN 1 ELSE 0 END                                                            AS e25,
           CASE WHEN json_ok = 1 AND fecha_hora_evento IS NULL THEN 1 ELSE 0 END             AS e26,
           CASE WHEN json_ok = 1
                 AND (tipo_evento IS NULL
                      OR tipo_evento NOT IN ('vista', 'añadir_al_carrito', 'checkout', 'compra'))
                THEN 1 ELSE 0 END                                                            AS n22,
           CASE WHEN json_ok = 1 AND tipo_evento IN ('vista', 'añadir_al_carrito')
                 AND producto_raw IS NULL THEN 1 ELSE 0 END                                  AS n23,
           CASE WHEN json_ok = 1 AND cliente_raw IS NULL AND sesion IS NULL
                THEN 1 ELSE 0 END                                                            AS n24,
           CASE WHEN id_evento IS NOT NULL AND rn > 1 THEN 1 ELSE 0 END                      AS n25,
           CASE WHEN id_cliente IS NOT NULL
                 AND id_cliente NOT IN (SELECT id_cliente FROM clean_cliente)
                THEN 1 ELSE 0 END                                                            AS i07,
           CASE WHEN id_producto IS NOT NULL
                 AND id_producto NOT IN (SELECT id_producto FROM clean_producto)
                THEN 1 ELSE 0 END                                                            AS i08
    FROM g
)
SELECT f.*,
       CASE WHEN e22 + e23 + e24 + e25 + e26 + n22 + n23 + n24 + n25 + i07 + i08 > 0
            THEN 1 ELSE 0 END AS rechazar
FROM f;


-- =====================================================================
-- EJECUCION EN ORDEN DE DEPENDENCIA: primero los padres, despues los hijos
--   cliente, producto -> pedido -> linea_pedido
--   producto -> inventario -> movimiento_inventario
--   cliente, producto -> evento_comportamiento
-- Para cada entidad: 1) se registran en la DLQ las reglas violadas
--                    2) se carga la capa limpia con lo que no fue rechazado
-- =====================================================================


-- ---------------------------------------------------------------- CLIENTE

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'cliente', COALESCE(CAST(id_cliente AS TEXT), id_raw, '(sin id)'), 'esquema', 'E01_id_cliente_invalido', 'RECHAZADO',
       'id_cliente vacio o no entero: ' || COALESCE(id_raw, 'NULL'), registro_json
FROM v3_cliente WHERE e01 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'cliente', COALESCE(CAST(id_cliente AS TEXT), id_raw, '(sin id)'), 'esquema', 'E02_edad_no_entera', 'CORREGIDO',
       'edad="' || edad_raw || '" no es entera -> NULL (se imputa)', registro_json
FROM v3_cliente WHERE e02 = 1 AND rechazar = 0;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'cliente', COALESCE(CAST(id_cliente AS TEXT), id_raw, '(sin id)'), 'esquema', 'E03_fecha_registro_invalida', 'CORREGIDO',
       'fecha_registro="' || fecha_raw || '" invalida -> NULL', registro_json
FROM v3_cliente WHERE e03 = 1 AND rechazar = 0;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'cliente', COALESCE(CAST(id_cliente AS TEXT), id_raw, '(sin id)'), 'negocio', 'N01_id_cliente_duplicado', 'RECHAZADO',
       'id repetido; se conserva la primera aparicion', registro_json
FROM v3_cliente WHERE n01 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'cliente', COALESCE(CAST(id_cliente AS TEXT), id_raw, '(sin id)'), 'negocio', 'N02_edad_fuera_de_rango_18_100', 'CORREGIDO',
       'edad=' || edad_raw || ' fuera de 18-100 -> NULL (se imputa)', registro_json
FROM v3_cliente WHERE n02 = 1 AND rechazar = 0;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'cliente', COALESCE(CAST(id_cliente AS TEXT), id_raw, '(sin id)'), 'negocio', 'N03_correo_formato_invalido', 'CORREGIDO',
       'correo="' || correo || '" sin formato valido -> NULL', registro_json
FROM v3_cliente WHERE n03 = 1 AND rechazar = 0;


INSERT INTO clean_cliente (id_cliente, nombre, correo, fecha_registro, edad, tipo_cliente, canal_adquisicion)
SELECT id_cliente, nombre,
       CASE WHEN n03 = 1 THEN NULL ELSE correo END,
       fecha_registro,                                   -- ya es NULL si era invalida (E03)
       CASE WHEN e02 = 1 OR n02 = 1 THEN NULL ELSE edad END,
       tipo_cliente, canal_adquisicion
FROM v3_cliente
WHERE rechazar = 0
ORDER BY id_cliente;


-- ---------------------------------------------------------------- PRODUCTO

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'producto', COALESCE(CAST(id_producto AS TEXT), id_raw, '(sin id)'), 'esquema', 'E04_id_producto_invalido', 'RECHAZADO',
       'id_producto vacio o no entero: ' || COALESCE(id_raw, 'NULL'), registro_json
FROM v3_producto WHERE e04 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'producto', COALESCE(CAST(id_producto AS TEXT), id_raw, '(sin id)'), 'esquema', 'E05_precio_lista_no_numerico', 'CORREGIDO',
       'precio_lista="' || precio_raw || '" no numerico -> NULL (se imputa)', registro_json
FROM v3_producto WHERE e05 = 1 AND rechazar = 0;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'producto', COALESCE(CAST(id_producto AS TEXT), id_raw, '(sin id)'), 'negocio', 'N04_id_producto_duplicado', 'RECHAZADO',
       'id repetido; se conserva la primera aparicion', registro_json
FROM v3_producto WHERE n04 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'producto', COALESCE(CAST(id_producto AS TEXT), id_raw, '(sin id)'), 'negocio', 'N05_precio_lista_no_positivo', 'CORREGIDO',
       'precio_lista=' || precio_raw || ' no es positivo -> NULL (se imputa)', registro_json
FROM v3_producto WHERE n05 = 1 AND rechazar = 0;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'producto', COALESCE(CAST(id_producto AS TEXT), id_raw, '(sin id)'), 'negocio', 'N06_estado_fuera_de_dominio', 'CORREGIDO',
       'estado="' || estado || '" fuera de dominio -> NULL', registro_json
FROM v3_producto WHERE n06 = 1 AND rechazar = 0;


-- Se reutiliza la tabla subcategoria de la Fase 2 (3FN) para obtener id_subcategoria.
INSERT INTO clean_producto (id_producto, nombre, id_subcategoria, precio_lista, marca, estado)
SELECT p.id_producto, p.nombre, s.id_subcategoria,
       CASE WHEN p.e05 = 1 OR p.n05 = 1 THEN NULL ELSE p.precio_lista END,
       p.marca,
       CASE WHEN p.n06 = 1 THEN NULL ELSE p.estado END
FROM v3_producto AS p
LEFT JOIN subcategoria AS s ON s.nombre = p.subcategoria
WHERE p.rechazar = 0
ORDER BY p.id_producto;


-- ---------------------------------------------------------------- PEDIDO

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'pedido', COALESCE(CAST(id_pedido AS TEXT), id_raw, '(sin id)'), 'esquema', 'E06_id_pedido_invalido', 'RECHAZADO',
       'id_pedido vacio o no entero: ' || COALESCE(id_raw, 'NULL'), registro_json
FROM v3_pedido WHERE e06 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'pedido', COALESCE(CAST(id_pedido AS TEXT), id_raw, '(sin id)'), 'esquema', 'E07_id_cliente_invalido', 'RECHAZADO',
       'id_cliente vacio o no entero: ' || COALESCE(cliente_raw, 'NULL'), registro_json
FROM v3_pedido WHERE e07 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'pedido', COALESCE(CAST(id_pedido AS TEXT), id_raw, '(sin id)'), 'esquema', 'E08_fecha_hora_invalida', 'RECHAZADO',
       'fecha_hora vacia o invalida: ' || COALESCE(fecha_raw, 'NULL'), registro_json
FROM v3_pedido WHERE e08 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'pedido', COALESCE(CAST(id_pedido AS TEXT), id_raw, '(sin id)'), 'esquema', 'E09_monto_total_invalido', 'RECHAZADO',
       'monto_total vacio o no numerico: ' || COALESCE(monto_raw, 'NULL'), registro_json
FROM v3_pedido WHERE e09 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'pedido', COALESCE(CAST(id_pedido AS TEXT), id_raw, '(sin id)'), 'negocio', 'N07_id_pedido_duplicado', 'RECHAZADO',
       'id repetido; se conserva la primera aparicion', registro_json
FROM v3_pedido WHERE n07 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'pedido', COALESCE(CAST(id_pedido AS TEXT), id_raw, '(sin id)'), 'negocio', 'N08_monto_total_no_positivo', 'RECHAZADO',
       'monto_total=' || monto_raw || ' no es positivo', registro_json
FROM v3_pedido WHERE n08 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'pedido', COALESCE(CAST(id_pedido AS TEXT), id_raw, '(sin id)'), 'negocio', 'N09_estado_fuera_de_dominio', 'RECHAZADO',
       'estado=' || COALESCE(estado, 'NULL') || ' fuera de dominio', registro_json
FROM v3_pedido WHERE n09 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'pedido', COALESCE(CAST(id_pedido AS TEXT), id_raw, '(sin id)'), 'negocio', 'N10_pedido_sin_lineas_R2', 'RECHAZADO',
       'el pedido no tiene ninguna linea (R2)', registro_json
FROM v3_pedido WHERE n10 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'pedido', COALESCE(CAST(id_pedido AS TEXT), id_raw, '(sin id)'), 'negocio', 'N11_monto_distinto_a_suma_de_lineas', 'RECHAZADO',
       'monto_total=' || monto_raw || ' vs suma de lineas=' || ROUND(suma_lineas, 2), registro_json
FROM v3_pedido WHERE n11 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'pedido', COALESCE(CAST(id_pedido AS TEXT), id_raw, '(sin id)'), 'integridad', 'I01_cliente_inexistente_R1', 'RECHAZADO',
       'cliente ' || id_cliente || ' no existe en clean_cliente (R1)', registro_json
FROM v3_pedido WHERE i01 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'pedido', COALESCE(CAST(id_pedido AS TEXT), id_raw, '(sin id)'), 'integridad', 'I02_pedido_con_linea_invalida', 'RECHAZADO',
       n_invalidas || ' linea(s) invalida(s): el pedido se rechaza completo', registro_json
FROM v3_pedido WHERE i02 = 1;


INSERT INTO clean_pedido (id_pedido, id_cliente, fecha_hora, monto_total, estado, metodo_pago, canal_venta)
SELECT id_pedido, id_cliente, fecha_hora, monto_total, estado, metodo_pago, canal_venta
FROM v3_pedido
WHERE rechazar = 0
ORDER BY id_pedido;


-- ---------------------------------------------------------------- LINEA_PEDIDO

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'linea_pedido', COALESCE(CAST(id_pedido AS TEXT), 'NULL') || '-' || COALESCE(CAST(numero_linea AS TEXT), 'NULL'), 'esquema', 'E10_numero_linea_invalido', 'RECHAZADO',
       'numero_linea vacio o no entero', registro_json
FROM v3_linea WHERE e10 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'linea_pedido', COALESCE(CAST(id_pedido AS TEXT), 'NULL') || '-' || COALESCE(CAST(numero_linea AS TEXT), 'NULL'), 'esquema', 'E11_producto_id_invalido', 'RECHAZADO',
       'producto_id vacio o no entero: ' || COALESCE(producto_raw, 'NULL'), registro_json
FROM v3_linea WHERE e11 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'linea_pedido', COALESCE(CAST(id_pedido AS TEXT), 'NULL') || '-' || COALESCE(CAST(numero_linea AS TEXT), 'NULL'), 'esquema', 'E12_cantidad_invalida', 'RECHAZADO',
       'cantidad vacia o no entera: ' || COALESCE(cantidad_raw, 'NULL'), registro_json
FROM v3_linea WHERE e12 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'linea_pedido', COALESCE(CAST(id_pedido AS TEXT), 'NULL') || '-' || COALESCE(CAST(numero_linea AS TEXT), 'NULL'), 'esquema', 'E13_precio_unitario_invalido', 'RECHAZADO',
       'precio_unitario vacio o no numerico: ' || COALESCE(precio_raw, 'NULL'), registro_json
FROM v3_linea WHERE e13 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'linea_pedido', COALESCE(CAST(id_pedido AS TEXT), 'NULL') || '-' || COALESCE(CAST(numero_linea AS TEXT), 'NULL'), 'negocio', 'N12_cantidad_no_positiva_R5', 'RECHAZADO',
       'cantidad=' || cantidad_raw || ' no es mayor a 0 (R5)', registro_json
FROM v3_linea WHERE n12 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'linea_pedido', COALESCE(CAST(id_pedido AS TEXT), 'NULL') || '-' || COALESCE(CAST(numero_linea AS TEXT), 'NULL'), 'negocio', 'N13_precio_unitario_no_positivo', 'RECHAZADO',
       'precio_unitario=' || precio_raw || ' no es positivo', registro_json
FROM v3_linea WHERE n13 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'linea_pedido', COALESCE(CAST(id_pedido AS TEXT), 'NULL') || '-' || COALESCE(CAST(numero_linea AS TEXT), 'NULL'), 'negocio', 'N14_descuento_fuera_de_0_1', 'RECHAZADO',
       'descuento=' || COALESCE(descuento_raw, 'NULL') || ' fuera del rango 0-1', registro_json
FROM v3_linea WHERE n14 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'linea_pedido', COALESCE(CAST(id_pedido AS TEXT), 'NULL') || '-' || COALESCE(CAST(numero_linea AS TEXT), 'NULL'), 'negocio', 'N15_linea_duplicada', 'RECHAZADO',
       'linea repetida dentro del mismo pedido', registro_json
FROM v3_linea WHERE n15 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'linea_pedido', COALESCE(CAST(id_pedido AS TEXT), 'NULL') || '-' || COALESCE(CAST(numero_linea AS TEXT), 'NULL'), 'integridad', 'I03_producto_inexistente_R5', 'RECHAZADO',
       'producto ' || id_producto || ' no existe en clean_producto (R5)', registro_json
FROM v3_linea WHERE i03 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'linea_pedido', COALESCE(CAST(id_pedido AS TEXT), 'NULL') || '-' || COALESCE(CAST(numero_linea AS TEXT), 'NULL'), 'integridad', 'I04_pedido_padre_rechazado', 'RECHAZADO',
       'pedido padre ' || COALESCE(CAST(id_pedido AS TEXT), 'NULL') || ' rechazado o inexistente', registro_json
FROM v3_linea WHERE i04 = 1 AND lin_invalida = 0;


INSERT INTO clean_linea_pedido (id_pedido, numero_linea, id_producto, cantidad, precio_unitario, descuento)
SELECT id_pedido, numero_linea, id_producto, cantidad, precio_unitario, descuento
FROM v3_linea
WHERE rechazar = 0
ORDER BY id_pedido, numero_linea;


-- ---------------------------------------------------------------- INVENTARIO

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'inventario', COALESCE(CAST(id_producto AS TEXT), 'NULL') || '|' || COALESCE(almacen, 'NULL'), 'esquema', 'E14_producto_id_invalido', 'RECHAZADO',
       'producto_id vacio o no entero: ' || COALESCE(producto_raw, 'NULL'), registro_json
FROM v3_inventario WHERE e14 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'inventario', COALESCE(CAST(id_producto AS TEXT), 'NULL') || '|' || COALESCE(almacen, 'NULL'), 'esquema', 'E15_almacen_vacio', 'RECHAZADO',
       'almacen vacio', registro_json
FROM v3_inventario WHERE e15 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'inventario', COALESCE(CAST(id_producto AS TEXT), 'NULL') || '|' || COALESCE(almacen, 'NULL'), 'esquema', 'E16_stock_disponible_invalido', 'RECHAZADO',
       'stock_disponible vacio o no entero: ' || COALESCE(stock_raw, 'NULL'), registro_json
FROM v3_inventario WHERE e16 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'inventario', COALESCE(CAST(id_producto AS TEXT), 'NULL') || '|' || COALESCE(almacen, 'NULL'), 'esquema', 'E17_stock_reservado_invalido', 'RECHAZADO',
       'stock_reservado vacio o no entero: ' || COALESCE(reservado_raw, 'NULL'), registro_json
FROM v3_inventario WHERE e17 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'inventario', COALESCE(CAST(id_producto AS TEXT), 'NULL') || '|' || COALESCE(almacen, 'NULL'), 'esquema', 'E18_ultima_actualizacion_invalida', 'CORREGIDO',
       'ultima_actualizacion="' || ult_raw || '" invalida -> NULL', registro_json
FROM v3_inventario WHERE e18 = 1 AND rechazar = 0;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'inventario', COALESCE(CAST(id_producto AS TEXT), 'NULL') || '|' || COALESCE(almacen, 'NULL'), 'negocio', 'N16_stock_disponible_negativo_R7', 'RECHAZADO',
       'stock_disponible=' || stock_raw || ' es negativo (R7)', registro_json
FROM v3_inventario WHERE n16 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'inventario', COALESCE(CAST(id_producto AS TEXT), 'NULL') || '|' || COALESCE(almacen, 'NULL'), 'negocio', 'N17_stock_reservado_negativo', 'RECHAZADO',
       'stock_reservado=' || reservado_raw || ' es negativo', registro_json
FROM v3_inventario WHERE n17 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'inventario', COALESCE(CAST(id_producto AS TEXT), 'NULL') || '|' || COALESCE(almacen, 'NULL'), 'negocio', 'N18_producto_almacen_duplicado_R6', 'RECHAZADO',
       'par (producto, almacen) repetido (R6)', registro_json
FROM v3_inventario WHERE n18 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'inventario', COALESCE(CAST(id_producto AS TEXT), 'NULL') || '|' || COALESCE(almacen, 'NULL'), 'integridad', 'I05_producto_inexistente_R6', 'RECHAZADO',
       'producto ' || id_producto || ' no existe en clean_producto (R6)', registro_json
FROM v3_inventario WHERE i05 = 1;


INSERT INTO clean_inventario (id_producto, almacen, stock_disponible, stock_reservado, ultima_actualizacion)
SELECT id_producto, almacen, stock_disponible, stock_reservado,
       ultima_actualizacion                              -- ya es NULL si era invalida (E18)
FROM v3_inventario
WHERE rechazar = 0
ORDER BY id_producto, almacen;


-- ---------------------------------------------------------------- MOVIMIENTO_INVENTARIO

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'movimiento_inventario', COALESCE(id_movimiento, '(sin id)'), 'esquema', 'E19_id_movimiento_vacio', 'RECHAZADO',
       'id_movimiento vacio', registro_json
FROM v3_movimiento WHERE e19 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'movimiento_inventario', COALESCE(id_movimiento, '(sin id)'), 'esquema', 'E20_cantidad_invalida', 'RECHAZADO',
       'cantidad vacia o no entera: ' || COALESCE(cantidad_raw, 'NULL'), registro_json
FROM v3_movimiento WHERE e20 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'movimiento_inventario', COALESCE(id_movimiento, '(sin id)'), 'esquema', 'E21_fecha_hora_invalida', 'RECHAZADO',
       'fecha_hora vacia o invalida: ' || COALESCE(fecha_raw, 'NULL'), registro_json
FROM v3_movimiento WHERE e21 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'movimiento_inventario', COALESCE(id_movimiento, '(sin id)'), 'negocio', 'N19_cantidad_no_positiva', 'RECHAZADO',
       'cantidad=' || cantidad_raw || ' no es mayor a 0', registro_json
FROM v3_movimiento WHERE n19 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'movimiento_inventario', COALESCE(id_movimiento, '(sin id)'), 'negocio', 'N20_tipo_fuera_de_dominio', 'RECHAZADO',
       'tipo=' || COALESCE(tipo_movimiento, 'NULL') || ' fuera de dominio (entrada, salida, ajuste)', registro_json
FROM v3_movimiento WHERE n20 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'movimiento_inventario', COALESCE(id_movimiento, '(sin id)'), 'negocio', 'N21_id_movimiento_duplicado', 'RECHAZADO',
       'id_movimiento repetido', registro_json
FROM v3_movimiento WHERE n21 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'movimiento_inventario', COALESCE(id_movimiento, '(sin id)'), 'integridad', 'I06_inventario_inexistente', 'RECHAZADO',
       'inventario (' || COALESCE(CAST(id_producto AS TEXT), 'NULL') || ', ' || COALESCE(almacen, 'NULL') || ') no existe o fue rechazado', registro_json
FROM v3_movimiento WHERE i06 = 1;


INSERT INTO clean_movimiento_inventario (id_movimiento, id_inventario, tipo_movimiento, cantidad, fecha_hora, motivo)
SELECT m.id_movimiento, ci.id_inventario, m.tipo_movimiento, m.cantidad, m.fecha_hora, m.motivo
FROM v3_movimiento AS m
JOIN clean_inventario AS ci ON ci.id_producto = m.id_producto AND ci.almacen = m.almacen
WHERE m.rechazar = 0
ORDER BY m.id_movimiento;


-- ---------------------------------------------------------------- EVENTO_COMPORTAMIENTO

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'evento_comportamiento', COALESCE(id_evento, '(sin id)'), 'esquema', 'E22_linea_no_es_json', 'RECHAZADO',
       'la linea no es JSON valido', registro_json
FROM v3_evento WHERE e22 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'evento_comportamiento', COALESCE(id_evento, '(sin id)'), 'esquema', 'E23_id_evento_vacio', 'RECHAZADO',
       'id_evento vacio', registro_json
FROM v3_evento WHERE e23 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'evento_comportamiento', COALESCE(id_evento, '(sin id)'), 'esquema', 'E24_id_cliente_invalido', 'RECHAZADO',
       'id_cliente no entero: ' || cliente_raw, registro_json
FROM v3_evento WHERE e24 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'evento_comportamiento', COALESCE(id_evento, '(sin id)'), 'esquema', 'E25_producto_id_invalido', 'RECHAZADO',
       'producto_id no entero: ' || producto_raw, registro_json
FROM v3_evento WHERE e25 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'evento_comportamiento', COALESCE(id_evento, '(sin id)'), 'esquema', 'E26_event_time_invalido', 'RECHAZADO',
       'event_time vacio o invalido: ' || COALESCE(event_raw, 'NULL'), registro_json
FROM v3_evento WHERE e26 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'evento_comportamiento', COALESCE(id_evento, '(sin id)'), 'negocio', 'N22_tipo_evento_fuera_de_dominio', 'RECHAZADO',
       'tipo_evento=' || COALESCE(tipo_evento, 'NULL') || ' fuera de dominio', registro_json
FROM v3_evento WHERE n22 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'evento_comportamiento', COALESCE(id_evento, '(sin id)'), 'negocio', 'N23_vista_o_carrito_sin_producto_R4', 'RECHAZADO',
       'tipo ' || tipo_evento || ' exige producto (R4)', registro_json
FROM v3_evento WHERE n23 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'evento_comportamiento', COALESCE(id_evento, '(sin id)'), 'negocio', 'N24_evento_sin_cliente_ni_sesion_R3', 'RECHAZADO',
       'sin cliente y sin sesion (R3)', registro_json
FROM v3_evento WHERE n24 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'evento_comportamiento', COALESCE(id_evento, '(sin id)'), 'negocio', 'N25_id_evento_duplicado', 'RECHAZADO',
       'id_evento repetido', registro_json
FROM v3_evento WHERE n25 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'evento_comportamiento', COALESCE(id_evento, '(sin id)'), 'integridad', 'I07_cliente_inexistente', 'RECHAZADO',
       'cliente ' || id_cliente || ' no existe en clean_cliente', registro_json
FROM v3_evento WHERE i07 = 1;

INSERT INTO dlq (entidad, clave, capa, regla, accion, detalle, registro_json)
SELECT 'evento_comportamiento', COALESCE(id_evento, '(sin id)'), 'integridad', 'I08_producto_inexistente', 'RECHAZADO',
       'producto ' || id_producto || ' no existe en clean_producto', registro_json
FROM v3_evento WHERE i08 = 1;


INSERT INTO clean_evento_comportamiento
    (id_evento, id_cliente, id_producto, tipo_evento, fecha_hora_evento, sesion, dispositivo, fuente_trafico)
SELECT id_evento, id_cliente, id_producto, tipo_evento, fecha_hora_evento, sesion, dispositivo, fuente_trafico
FROM v3_evento
WHERE rechazar = 0
ORDER BY id_evento;
