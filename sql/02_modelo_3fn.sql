-- =====================================================================
-- FASE 2 - TRANSFORM, parte 2: MODELO NORMALIZADO (3FN)
-- Crea las tablas finales con llaves primarias y foraneas, y las llena desde staging.
-- Lo que no cumple integridad (cliente o producto inexistente, etc.) NO entra:
-- se guarda en rechazado_integridad para auditoria (antecedente de la DLQ de la Fase 3).
-- Las conexiones se abren con PRAGMA foreign_keys = ON.
-- =====================================================================

-- Se borra en orden inverso a las dependencias (hijos primero)
DROP TABLE IF EXISTS rechazado_integridad;
DROP TABLE IF EXISTS evento_comportamiento;
DROP TABLE IF EXISTS movimiento_inventario;
DROP TABLE IF EXISTS inventario;
DROP TABLE IF EXISTS linea_pedido;
DROP TABLE IF EXISTS pedido;
DROP TABLE IF EXISTS cliente;
DROP TABLE IF EXISTS producto;
DROP TABLE IF EXISTS subcategoria;
DROP TABLE IF EXISTS categoria;

-- ---------------------------------------------------------------- DDL
-- 3FN: subcategoria -> categoria era una dependencia transitiva dentro de producto
--      (id_producto -> subcategoria -> categoria). Se separa en sus propias tablas.
CREATE TABLE categoria (
    id_categoria     INTEGER PRIMARY KEY,
    nombre           TEXT NOT NULL UNIQUE
);

CREATE TABLE subcategoria (
    id_subcategoria  INTEGER PRIMARY KEY,
    nombre           TEXT NOT NULL UNIQUE,
    id_categoria     INTEGER NOT NULL REFERENCES categoria (id_categoria)
);

CREATE TABLE producto (
    id_producto      INTEGER PRIMARY KEY,
    nombre           TEXT,
    id_subcategoria  INTEGER REFERENCES subcategoria (id_subcategoria),
    precio_lista     REAL,
    marca            TEXT,
    estado           TEXT
);

CREATE TABLE cliente (
    id_cliente         INTEGER PRIMARY KEY,
    nombre             TEXT,
    correo             TEXT,
    fecha_registro     TEXT,
    edad               INTEGER,
    tipo_cliente       TEXT,
    canal_adquisicion  TEXT
);

CREATE TABLE pedido (
    id_pedido    INTEGER PRIMARY KEY,
    id_cliente   INTEGER NOT NULL REFERENCES cliente (id_cliente),
    fecha_hora   TEXT,
    monto_total  REAL,
    estado       TEXT,
    metodo_pago  TEXT,
    canal_venta  TEXT
);

CREATE TABLE linea_pedido (
    id_pedido        INTEGER NOT NULL REFERENCES pedido (id_pedido),
    numero_linea     INTEGER NOT NULL,
    id_producto      INTEGER NOT NULL REFERENCES producto (id_producto),
    cantidad         INTEGER,
    precio_unitario  REAL,
    descuento        REAL,
    PRIMARY KEY (id_pedido, numero_linea)
);

CREATE TABLE inventario (
    id_inventario         INTEGER PRIMARY KEY AUTOINCREMENT,
    id_producto           INTEGER NOT NULL REFERENCES producto (id_producto),
    almacen               TEXT NOT NULL,
    stock_disponible      INTEGER,
    stock_reservado       INTEGER,
    ultima_actualizacion  TEXT,
    UNIQUE (id_producto, almacen)
);

CREATE TABLE movimiento_inventario (
    id_movimiento    TEXT PRIMARY KEY,
    id_inventario    INTEGER NOT NULL REFERENCES inventario (id_inventario),
    tipo_movimiento  TEXT,
    cantidad         INTEGER,
    fecha_hora       TEXT,
    motivo           TEXT
);

CREATE TABLE evento_comportamiento (
    id_evento          TEXT PRIMARY KEY,
    id_cliente         INTEGER REFERENCES cliente (id_cliente),
    id_producto        INTEGER REFERENCES producto (id_producto),
    tipo_evento        TEXT,
    fecha_hora_evento  TEXT,
    sesion             TEXT,
    dispositivo        TEXT,
    fuente_trafico     TEXT
);

CREATE TABLE rechazado_integridad (
    id_rechazo     INTEGER PRIMARY KEY AUTOINCREMENT,
    tabla_destino  TEXT,
    clave          TEXT,
    motivo         TEXT,
    detalle        TEXT,
    registrado_en  TEXT DEFAULT CURRENT_TIMESTAMP
);

-- ------------------------------------------------------ CATEGORIA / SUBCATEGORIA
INSERT INTO categoria (nombre)
SELECT DISTINCT categoria
FROM stg_productos
WHERE categoria IS NOT NULL
ORDER BY categoria;

INSERT OR IGNORE INTO subcategoria (nombre, id_categoria)
SELECT s.subcategoria, c.id_categoria
FROM (SELECT DISTINCT subcategoria, categoria
      FROM stg_productos
      WHERE categoria IS NOT NULL AND subcategoria IS NOT NULL) AS s
JOIN categoria AS c ON c.nombre = s.categoria
ORDER BY s.subcategoria;

-- ---------------------------------------------------------------- PRODUCTO
-- Si el id se repite, se conserva la primera aparicion.
-- Un producto sin categoria en la fuente la recupera por su subcategoria.
INSERT INTO producto (id_producto, nombre, id_subcategoria, precio_lista, marca, estado)
SELECT p.id_producto, p.nombre, sc.id_subcategoria, p.precio_lista, p.marca, p.estado
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY id_producto ORDER BY rowid) AS rn
      FROM stg_productos
      WHERE id_producto IS NOT NULL) AS p
LEFT JOIN subcategoria AS sc ON sc.nombre = p.subcategoria
WHERE p.rn = 1
ORDER BY p.id_producto;

-- ----------------------------------------------------------------- CLIENTE
INSERT INTO cliente (id_cliente, nombre, correo, fecha_registro, edad, tipo_cliente, canal_adquisicion)
SELECT c.id_cliente, c.nombre, c.correo, c.fecha_registro, c.edad, c.tipo_cliente, c.canal_adquisicion
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY id_cliente ORDER BY rowid) AS rn
      FROM stg_clientes
      WHERE id_cliente IS NOT NULL) AS c
WHERE c.rn = 1
ORDER BY c.id_cliente;

-- ------------------------------------------------------------------ PEDIDO
INSERT INTO pedido (id_pedido, id_cliente, fecha_hora, monto_total, estado, metodo_pago, canal_venta)
SELECT p.id_pedido, p.id_cliente, p.fecha_hora, p.monto_total, p.estado, p.metodo_pago, p.canal_venta
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY id_pedido ORDER BY rowid) AS rn
      FROM stg_pedidos
      WHERE id_pedido IS NOT NULL) AS p
WHERE p.rn = 1
  AND p.id_cliente IN (SELECT id_cliente FROM cliente)
ORDER BY p.id_pedido;

INSERT INTO rechazado_integridad (tabla_destino, clave, motivo, detalle)
SELECT 'pedido', CAST(p.id_pedido AS TEXT), 'cliente_inexistente_o_nulo',
       'id_cliente=' || COALESCE(CAST(p.id_cliente AS TEXT), 'NULL')
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY id_pedido ORDER BY rowid) AS rn
      FROM stg_pedidos
      WHERE id_pedido IS NOT NULL) AS p
WHERE p.rn = 1
  AND (p.id_cliente IS NULL OR p.id_cliente NOT IN (SELECT id_cliente FROM cliente));

-- ------------------------------------------------------------ LINEA_PEDIDO
-- Aqui se aplica el JOIN ROTO ya traducido: id_producto (entero) vs producto.id_producto (entero).
INSERT OR IGNORE INTO linea_pedido (id_pedido, numero_linea, id_producto, cantidad, precio_unitario, descuento)
SELECT l.id_pedido, l.numero_linea, l.id_producto, l.cantidad, l.precio_unitario, l.descuento
FROM stg_lineas_pedido AS l
WHERE l.numero_linea IS NOT NULL
  AND l.id_pedido IN (SELECT id_pedido FROM pedido)
  AND l.id_producto IN (SELECT id_producto FROM producto)
ORDER BY l.id_pedido, l.numero_linea;

INSERT INTO rechazado_integridad (tabla_destino, clave, motivo, detalle)
SELECT 'linea_pedido',
       COALESCE(CAST(l.id_pedido AS TEXT), 'NULL') || '-' || COALESCE(CAST(l.numero_linea AS TEXT), 'NULL'),
       CASE
           WHEN l.id_pedido IS NULL OR l.id_pedido NOT IN (SELECT id_pedido FROM pedido)
               THEN 'pedido_inexistente_o_rechazado'
           WHEN l.numero_linea IS NULL
               THEN 'numero_linea_nulo'
           ELSE 'producto_inexistente_o_nulo'
       END,
       'producto_id_original=' || COALESCE(CAST(l.producto_id_texto AS TEXT), 'NULL')
FROM stg_lineas_pedido AS l
WHERE l.id_pedido IS NULL
   OR l.numero_linea IS NULL
   OR l.id_pedido NOT IN (SELECT id_pedido FROM pedido)
   OR l.id_producto IS NULL
   OR l.id_producto NOT IN (SELECT id_producto FROM producto);

-- -------------------------------------------------------------- INVENTARIO
INSERT INTO inventario (id_producto, almacen, stock_disponible, stock_reservado, ultima_actualizacion)
SELECT i.id_producto, i.almacen, i.stock_disponible, i.stock_reservado, i.ultima_actualizacion
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY id_producto, almacen ORDER BY rowid) AS rn
      FROM stg_inventario
      WHERE id_producto IS NOT NULL AND almacen IS NOT NULL) AS i
WHERE i.rn = 1
  AND i.id_producto IN (SELECT id_producto FROM producto)
ORDER BY i.id_producto, i.almacen;

INSERT INTO rechazado_integridad (tabla_destino, clave, motivo, detalle)
SELECT 'inventario',
       COALESCE(CAST(i.id_producto AS TEXT), 'NULL') || '-' || COALESCE(i.almacen, 'NULL'),
       'producto_o_almacen_invalido',
       'id_producto=' || COALESCE(CAST(i.id_producto AS TEXT), 'NULL')
FROM stg_inventario AS i
WHERE i.id_producto IS NULL
   OR i.almacen IS NULL
   OR i.id_producto NOT IN (SELECT id_producto FROM producto);

-- ------------------------------------------------- MOVIMIENTO_INVENTARIO
INSERT OR IGNORE INTO movimiento_inventario (id_movimiento, id_inventario, tipo_movimiento, cantidad, fecha_hora, motivo)
SELECT m.id_movimiento, inv.id_inventario, m.tipo_movimiento, m.cantidad, m.fecha_hora, m.motivo
FROM stg_movimientos AS m
JOIN inventario AS inv
  ON inv.id_producto = m.id_producto AND inv.almacen = m.almacen
WHERE m.id_movimiento IS NOT NULL
ORDER BY m.id_movimiento;

INSERT INTO rechazado_integridad (tabla_destino, clave, motivo, detalle)
SELECT 'movimiento_inventario', COALESCE(m.id_movimiento, 'NULL'),
       'inventario_inexistente_o_id_nulo',
       'id_producto=' || COALESCE(CAST(m.id_producto AS TEXT), 'NULL') || ' almacen=' || COALESCE(m.almacen, 'NULL')
FROM stg_movimientos AS m
LEFT JOIN inventario AS inv
  ON inv.id_producto = m.id_producto AND inv.almacen = m.almacen
WHERE m.id_movimiento IS NULL OR inv.id_inventario IS NULL;

-- ------------------------------------------------- EVENTO_COMPORTAMIENTO
-- Cliente y producto son opcionales en un evento; si vienen informados deben existir.
INSERT OR IGNORE INTO evento_comportamiento
    (id_evento, id_cliente, id_producto, tipo_evento, fecha_hora_evento, sesion, dispositivo, fuente_trafico)
SELECT e.id_evento, e.id_cliente, e.id_producto, e.tipo_evento, e.fecha_hora_evento,
       e.sesion, e.dispositivo, e.fuente_trafico
FROM stg_eventos AS e
WHERE e.id_evento IS NOT NULL
  AND (e.id_cliente IS NULL OR e.id_cliente IN (SELECT id_cliente FROM cliente))
  AND (e.id_producto IS NULL OR e.id_producto IN (SELECT id_producto FROM producto))
ORDER BY e.id_evento;

INSERT INTO rechazado_integridad (tabla_destino, clave, motivo, detalle)
SELECT 'evento_comportamiento', COALESCE(e.id_evento, 'NULL'),
       'id_nulo_o_referencia_inexistente',
       'id_cliente=' || COALESCE(CAST(e.id_cliente AS TEXT), 'NULL')
       || ' id_producto=' || COALESCE(CAST(e.id_producto AS TEXT), 'NULL')
FROM stg_eventos AS e
WHERE e.id_evento IS NULL
   OR (e.id_cliente IS NOT NULL AND e.id_cliente NOT IN (SELECT id_cliente FROM cliente))
   OR (e.id_producto IS NOT NULL AND e.id_producto NOT IN (SELECT id_producto FROM producto));
