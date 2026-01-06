--Reporte de ventas  Totales
WITH Ventas AS(  
(  
--Custo
 --CREATE OR REPLACE VIEW `Ventas.VentasCustoIngresos` AS
 WITH CUSTOVENTAS AS(
    WITH temp_nombres_asesor AS (
        SELECT  
            id_usuario,
            -- Asesor   
            TRIM(CONCAT(
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(NombreAsesor), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_paterno, '-', ''), '.', '')), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_materno, '-', ''), '.', '')), ' ')) AS word), ' ')))
            AS Asesor
        FROM EXTERNAL_QUERY("terraviva-439415.us.custo", "SELECT u.id_usuario, u.nombre AS NombreAsesor, u.apellido_paterno, u.apellido_materno FROM usuario AS u")
    ),
    temp_nombres_clientes AS (
        SELECT  
            id_cliente,
            -- Cliente
            TRIM(CONCAT( 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(nombre, ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_p, '-', ''), '.', '')), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_m, '-', ''), '.', '')), ' ')) AS word), ' ')))
            AS Cliente
        FROM EXTERNAL_QUERY("terraviva-439415.us.custo", "SELECT c.id_cliente, c.nombre, c.apellido_p, c.apellido_m FROM cliente AS c")
    ),
    temp_desarrollos_marcas AS (
        SELECT
            id_desarrollo,
            nombre_desarrollo
        FROM EXTERNAL_QUERY("terraviva-439415.us.custo", "SELECT d.id_desarrollo, d.nombre_desarrollo FROM desarrollo AS d")
    ),
    estatusventas AS (
        SELECT
            id_status,
            nombre
        FROM EXTERNAL_QUERY("terraviva-439415.us.custo", "SELECT id_status, nombre FROM status_venta")
    ),
    temp_unidades AS (
        SELECT
            id_unidad,
            id_desarrollo,
            modelo,
            numero_unidad AS Unidad,
            metros_cuadrados_totales AS M2, 
            precio_metros_cuadrados,
            privada, 
            numero_etapa AS Etapa
        FROM EXTERNAL_QUERY("terraviva-439415.us.custo", "SELECT uni.id_desarrollo, uni.privada, uni.numero_etapa, uni.id_unidad, uni.numero_unidad, uni.modelo, uni.metros_cuadrados_totales, uni.precio_metros_cuadrados FROM unidades AS uni")
    ),
    temp_ventas AS (
        SELECT
            id_venta,
            id_usuario,
            id_cliente, 
            id_unidad,
            precio_venta AS PrecioVenta,
            -- Proceso    
            fecha_venta AS Proceso, 
            numero_acciones,
            aportacion_accion AS PU_Capital,
            aportacion_prim_accion AS PU_Prima,
            aportacion_accion_total AS Capital,
            aportacion_prim_accion_total AS Prima,
            total_pagado,
            saldo_total,
            numero_pagos,
            -- Finalizado
            fecha_carga_contrato,
            status_venta,
            cantidad_enganche,
            DATE(NULLIF(fecha_cierre_venta, '0000-00-01')) AS Finalizado
        FROM EXTERNAL_QUERY("terraviva-439415.us.custo", "SELECT v.id_venta, v.id_unidad, v.id_usuario, v.id_cliente, NULLIF(v.fecha_cierre_venta, '0000-00-00') AS fecha_cierre_venta, v.precio_venta, v.fecha_venta, v.numero_acciones, v.cantidad_enganche, v.aportacion_accion, v.aportacion_prim_accion, v.status_venta, v.aportacion_accion_total, v.aportacion_prim_accion_total, v.total_pagado, v.status_venta AS status, v.saldo_total, v.numero_pagos, NULLIF(v.fecha_carga_contrato, '0000-00-00') AS fecha_carga_contrato FROM venta AS v WHERE status_venta  IN (7,8) ")
    ),
    temp_fecha_ingreso AS (
        SELECT
            DISTINCT id_venta,   
            MAX(NULLIF(fecha_ingreso, '0000-00-00')) OVER(PARTITION BY id_venta) AS fecha_ingreso
        FROM EXTERNAL_QUERY("terraviva-439415.us.custo", "SELECT id_venta, NULLIF(fecha_ingreso, '0000-00-00') AS fecha_ingreso FROM ingreso")
    ),
    temp_normalizacion_nombre AS (
        SELECT 
            -- Asesor
            id_usuario,
            TRIM(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(Asesor, 'Merida', ''), 'Miami', ''), 'Cdmx', ''), 'Dam', ''), 'Interno', ''), 'Externo', '')) AS Asesor
        FROM temp_nombres_asesor
    ),
    flujo_ingreso AS (
        SELECT 
            id_venta,    
            SUM(CASE WHEN status = 1 THEN monto_ingresado ELSE 0 END) AS monto
        FROM EXTERNAL_QUERY("terraviva-439415.us.custo", 
            "SELECT id_ingreso, id_venta, id_banco, concepto, status, monto_ingresado FROM ingreso") GROUP BY id_venta 
    )
    -- Consulta final
    SELECT
        tv.id_venta,
        va.Marca,
        va.Desarrollo,
        tu.Privada,
        tu.Etapa,
        tu.Unidad,  
        tu.Modelo, 
        tu.M2 AS M2,
        IFNULL(SAFE_DIVIDE(tv.PrecioVenta, tu.M2), 0) AS PrecioM2,
        tv.PrecioVenta,
        fing.monto AS Pagado,
        tv.cantidad_enganche AS Enganche,
        tnn.Asesor,
        ts.Sucursal,
        ts.Tipo,
        ts.Equipo,
        tnc.Cliente,
        tv.Proceso,
        DATE(tv.Finalizado) AS Finalizado,
        CASE 
            WHEN tv.status_venta = 8  THEN tfi.fecha_ingreso
            ELSE NULL 
        END AS Fecha,
        DATE_DIFF(DATE(tv.Finalizado), DATE(tv.Proceso), DAY) AS Dias,
        sv.nombre AS Estatus

    FROM temp_ventas AS tv
    LEFT JOIN temp_nombres_asesor AS tna ON tv.id_usuario = tna.id_usuario
    LEFT JOIN temp_nombres_clientes AS tnc ON tv.id_cliente = tnc.id_cliente
    LEFT JOIN temp_unidades AS tu ON tv.id_unidad = tu.id_unidad

    LEFT JOIN flujo_ingreso AS fing ON fing.id_venta = tv.id_venta

    LEFT JOIN temp_desarrollos_marcas AS tdm ON tdm.id_desarrollo = tu.id_desarrollo
    LEFT JOIN temp_normalizacion_nombre AS tnn ON tnn.id_usuario = tv.id_usuario 
    LEFT JOIN temp_fecha_ingreso AS tfi ON tfi.id_venta = tv.id_venta
    LEFT JOIN estatusventas AS sv ON sv.id_status = tv.status_venta
    LEFT JOIN `Dimensiones.NombreDesarrollo` AS va ON va.id_nombre_desarrollo = tdm.nombre_desarrollo 
    LEFT JOIN `Dimensiones.NombresVendedores` AS ts ON ts.Vendedor = tnn.Asesor 
 ) SELECT * FROM CUSTOVENTAS
    WHERE Cliente NOT LIKE '%Prueba%'
    AND Cliente NOT LIKE "%Manivela%"
    AND Cliente NOT LIKE '%Oficina Dam%' 
    AND Cliente NOT LIKE 'Demo%' 
    AND Desarrollo NOT IN ('Demo','DEMO', 'Vista Esmeralda', 'Real Del Angel')
-- ;
)UNION ALL(
--DAM
 --CREATE OR REPLACE VIEW `Ventas.VentasDAMIngresos` AS
 WITH DAMVENTAS AS(
    WITH temp_nombres_asesor AS (
        SELECT  
            id_usuario,
            -- Asesor   
            TRIM(CONCAT(
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(NombreAsesor), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_paterno, '-', ''), '.', '')), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_materno, '-', ''), '.', '')), ' ')) AS word), ' ')))
            AS Asesor
        FROM EXTERNAL_QUERY("terraviva-439415.us.dam", "SELECT u.id_usuario, u.nombre AS NombreAsesor, u.apellido_paterno, u.apellido_materno FROM usuario AS u")
    ),
    temp_nombres_clientes AS (
        SELECT  
            id_cliente,
            -- Cliente
            TRIM(CONCAT( 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(nombre, ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_p, '-', ''), '.', '')), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_m, '-', ''), '.', '')), ' ')) AS word), ' ')))
            AS Cliente
        FROM EXTERNAL_QUERY("terraviva-439415.us.dam", "SELECT c.id_cliente, c.nombre, c.apellido_p, c.apellido_m FROM cliente AS c")
    ),
    temp_desarrollos_marcas AS (
        SELECT
            id_desarrollo,
            nombre_desarrollo
        FROM EXTERNAL_QUERY("terraviva-439415.us.dam", "SELECT d.id_desarrollo, d.nombre_desarrollo FROM desarrollo AS d")
    ),
    estatusventas AS (
        SELECT
            id_status,
            nombre
        FROM EXTERNAL_QUERY("terraviva-439415.us.dam", "SELECT id_status, nombre FROM status_venta")
    ),
    temp_unidades AS (
        SELECT
            id_unidad,
            id_desarrollo,
            -- Unidad, Modelo, M2, PrecioM2, PrecioVenta   
            numero_unidad AS Unidad,
            metros_cuadrados_totales AS M2, 
            precio_metros_cuadrados,
            privada, 
            numero_etapa AS Etapa
        FROM EXTERNAL_QUERY("terraviva-439415.us.dam", "SELECT uni.id_desarrollo, uni.privada, uni.numero_etapa, uni.id_unidad, uni.numero_unidad, uni.modelo, uni.metros_cuadrados_totales, uni.precio_metros_cuadrados FROM unidades AS uni")
    ),
    temp_ventas AS (
        SELECT
            id_venta,
            id_usuario,
            id_cliente, 
            id_unidad,
            precio_venta AS PrecioVenta,
            -- Proceso    
            fecha_venta AS Proceso, 
            numero_acciones,
            aportacion_accion AS PU_Capital,
            aportacion_prim_accion AS PU_Prima,
            aportacion_accion_total AS Capital,
            aportacion_prim_accion_total AS Prima,
            total_pagado,
            saldo_total,
            numero_pagos,
            -- Finalizado
            fecha_carga_contrato,
            status_venta,
            cantidad_enganche,
            DATE(NULLIF(fecha_cierre_venta, '0000-00-01')) AS Finalizado
        FROM EXTERNAL_QUERY("terraviva-439415.us.dam", "SELECT v.id_venta, v.id_unidad, v.id_usuario, v.id_cliente, NULLIF(v.fecha_cierre_venta, '0000-00-00') AS fecha_cierre_venta, v.precio_venta, v.fecha_venta, v.cantidad_enganche, v.numero_acciones, v.aportacion_accion, v.aportacion_prim_accion, v.status_venta, v.aportacion_accion_total, v.aportacion_prim_accion_total, v.total_pagado, v.status_venta AS status, v.saldo_total, v.numero_pagos, NULLIF(v.fecha_carga_contrato, '0000-00-00') AS fecha_carga_contrato FROM venta AS v WHERE status_venta  IN (7,8) ")
    ),
    temp_fecha_ingreso AS (
        SELECT
            DISTINCT id_venta,   
            MAX(NULLIF(fecha_ingreso, '0000-00-00')) OVER(PARTITION BY id_venta) AS fecha_ingreso
        FROM EXTERNAL_QUERY("terraviva-439415.us.dam", "SELECT id_venta, NULLIF(fecha_ingreso, '0000-00-00') AS fecha_ingreso FROM ingreso")
    ),
    temp_normalizacion_nombre AS (
        SELECT 
            -- Asesor
            id_usuario,
            TRIM(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(Asesor, 'Merida', ''), 'Miami', ''), 'Cdmx', ''), 'Dam', ''), 'Interno', ''), 'Externo', '')) AS Asesor
        FROM temp_nombres_asesor
    ),
    flujo_ingreso AS (
        SELECT 
            id_venta,    
            SUM(CASE WHEN status = 1 THEN monto_ingresado ELSE 0 END) AS monto
        FROM EXTERNAL_QUERY("terraviva-439415.us.dam", 
            "SELECT id_ingreso, id_venta, id_banco, concepto, status, monto_ingresado FROM ingreso") GROUP BY id_venta 
    )
    -- Consulta final
    SELECT
        tv.id_venta,
        va.Marca,
        va.Desarrollo,
        tu.Privada,
        tu.Etapa,
        tu.Unidad,  
        CASE
            WHEN va.Desarrollo = 'Parque Pimienta' THEN 'Accion'
            WHEN va.Desarrollo = 'Playaviva Apartments' THEN 'Unidad'
            WHEN va.Desarrollo = 'Business Center' THEN 'Unidad'
            WHEN va.Desarrollo = 'Centro Corporativo' THEN 'Oficina'
            ELSE NULL 
        END AS Modelo,
        CASE 
            WHEN va.Desarrollo = 'Centro Corporativo' THEN tu.M2
            WHEN va.Desarrollo = 'Parque Pimienta' THEN tv.numero_acciones
            WHEN va.Desarrollo = 'Business Center' THEN tv.numero_acciones
            WHEN va.Desarrollo = 'Playaviva Apartments' THEN tv.numero_acciones
            ELSE NULL 
        END AS M2,
        COALESCE(
            SAFE_DIVIDE(
                tv.PrecioVenta,
                CASE 
                    WHEN va.Desarrollo = 'Centro Corporativo' AND tu.M2 > 0 THEN tu.M2
                    WHEN va.Desarrollo IN ('Parque Pimienta', 'Business Center', 'Playaviva Apartments') 
                        AND tv.numero_acciones > 0 THEN tv.numero_acciones
                    ELSE NULL 
                END
            ),
            0
        ) AS PrecioM2,
        tv.PrecioVenta,
        fing.monto AS Pagado,
        tv.cantidad_enganche AS Enganche,
        tnn.Asesor,
        ts.Sucursal,
        ts.Tipo,
        ts.Equipo,
        tnc.Cliente,
        tv.Proceso,
        DATE(tv.Finalizado) AS Finalizado,
        
        CASE 
            WHEN tv.status_venta = 8  THEN tfi.fecha_ingreso
            ELSE NULL 
        END AS Fecha,

        DATE_DIFF(DATE(tv.Finalizado), DATE(tv.Proceso), 
        DAY) AS Dias,

        sv.nombre AS Estatus

    FROM temp_ventas AS tv
    LEFT JOIN temp_nombres_asesor AS tna ON tv.id_usuario = tna.id_usuario
    LEFT JOIN temp_nombres_clientes AS tnc ON tv.id_cliente = tnc.id_cliente
    LEFT JOIN temp_unidades AS tu ON tv.id_unidad = tu.id_unidad
    LEFT JOIN flujo_ingreso AS fing ON fing.id_venta = tv.id_venta

    LEFT JOIN temp_desarrollos_marcas AS tdm ON tdm.id_desarrollo = tu.id_desarrollo 
    LEFT JOIN temp_normalizacion_nombre AS tnn ON tnn.id_usuario = tv.id_usuario 
    LEFT JOIN temp_fecha_ingreso AS tfi ON tfi.id_venta = tv.id_venta
    LEFT JOIN estatusventas AS sv ON sv.id_status = tv.status_venta
    LEFT JOIN `Dimensiones.NombreDesarrollo` AS va ON va.id_nombre_desarrollo = tdm.nombre_desarrollo 
    LEFT JOIN `Dimensiones.NombresVendedores` AS ts ON ts.Vendedor = tnn.Asesor 
 )SELECT * FROM DAMVENTAS    
    WHERE Cliente NOT LIKE '%Prueba%'
    AND Cliente NOT LIKE "%Manivela%"
    AND Cliente NOT LIKE '%Oficina Dam%' 
    AND Cliente NOT LIKE 'Demo%'
    AND Desarrollo NOT IN ('Demo','DEMO', 'Vista Esmeralda', 'Real Del Angel')
-- ;
)UNION ALL(

--Terraviva
 --CREATE OR REPLACE VIEW `Ventas.VentasTerravivaIngresos` AS
 WITH TERRAVIVAVENTAS AS(
    WITH temp_nombres_asesor AS (
        SELECT  
            id_usuario,
            -- Asesor   
            TRIM(CONCAT(
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(NombreAsesor), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_paterno, '-', ''), '.', '')), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_materno, '-', ''), '.', '')), ' ')) AS word), ' ')))
            AS Asesor
        FROM EXTERNAL_QUERY("terraviva-439415.us.terraviva", "SELECT u.id_usuario, u.nombre AS NombreAsesor, u.apellido_paterno, u.apellido_materno FROM usuario AS u")
    ),
    temp_nombres_clientes AS (
        SELECT  
            id_cliente,
            -- Cliente
            TRIM(CONCAT( 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(nombre, ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_p, '-', ''), '.', '')), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_m, '-', ''), '.', '')), ' ')) AS word), ' ')))
            AS Cliente
        FROM EXTERNAL_QUERY("terraviva-439415.us.terraviva", "SELECT c.id_cliente, c.nombre, c.apellido_p, c.apellido_m FROM cliente AS c")
    ),
    temp_desarrollos_marcas AS (
        SELECT
            id_desarrollo,
            nombre_desarrollo
        FROM EXTERNAL_QUERY("terraviva-439415.us.terraviva", "SELECT d.id_desarrollo, d.nombre_desarrollo FROM desarrollo AS d")
    ),
    estatusventas AS (
        SELECT
            id_status,
            nombre
        FROM EXTERNAL_QUERY("terraviva-439415.us.terraviva", "SELECT id_status, nombre FROM status_venta")
    ),
    temp_unidades AS (
        SELECT
            id_unidad,
            id_desarrollo,
            -- Unidad, Modelo, M2, PrecioM2, PrecioVenta   
            --'Regular' AS  Modelo,
            modelo AS Modelo,
            numero_unidad AS Unidad,
            metros_cuadrados_totales AS M2, 
            precio_metros_cuadrados,
            privada, 
            numero_etapa AS Etapa
        FROM EXTERNAL_QUERY("terraviva-439415.us.terraviva", "SELECT uni.id_desarrollo, uni.privada, uni.numero_etapa, uni.id_unidad, uni.numero_unidad, uni.modelo, uni.metros_cuadrados_totales, uni.precio_metros_cuadrados FROM unidades AS uni")
    ),
    temp_ventas AS (
        SELECT
            id_venta,
            id_usuario,
            id_cliente, 
            id_unidad,
            precio_venta AS PrecioVenta,  
            --Proceso  
            fecha_venta AS Proceso, 
            numero_acciones,
            aportacion_accion AS PU_Capital,
            aportacion_prim_accion AS PU_Prima,
            aportacion_accion_total AS Capital,
            aportacion_prim_accion_total AS Prima,
            total_pagado,
            saldo_total,
            numero_pagos,
            --Finalizado
            fecha_carga_contrato,
            status_venta,
            cantidad_enganche,
            DATE(NULLIF(fecha_cierre_venta, '0000-00-01')) AS Finalizado

        FROM EXTERNAL_QUERY("terraviva-439415.us.terraviva", "SELECT v.id_venta, v.id_unidad, NULLIF(v.fecha_cierre_venta, '0000-00-00') AS fecha_cierre_venta, v.id_usuario, v.id_cliente, v.precio_venta, v.cantidad_enganche, v.fecha_venta, v.numero_acciones, v.aportacion_accion, v.aportacion_prim_accion, v.status_venta, v.aportacion_accion_total, v.aportacion_prim_accion_total, v.total_pagado, v.status_venta AS status, v.saldo_total, v.numero_pagos, NULLIF(v.fecha_carga_contrato, '0000-00-00') AS fecha_carga_contrato FROM venta AS v WHERE v.status_venta IN (7,8)")

    ),
    temp_fecha_ingreso AS (
        SELECT
            DISTINCT id_venta,   
            MAX(NULLIF(fecha_ingreso, '0000-00-00')) OVER(PARTITION BY id_venta) AS fecha_ingreso
        FROM EXTERNAL_QUERY("terraviva-439415.us.terraviva", "SELECT id_venta, NULLIF(fecha_ingreso, '0000-00-00') AS fecha_ingreso FROM ingreso")
    ), 
    temp_normalizacion_nombre AS (
        SELECT 
            -- Asesor
            id_usuario,
            TRIM(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(Asesor, 'Merida', ''), 'Miami', ''), 'Cdmx', ''), 'Dam', ''), 'Interno', ''), 'Externo', '')) AS Asesor
        FROM temp_nombres_asesor
    ),
    flujo_ingreso AS (
        SELECT 
            id_venta,    
            SUM(CASE WHEN status = 1 THEN monto_ingresado ELSE 0 END) AS monto
        FROM EXTERNAL_QUERY("terraviva-439415.us.terraviva", 
            "SELECT id_ingreso, id_venta, id_banco, concepto, status, monto_ingresado FROM ingreso") GROUP BY id_venta 
    )
    -- Consulta final
    SELECT
        tv.id_venta,
        va.Marca,
        va.Desarrollo,
        tu.Privada,
        tu.Etapa,
        tu.Unidad,  
        tu.Modelo,
        tu.M2 AS M2,
        IFNULL(SAFE_DIVIDE(tv.PrecioVenta, tu.M2), 0) AS PrecioM2,
        tv.PrecioVenta,
        fing.monto AS Pagado,
        tv.cantidad_enganche AS Enganche,
        --tv.total_pagado,
        tnn.Asesor,
        ts.Sucursal,
        ts.Tipo,
        ts.Equipo,
        tnc.Cliente,
        tv.Proceso,
        DATE(tv.Finalizado) AS Finalizado,
        
        CASE 
            WHEN tv.status_venta = 8  THEN tfi.fecha_ingreso
            ELSE NULL 
        END AS Fecha,

        DATE_DIFF(DATE(tv.Finalizado), DATE(tv.Proceso), DAY) AS Dias,

        sv.nombre AS Estatus

    FROM temp_ventas AS tv
    LEFT JOIN temp_nombres_asesor AS tna ON tv.id_usuario = tna.id_usuario
    LEFT JOIN temp_nombres_clientes AS tnc ON tv.id_cliente = tnc.id_cliente
    LEFT JOIN temp_unidades AS tu ON tv.id_unidad = tu.id_unidad
    LEFT JOIN temp_desarrollos_marcas AS tdm ON tdm.id_desarrollo = tu.id_desarrollo
    LEFT JOIN flujo_ingreso AS fing ON fing.id_venta = tv.id_venta
    LEFT JOIN temp_normalizacion_nombre AS tnn ON tnn.id_usuario = tv.id_usuario  
    LEFT JOIN temp_fecha_ingreso AS tfi ON tfi.id_venta = tv.id_venta
    LEFT JOIN estatusventas AS sv ON sv.id_status = tv.status_venta
    LEFT JOIN `Dimensiones.NombreDesarrollo` AS va ON va.id_nombre_desarrollo = tdm.nombre_desarrollo 
    LEFT JOIN `Dimensiones.NombresVendedores` AS ts ON ts.Vendedor = tnn.Asesor 
 )SELECT * FROM TERRAVIVAVENTAS
    WHERE Cliente NOT LIKE '%Prueba%'
    AND Cliente NOT LIKE "%Manivela%"
    AND Cliente NOT LIKE '%Oficina Dam%' 
    AND Cliente NOT LIKE 'Demo%'
    AND Desarrollo NOT IN ('Demo','DEMO', 'Vista Esmeralda', 'Real Del Angel')
-- ;
)UNION ALL(

--Almaviva
 --CREATE OR REPLACE VIEW `Ventas.VentasAlmavivaIngresos` AS
 WITH ALMAVIVAVENTAS AS(
    WITH temp_nombres_asesor AS (
        SELECT  
            id_usuario,
            -- Asesor   
            TRIM(CONCAT(
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(NombreAsesor), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_paterno, '-', ''), '.', '')), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_materno, '-', ''), '.', '')), ' ')) AS word), ' ')))
            AS Asesor
        FROM EXTERNAL_QUERY("terraviva-439415.us.bq_almaviva", "SELECT u.id_usuario, u.nombre AS NombreAsesor, u.apellido_paterno, u.apellido_materno FROM usuario AS u")
    ),
    temp_nombres_clientes AS (
        SELECT  
            id_cliente,
            -- Cliente
            TRIM(CONCAT( 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(nombre, ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_p, '-', ''), '.', '')), ' ')) AS word), ' '), ' ', 
                ARRAY_TO_STRING(ARRAY(SELECT INITCAP(word) FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(apellido_m, '-', ''), '.', '')), ' ')) AS word), ' ')))
            AS Cliente
        FROM EXTERNAL_QUERY("terraviva-439415.us.bq_almaviva", "SELECT c.id_cliente, c.nombre, c.apellido_p, c.apellido_m FROM cliente AS c")
    ),
    temp_desarrollos_marcas AS (
        SELECT
            id_desarrollo,
            nombre_desarrollo
        FROM EXTERNAL_QUERY("terraviva-439415.us.bq_almaviva", "SELECT d.id_desarrollo, d.nombre_desarrollo FROM desarrollo AS d")
    ),
    estatusventas AS (
        SELECT
            id_status,
            nombre
        FROM EXTERNAL_QUERY("terraviva-439415.us.bq_almaviva", "SELECT id_status, nombre FROM status_venta")
    ),
    temp_unidades AS (
        SELECT
            id_unidad,
            id_desarrollo,
            -- Unidad, Modelo, M2, PrecioM2, PrecioVenta   
            --'Regular' AS  Modelo,
            modelo AS Modelo,
            numero_unidad AS Unidad,
            metros_cuadrados_totales AS M2, 
            precio_metros_cuadrados,
            privada, 
            numero_etapa AS Etapa
        FROM EXTERNAL_QUERY("terraviva-439415.us.bq_almaviva", "SELECT uni.id_desarrollo, uni.privada, uni.numero_etapa, uni.id_unidad, uni.numero_unidad, uni.modelo, uni.metros_cuadrados_totales, uni.precio_metros_cuadrados FROM unidades AS uni")
    ),
    temp_ventas AS (
        SELECT
            id_venta,
            id_usuario,
            id_cliente, 
            id_unidad,
            precio_venta AS PrecioVenta,  
            --Proceso  
            fecha_venta AS Proceso, 
            numero_acciones,
            aportacion_accion AS PU_Capital,
            aportacion_prim_accion AS PU_Prima,
            aportacion_accion_total AS Capital,
            aportacion_prim_accion_total AS Prima,
            total_pagado,
            saldo_total,
            numero_pagos,
            --Finalizado
            fecha_carga_contrato,
            status_venta,
            cantidad_enganche,
            DATE(NULLIF(fecha_cierre_venta, '0000-00-01')) AS Finalizado

        FROM EXTERNAL_QUERY("terraviva-439415.us.bq_almaviva", "SELECT v.id_venta, v.id_unidad, v.cantidad_enganche, NULLIF(v.fecha_cierre_venta, '0000-00-00') AS fecha_cierre_venta, v.id_usuario, v.id_cliente, v.precio_venta, v.fecha_venta, v.numero_acciones, v.aportacion_accion, v.aportacion_prim_accion, v.status_venta, v.aportacion_accion_total, v.aportacion_prim_accion_total, v.total_pagado, v.status_venta AS status, v.saldo_total, v.numero_pagos, NULLIF(v.fecha_carga_contrato, '0000-00-00') AS fecha_carga_contrato FROM venta AS v WHERE v.status_venta IN (7,8)")

    ),
    temp_fecha_ingreso AS (
        SELECT
            DISTINCT id_venta,   
            MAX(NULLIF(fecha_ingreso, '0000-00-00')) OVER(PARTITION BY id_venta) AS fecha_ingreso
        FROM EXTERNAL_QUERY("terraviva-439415.us.bq_almaviva", "SELECT id_venta, NULLIF(fecha_ingreso, '0000-00-00') AS fecha_ingreso FROM ingreso")
    ), 
    temp_normalizacion_nombre AS (
        SELECT 
            -- Asesor
            id_usuario,
            TRIM(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(Asesor, 'Merida', ''), 'Miami', ''), 'Cdmx', ''), 'Dam', ''), 'Interno', ''), 'Externo', '')) AS Asesor
        FROM temp_nombres_asesor
    ),
    flujo_ingreso AS (
        SELECT 
            id_venta,    
            SUM(CASE WHEN status = 1 THEN monto_ingresado ELSE 0 END) AS monto
        FROM EXTERNAL_QUERY("terraviva-439415.us.bq_almaviva", 
            "SELECT id_ingreso, id_venta, id_banco, concepto, status, monto_ingresado FROM ingreso") GROUP BY id_venta 
    )
    -- Consulta final
    SELECT
        tv.id_venta,
        va.Marca,
        va.Desarrollo,
        tu.Privada,
        tu.Etapa,
        tu.Unidad,  
        tu.Modelo,
        tu.M2 AS M2,
        IFNULL(SAFE_DIVIDE(tv.PrecioVenta, tu.M2), 0) AS PrecioM2,
        --tv.total_pagado,
        tv.PrecioVenta,
        fing.monto AS Pagado,
        tv.cantidad_enganche AS Enganche,
        tnn.Asesor,
        ts.Sucursal,
        ts.Tipo,
        ts.Equipo,
        tnc.Cliente,
        tv.Proceso,
        DATE(tv.Finalizado) AS Finalizado, 
        -- enganche
        
        CASE 
            WHEN tv.status_venta = 8  THEN tfi.fecha_ingreso
            ELSE NULL 
        END AS Fecha,

        DATE_DIFF(DATE(tv.Finalizado), DATE(tv.Proceso), DAY) AS Dias,

        sv.nombre AS Estatus

    FROM temp_ventas AS tv
    LEFT JOIN temp_nombres_asesor AS tna ON tv.id_usuario = tna.id_usuario
    LEFT JOIN temp_nombres_clientes AS tnc ON tv.id_cliente = tnc.id_cliente
    LEFT JOIN temp_unidades AS tu ON tv.id_unidad = tu.id_unidad
    LEFT JOIN temp_desarrollos_marcas AS tdm ON tdm.id_desarrollo = tu.id_desarrollo
    LEFT JOIN flujo_ingreso AS fing ON fing.id_venta = tv.id_venta
    LEFT JOIN temp_normalizacion_nombre AS tnn ON tnn.id_usuario = tv.id_usuario  
    LEFT JOIN temp_fecha_ingreso AS tfi ON tfi.id_venta = tv.id_venta
    LEFT JOIN estatusventas AS sv ON sv.id_status = tv.status_venta
    LEFT JOIN `Dimensiones.NombreDesarrollo` AS va ON va.id_nombre_desarrollo = tdm.nombre_desarrollo 
    LEFT JOIN `Dimensiones.NombresVendedores` AS ts ON ts.Vendedor = tnn.Asesor 
 ) SELECT * FROM ALMAVIVAVENTAS
    WHERE Cliente NOT LIKE '%Prueba%'
    AND Cliente NOT LIKE "%Manivela%"
    AND Cliente NOT LIKE '%Oficina Dam%' 
    AND Cliente NOT LIKE 'Demo%'
    AND Desarrollo NOT IN ('Demo','DEMO', 'Vista Esmeralda', 'Real Del Angel')
-- ;
)
)
SELECT 
    id_venta,
    Marca,
    Privada AS Etapa,
    Desarrollo, 
    CONCAT(Privada, ' ', Unidad) AS combinado,
    Etapa AS Etapa2,
    Unidad,
    Modelo AS Tipo,
    M2 AS M2_Accion,
    PrecioM2 AS PrecioM2_Accion,
    PrecioVenta,
    Asesor,
    Sucursal AS Zona,
    Tipo AS Int_Ext,
    Equipo AS Eq,
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Cliente, 'á', 'a'), 'é', 'e'), 'í', 'i'), 'ó', 'o'), 'ú', 'u'), 'ñ', 'n') AS Cliente,
    Finalizado AS F_Venta,
    Enganche,
    Pagado AS Cobrado,
    (PrecioVenta - Pagado) AS PrecioVenta_Pagado__Saldo,
    CASE 
        WHEN MARCA = 'Almaviva'  THEN (PrecioVenta * 0.01) / 3
        WHEN MARCA = 'CO-IN'     THEN (PrecioVenta * 0.015) / 3
        WHEN MARCA = 'Custo'     THEN (PrecioVenta * 0.025) / 3
        WHEN MARCA = 'Terraviva' THEN (PrecioVenta * 0.024) / 3
        ELSE NULL
    END AS Comision_DireccionCU,
    Estatus,
    (Pagado - Enganche )AS Cobrado_Enganche


FROM Ventas 
WHERE Asesor NOT LIKE '%Atencion%'
AND EXTRACT(MONTH FROM Finalizado) = 12
AND EXTRACT(YEAR FROM Finalizado) = 2025
--296

