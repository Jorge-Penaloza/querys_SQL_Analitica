#Matriz Temporal
DROP TABLE IF EXISTS datos_temporales.sug_temp_jpg ;
CREATE TABLE datos_temporales.sug_temp_jpg (
 id_tienda INT(11),	
 sku VARCHAR(25),
 formato CHAR(5),
 extrema CHAR(2),
 region INTEGER(2),		
 PRIMARY KEY  (sku,id_tienda)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
INSERT INTO 
	datos_temporales.sug_temp_jpg
			(SELECT
				tienda_escasos.id_tienda,
				push.sku, #
   			tienda_escasos.formato,
				tienda_escasos.Extrema,
				tienda_escasos.region
			FROM
				datos_temporales.codigos_centrales AS push ,
				tienda_escasos
			WHERE tienda_escasos.silvina_dogchow = 0 AND push.met = 1

			ORDER BY
				push.sku ASC,
				tienda_escasos.id_tienda ASC);
#Fin Matriz Temporal

# Matriz Codigos SELECT
DROP TABLE IF EXISTS datos_temporales.sug_final_jpg;

CREATE TABLE datos_temporales.sug_final_jpg (
 id_tienda INT(11),	
 sku VARCHAR(25),
 #dv CHAR(1),
 carga_new INT(11),
 descripcion 	CHAR(60) DEFAULT '',
 mix CHAR(2),
 #carga_new INT(11),
 disp_tda INT(11),
 pend_tda INT(11),	
 disp_bo INT(11),	
 mid INT(11),
 sds_opt DOUBLE,	
 sds_actual_min DOUBLE,
 sds_actual DOUBLE,	
 PV5_min DOUBLE,	
 PV6 DOUBLE,
 v6 INT(11),
 v5 INT(11),
 v4 INT(11),
 v3 INT(11),
 v2 INT(11),
 v1 INT(11),
 tot_v INT(12),
 formato CHAR(5),
 extrema CHAR(2),
 region INTEGER (2),	
 push INTEGER(2), 	
 obs INTEGER(1),	
 dia INTEGER(1),	
 lu INTEGER(1),	
 ma INTEGER(1),
 mi INTEGER(1),
 ju INTEGER(1),
 vi INTEGER(1),
 #sku VARCHAR(25) NOT NULL,
 PRIMARY KEY  (sku,id_tienda )
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO 
	datos_temporales.sug_final_jpg
			(SELECT
				ccc.id_tienda,
				ccc.sku,
				#ccc.dvProd,
				0 AS Carga,
				TRIM(mae.descripcion_producto),
				IF(ass.assorment is null, 'No',ass.assorment) AS Mix,
				#0 AS carga_new,
				IFNULL(inv_tda.saldo_disponible,0)  AS Disp_Tda,
				IFNULL(inv_tda.saldo_pendiente,0) AS Pend_Tda,
				saldo.saldo_disponible AS Disp_bo,
				mid.MID,
				0 AS sds_opt,
				(IFNULL(inv_tda.saldo_disponible,0)+IFNULL(inv_tda.saldo_pendiente,0))/
				((venta.venta_unidades_6 +
					venta.venta_unidades_5 +
					venta.venta_unidades_4 +
					venta.venta_unidades_3 +
					venta.venta_unidades_2 +
					venta.venta_unidades_1 -
					GREATEST(venta.venta_unidades_6,
					venta.venta_unidades_5,
					venta.venta_unidades_4,
					venta.venta_unidades_3,
					venta.venta_unidades_2,
					venta.venta_unidades_1))/5) AS sds_actual_min,
				((IFNULL(inv_tda.saldo_disponible,0)+IFNULL(inv_tda.saldo_pendiente,0))/
				((venta.venta_unidades_6 +
					venta.venta_unidades_5 +
					venta.venta_unidades_4 +
					venta.venta_unidades_3 +
					venta.venta_unidades_2 +
					venta.venta_unidades_1)/6)) AS sds_actual,
				(venta.venta_unidades_6 +
					venta.venta_unidades_5 +
					venta.venta_unidades_4 +
					venta.venta_unidades_3 +
					venta.venta_unidades_2 +
					venta.venta_unidades_1 -
					GREATEST(venta.venta_unidades_6,
					venta.venta_unidades_5,
					venta.venta_unidades_4,
					venta.venta_unidades_3,
					venta.venta_unidades_2,
					venta.venta_unidades_1))/5 AS PV5_min,
				(venta.venta_unidades_6 +
					venta.venta_unidades_5 +
					venta.venta_unidades_4 +
					venta.venta_unidades_3 +
					venta.venta_unidades_2 +
					venta.venta_unidades_1)/6 AS PV6,
				venta.venta_unidades_6 AS V6,
				venta.venta_unidades_5 AS V5,
				venta.venta_unidades_4 AS V4,
				venta.venta_unidades_3 AS V3,
				venta.venta_unidades_2 AS V2,
				venta.venta_unidades_1 AS V1,
				(venta.venta_unidades_6 +
					venta.venta_unidades_5 +
					venta.venta_unidades_4 +
					venta.venta_unidades_3 +
					venta.venta_unidades_2 +
					venta.venta_unidades_1) AS Vta_Tot,
				ccc.formato,
				ccc.extrema,
				ccc.region,
				mae.push,
				IF(obs.sku is null,0,1) AS Obsoleto,
				IF( dia.dia is null, 0, dia.dia)  AS Dia,
				IF( ISNULL(lu.dia ), 0, 1) AS LU,
				IF( ISNULL(ma.dia ), 0, 1) AS MA,
				IF( ISNULL(mi.dia ), 0, 1) AS MI,
				IF( ISNULL(ju.dia ), 0, 1) AS JU,
				IF( ISNULL(vi.dia ), 0, 1) AS VI
				#Concat(ccc.CodProd, ccc.dvProd) AS sku
				FROM
				datos_temporales.sug_temp_jpg AS ccc
				LEFT JOIN retail_grt.productos_maestro AS mae ON mae.sku = ccc.sku
				LEFT JOIN retail_grt.assorment_sku_tiendas AS ass ON ass.sku = ccc.sku AND ass.id_tienda = ccc.id_tienda
				LEFT JOIN retail_grt.inventario_diario AS inv_tda ON inv_tda.sku = ccc.sku AND inv_tda.id_sucursal = ccc.id_tienda
				LEFT JOIN retail_grt.MID_sku_tiendas AS mid ON mid.sku = ccc.sku AND mid.id_tienda = ccc.id_tienda
				LEFT JOIN retail_grt.ventas_detalle_semanal_l6w AS venta ON venta.sku = ccc.sku AND venta.id_tienda = ccc.id_tienda
				LEFT JOIN retail_grt.obsoletos_sku_tiendas AS obs ON obs.sku = ccc.sku AND obs.id_tienda = ccc.id_tienda
				Left Join retail_grt.proveedores_calendario_tiendas AS dia ON ccc.id_tienda = dia.id_tienda AND mae.id_proveedor = dia.id_proveedor AND dia.dia = DATE_FORMAT(curdate(),'%w')+1
				Left Join retail_grt.proveedores_calendario_tiendas AS lu ON ccc.id_tienda = lu.id_tienda AND mae.id_proveedor = lu.id_proveedor AND lu.dia = 2
				Left Join retail_grt.proveedores_calendario_tiendas AS ma ON ccc.id_tienda = ma.id_tienda AND mae.id_proveedor = ma.id_proveedor AND ma.dia = 3
				Left Join retail_grt.proveedores_calendario_tiendas AS mi ON ccc.id_tienda = mi.id_tienda AND mae.id_proveedor = mi.id_proveedor AND mi.dia = 4
				Left Join retail_grt.proveedores_calendario_tiendas AS ju ON ccc.id_tienda = ju.id_tienda AND mae.id_proveedor = ju.id_proveedor AND ju.dia = 5
				Left Join retail_grt.proveedores_calendario_tiendas AS vi ON ccc.id_tienda = vi.id_tienda AND mae.id_proveedor = vi.id_proveedor AND vi.dia = 6
				LEFT JOIN (
						SELECT
							id.sku,
							SUM(id.saldo_disponible) AS saldo_disponible,
							SUM(id.saldo_pendiente) AS saldo_pendiente
						FROM
							retail_grt.inventario_diario AS id
						WHERE
							id.id_sucursal IN (SELECT bod.id_bodega FROM retail_grt.bodegas AS bod WHERE (bod.retail = 1 OR bod.tramo = 1) AND bod.MD_asociado = 3)
						GROUP BY
						id.sku) AS saldo ON ccc.sku = saldo.sku
				WHERE
				ass.assorment = 'Si' AND obs.sku is NULL AND dia.dia = DATE_FORMAT(curdate(),'%w')+1);

#Calculo de Carga

UPDATE datos_temporales.sug_final_jpg 
				SET sds_opt = IF(extrema = 'Si',5.5,5),	
						carga_new = IF(CEIL((IF(extrema = 'Si',5.5,5) - sds_actual_min)*PV5_min/mid)*mid>0, CEIL((IF(extrema = 'Si',5.5,5) - sds_actual_min)*PV5_min/mid)*mid, 0)


   