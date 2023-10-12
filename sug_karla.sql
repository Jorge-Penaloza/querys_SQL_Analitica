#Matriz Temporal
DROP TABLE IF EXISTS datos_temporales.sug_temp_jpg ;
CREATE TABLE datos_temporales.sug_temp_jpg (
 id_tienda INT(11),	
 CodProd INT(25),
 dvProd CHAR(1),
 formato CHAR(5),
 extrema CHAR(2),		
 max_esc DOUBLE,	
 sku VARCHAR(25) NOT NULL,
 PRIMARY KEY  (sku,id_tienda)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO 
	datos_temporales.sug_temp_jpg
			(SELECT
				tienda_escasos.id_tienda,
				push.CodProd,
				push.dvProd,
   			tienda_escasos.formato,
				tienda_escasos.Extrema,
				tienda_escasos.max_esc,
				Concat(push.CodProd, push.dvProd)
			FROM
				datos_temporales.codigos_centrales AS push ,
				datos_temporales.tienda_escasos
			WHERE push.met = 2 AND tienda_escasos.id_tienda <> 37
			ORDER BY
				push.CodProd ASC,
				tienda_escasos.id_tienda ASC);


#Fin Matriz Temporal

# Matriz Codigos SELECT
DROP TABLE IF EXISTS datos_temporales.sug_final_jpg;

CREATE TABLE datos_temporales.sug_final_jpg (
 id_tienda INT(11),	
 codigo INT(25),
 dv CHAR(1),
 carga INT(11),
 descripcion 	CHAR(60) DEFAULT '',
 mix CHAR(2),
 disp_tda INT(11),
 pend_tda INT(11),	
 disp_bo INT(11),	
 mid INT(11),
 sds_opt DOUBLE,	
 max_esc DOUBLE,	
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
 push INTEGER(2), 	
 obs INTEGER(1),	
 sku VARCHAR(25) NOT NULL,
 PRIMARY KEY  (sku,id_tienda )
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO 
	datos_temporales.sug_final_jpg
			(SELECT
				ccc.id_tienda,
				ccc.CodProd,
				ccc.dvProd,
				0 AS Carga,
				TRIM(mae.descripcion_producto),
				ass.assorment,
				IFNULL(inv_tda.saldo_disponible,0)  AS Disp_Tda,
				IFNULL(inv_tda.saldo_pendiente,0) AS Pend_Tda,
				saldo.saldo_disponible AS Disp_bo,
				mid.MID,
				0 AS sds_opt,
				ccc.max_esc,
				(if(inv_tda.saldo_disponible IS NULL OR inv_tda.saldo_disponible < 0,0,inv_tda.saldo_disponible) + if(inv_tda.saldo_pendiente IS NULL,0,inv_tda.saldo_pendiente ))/
				((venta.ventas_unidades_6 +
					venta.ventas_unidades_5 +
					venta.ventas_unidades_4 +
					venta.ventas_unidades_3 +
					venta.ventas_unidades_2 +
					venta.ventas_unidades_1 -
					GREATEST(venta.ventas_unidades_6,
					venta.ventas_unidades_5,
					venta.ventas_unidades_4,
					venta.ventas_unidades_3,
					venta.ventas_unidades_2,
					venta.ventas_unidades_1))/5) AS sds_actual_min,
				((IFNULL(inv_tda.saldo_disponible,0)+IFNULL(inv_tda.saldo_pendiente,0))/
				((venta.ventas_unidades_6 +
					venta.ventas_unidades_5 +
					venta.ventas_unidades_4 +
					venta.ventas_unidades_3 +
					venta.ventas_unidades_2 +
					venta.ventas_unidades_1)/6)) AS sds_actual,
				(venta.ventas_unidades_6 +
					venta.ventas_unidades_5 +
					venta.ventas_unidades_4 +
					venta.ventas_unidades_3 +
					venta.ventas_unidades_2 +
					venta.ventas_unidades_1 -
					GREATEST(venta.ventas_unidades_6,
					venta.ventas_unidades_5,
					venta.ventas_unidades_4,
					venta.ventas_unidades_3,
					venta.ventas_unidades_2,
					venta.ventas_unidades_1))/5 AS PV5_min,
				(venta.ventas_unidades_6 +
					venta.ventas_unidades_5 +
					venta.ventas_unidades_4 +
					venta.ventas_unidades_3 +
					venta.ventas_unidades_2 +
					venta.ventas_unidades_1)/6 AS PV6,
				venta.ventas_unidades_6 AS V6,
				venta.ventas_unidades_5 AS V5,
				venta.ventas_unidades_4 AS V4,
				venta.ventas_unidades_3 AS V3,
				venta.ventas_unidades_2 AS V2,
				venta.ventas_unidades_1 AS V1,
				(venta.ventas_unidades_6 +
					venta.ventas_unidades_5 +
					venta.ventas_unidades_4 +
					venta.ventas_unidades_3 +
					venta.ventas_unidades_2 +
					venta.ventas_unidades_1) AS Vta_Tot,
				ccc.formato,
				ccc.extrema,
				mae.push,
				IF(obs.sku is null,0,1) AS Obsoleto,
				Concat(ccc.CodProd, ccc.dvProd) AS sku
				FROM
				datos_temporales.sug_temp_jpg AS ccc
				LEFT JOIN retail_grt.productos_maestro AS mae ON mae.sku = ccc.sku
				LEFT JOIN retail_grt.assorment_sku_tiendas AS ass ON ass.sku = ccc.sku AND ass.id_tienda = ccc.id_tienda
				LEFT JOIN retail_grt.inventario_diario AS inv_tda ON inv_tda.sku = ccc.sku AND inv_tda.id_sucursal = ccc.id_tienda
				LEFT JOIN retail_grt.MID_sku_tiendas AS mid ON mid.sku = ccc.sku AND mid.id_tienda = ccc.id_tienda
				LEFT JOIN retail_grt.ventas_detalle_semanal_l6w AS venta ON venta.sku = ccc.sku AND venta.id_tienda = ccc.id_tienda
				LEFT JOIN retail_grt.obsoletos_sku_tiendas AS obs ON obs.sku = ccc.sku AND obs.id_tienda = ccc.id_tienda
				#LEFT JOIN datos_temporales.cargar_tienda_madera AS saldo ON saldo.CodProd = ccc.CodProd
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
				ass.assorment = 'Si' AND obs.sku is NULL);
# F MAtriz	

# Actualizar Matriz
DROP PROCEDURE IF EXISTS SDS_Test;
CREATE PROCEDURE SDS_Test (SDS_Max FLOAT)
BEGIN
	DECLARE done TINYINT DEFAULT FALSE;
  DECLARE uid VARCHAR(255);  
	DECLAre saldo INTEGER(10);
	DECLARE aa1 DOUBLE;
	DECLARE aa2 DOUBLE;
	#DECLARE @aux1 DOUBLE;
	DECLARE puntsku CURSOR FOR 
		SELECT
			sug_final_jpg.sku AS sku,
			sug_final_jpg.disp_bo AS disp
		FROM
			sug_final_jpg
		GROUP BY
			sug_final_jpg.sku;

  DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET done = TRUE;  	
		open puntsku;  
    # Comienza lectura secuencial
		sku_loop: LOOP  

  			fetch puntsku into uid, saldo;
				
				IF done THEN LEAVE sku_loop; END IF; #Termina El sku_loop

				Set aa1 = SDS_Max;

				WHILE aa1 > 0 DO

							UPDATE datos_temporales.sug_final_jpg 
								SET sds_opt = LEAST(IF(extrema = 'Si',aa1 + (aa1/SDS_Max),aa1), max_esc),	
										carga = IF(ROUND(( LEAST(IF(extrema = 'Si',aa1 + (aa1/SDS_Max),aa1), max_esc) - sds_actual_min)*PV5_min/mid,0)*mid>0, ROUND(( LEAST(IF(extrema = 'Si',aa1 + (aa1/SDS_Max),aa1),max_esc) - sds_actual_min)*PV5_min/mid,0)*mid, 0)
								WHERE sku = uid; 

							#UPDATE datos_temporales.sug_final_jpg 
							#	SET sds_opt = IF(extrema = 'Si',aa1 + 0.5,aa1),	
							#			carga = IF(ROUND((IF(extrema = 'Si',aa1 + 0.5,aa1) - sds_actual_min)*PV5_min/mid,0)*mid>0, ROUND((IF(extrema = 'Si',aa1 + 0.5,aa1) - sds_actual_min)*PV5_min/mid,0)*mid, 0)
							#	WHERE sku = uid; 

							SET @aux1 = 0;
							SET @aux1 := (SELECT SUM(www.carga) 
								FROM datos_temporales.sug_final_jpg AS www
								WHERE www.sku = uid GROUP BY www.sku);
							IF @aux1 < 0.8*saldo  THEN 
								UPDATE datos_temporales.sug_final_jpg 
									SET carga = IF( (disp_tda + pend_tda) <= 0 AND carga = 0, mid, carga)
									WHERE sku = uid; 
							END IF;
							

							SET @aux1 = 0;			
							SET @aux1 := (SELECT SUM(www.carga) 
								FROM datos_temporales.sug_final_jpg AS www
								WHERE www.sku = uid GROUP BY www.sku);
							SET aa2 = aa1;
							IF @aux1 <= 0.9*saldo  THEN 
								 SET aa1 = 0;
							END IF;

							IF saldo <= 0  THEN # para madera
								UPDATE datos_temporales.sug_final_jpg 
								SET sds_opt = 0,	
										carga = 0
								WHERE sku = uid; 	
							END IF;

							SET aa1 = aa1 - 0.01;
						
							#SET aux1 = 	sum_carga;

							#SELECT @aux1;#, saldo, uid;	

							
				END WHILE;
				#SELECT @aux1, saldo, uid,	aa2;
		END LOOP sku_loop;  
		# Termino lectura secuencial
    CLOSE puntsku;  
END;

call SDS_Test(3);
   