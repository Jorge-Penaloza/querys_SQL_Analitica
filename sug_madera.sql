#Matriz Temporal
DROP TABLE IF EXISTS datos_temporales.sug_temp_jpg ;
CREATE TABLE datos_temporales.sug_temp_jpg (
 id_tienda INT(11),	
 CodProd INT(25),
 dvProd CHAR(1),
 formato CHAR(5),
 extrema tinyint(1),
 max_mad DOUBLE,		
 sku VARCHAR(25) NOT NULL,
 obs VARCHAR(255),
 PRIMARY KEY  (id_tienda,sku)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO datos_temporales.sug_temp_jpg
	SELECT
		tienda_escasos.id_tienda,
		push.CodProd,
		push.dvProd,
		tienda_escasos.formato,
		tienda_escasos.Extrema,
		tienda_escasos.max_mad,
		Concat(push.CodProd, push.dvProd),
		tienda_escasos.obs
	FROM
		cargar_tienda AS push ,
		tienda_escasos
	WHERE tienda_escasos.mad = 0
	ORDER BY
		push.CodProd ASC,
		tienda_escasos.id_tienda ASC;
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
 max_mad DOUBLE,	
 sds_actual DOUBLE,	
 sds_actual_post_carga DOUBLE,		
 PV6 DOUBLE,
 Tendencia DOUBLE,	
 v6 INT(11),
 v5 INT(11),
 v4 INT(11),
 v3 INT(11),
 v2 INT(11),
 v1 INT(11),
 tot_v INT(12),
 formato CHAR(5),
 extrema tinyint(1),
 push INTEGER(2), 	
 sku VARCHAR(25) NOT NULL,
 carga_2011 INT(11),
 SDS2011 DOUBLE,
 PV2011 DOUBLE,
 `V-3` INT(11),
 `V-2` INT(11),
 `V-1` INT(11),
 `V-0` INT(11),
 `V+1` INT(11),
 `V+2` INT(11),
 `V+3` INT(11),
	nombre_tienda VARCHAR(255),
	region INT(3),
 PRIMARY KEY  (id_tienda,sku)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
#jp_ventas_semanal


INSERT INTO datos_temporales.sug_final_jpg
			(SELECT
				ccc.id_tienda,
				ccc.CodProd,
				ccc.dvProd,
				0 AS Carga,
				TRIM(mae.descripcion_producto),
				ass.assorment,
				if(inv_tda.saldo_disponible IS NULL,0,inv_tda.saldo_disponible) AS Disp_Tda,
				if(inv_tda.saldo_pendiente IS NULL,0,inv_tda.saldo_pendiente ) AS Pend_Tda,
				saldo.saldo_disponible AS Disp_bo,
				mid.MID,
				0 AS sds_opt,
				ccc.max_mad,

				((if(inv_tda.saldo_disponible IS NULL OR inv_tda.saldo_disponible < 0,0,inv_tda.saldo_disponible) + if(inv_tda.saldo_pendiente IS NULL,0,inv_tda.saldo_pendiente ))/
				(( venta.venta_unidades_L6W - 
					GREATEST(venta.venta_unidades_6,
					venta.venta_unidades_5,
					venta.venta_unidades_4,
					venta.venta_unidades_3,
					venta.venta_unidades_2,
					venta.venta_unidades_1))/5)) AS sds_actual,
   
        0 AS sds_actual_post_carga, 

				( venta.venta_unidades_L6W - 
					GREATEST(venta.venta_unidades_6,
					venta.venta_unidades_5,
					venta.venta_unidades_4,
					venta.venta_unidades_3,
					venta.venta_unidades_2,
					venta.venta_unidades_1))/5 AS PV6,
				regresion_pend_corr_0(venta.venta_unidades_6,	venta.venta_unidades_5,	venta.venta_unidades_4,
															venta.venta_unidades_3, venta.venta_unidades_2,	venta.venta_unidades_1) AS Tendencia, 
				venta.venta_unidades_6 AS V6,
				venta.venta_unidades_5 AS V5,
				venta.venta_unidades_4 AS V4,
				venta.venta_unidades_3 AS V3,
				venta.venta_unidades_2 AS V2,
				venta.venta_unidades_1 AS V1,
				venta.venta_unidades_L6W AS Vta_Tot,
				ccc.formato,
				ccc.extrema,
				mae.push,
				Concat(ccc.CodProd, ccc.dvProd) AS sku,
				0 AS carga_2011,
				0 AS SDS2011,
				(venta_old.`LYW-3` + venta_old.`LYW-2` + venta_old.`LYW-1` + venta_old.LYP + venta_old.LYNW1 + 
				venta_old.LYNW2 + venta_old.LYNW3 - GREATEST(venta_old.`LYW-3`, venta_old.`LYW-2`, venta_old.`LYW-1`,
				venta_old.LYP, venta_old.LYNW1,	venta_old.LYNW2 + venta_old.LYNW3))/6 AS PV2011,
				venta_old.`LYW-3` AS `V-3`,
				venta_old.`LYW-2` AS `V-2`,
				venta_old.`LYW-1` AS `V-1`,
				venta_old.LYP AS `V-0`,
				venta_old.LYNW1 AS `V+1`,
				venta_old.LYNW2 AS `V+2`,
				venta_old.LYNW3 AS `V+3`,
				tdas.nombre_tienda, 
				tdas.region
				FROM
				datos_temporales.sug_temp_jpg AS ccc
				LEFT JOIN retail_grt.productos_maestro AS mae ON mae.sku = ccc.sku
				LEFT JOIN retail_grt.assorment_sku_tiendas AS ass ON ass.sku = ccc.sku AND ass.id_tienda = ccc.id_tienda
				LEFT JOIN retail_grt.inventario_diario AS inv_tda ON inv_tda.sku = ccc.sku AND inv_tda.id_sucursal = ccc.id_tienda
				LEFT JOIN retail_grt.MID_sku_tiendas AS mid ON mid.sku = ccc.sku AND mid.id_tienda = ccc.id_tienda
				LEFT JOIN retail_grt.ventas_detalle_semanal_L6W AS venta ON venta.sku = ccc.sku AND venta.id_tienda = ccc.id_tienda
				LEFT JOIN retail_grt.jp_ventas_semanal AS venta_old ON venta_old.sku = ccc.sku AND venta_old.id_tienda = ccc.id_tienda
				LEFT JOIN retail_grt.tiendas AS tdas ON tdas.id_tienda = ccc.id_tienda
				#LEFT JOIN datos_temporales.cargar_tienda_madera AS saldo ON saldo.CodProd = ccc.CodProd
				LEFT JOIN (
						SELECT
							id.sku,
							SUM(IF(id.saldo_disponible < 0 OR id.saldo_disponible iS NULL,0,id.saldo_disponible)) AS saldo_disponible
							#SUM(id.saldo_pendiente) AS saldo_pendiente
						FROM
							retail_grt.inventario_diario AS id
						WHERE
							id.id_sucursal IN (SELECT bod.id_bodega FROM retail_grt.bodegas AS bod WHERE ((bod.retail = 1 OR bod.tramo = 1) AND bod.MD_asociado = 3 AND bod.id_bodega <> 113) OR bod.id_bodega = 117)
						GROUP BY
						id.sku) AS saldo ON ccc.sku = saldo.sku
				WHERE	ass.assorment = 'Si' #AND mae.push = 1
				ORDER BY	ccc.CodProd,ccc.id_tienda
				);

#Actualizar Carga 2011
	UPDATE datos_temporales.sug_final_jpg 
	SET SDS2011 = 
IF((if(Disp_Tda IS NULL OR Disp_Tda < 0, 0, Disp_Tda) + if(Pend_Tda IS NULL OR Pend_Tda < 0, 0, Pend_Tda ))/PV2011
  IS NULL,0,
	(if(Disp_Tda IS NULL OR Disp_Tda < 0, 0, Disp_Tda) + if(Pend_Tda IS NULL  OR Pend_Tda < 0, 0, Pend_Tda ))/PV2011
	);

	UPDATE datos_temporales.sug_final_jpg 
	SET carga_2011 = IF(ROUND(( LEAST(IF(extrema = 1,2.5,2), max_mad) - SDS2011)*PV2011/mid,0)*mid > 0, 
											ROUND(( LEAST(IF(extrema = 1,2.5,2), max_mad) - SDS2011)*PV2011/mid,0)*mid, 0);

	
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

	#DECLARE puntsku CURSOR FOR 
	#	SELECT Concat(mad.CodProd, mad.dvProd) AS sku, 
	#				 mad.saldo_disponible AS disp
	#	FROM datos_temporales.cargar_tienda_madera AS mad 
	#	ORDER BY mad.CodProd ASC;
  DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET done = TRUE;  	
		open puntsku;  
    # Comienza lectura secuencial
		sku_loop: LOOP  

  			fetch puntsku into uid, saldo;
				
				IF done THEN LEAVE sku_loop; END IF; #Termina El sku_loop

				Set aa1 = SDS_Max;

				WHILE aa1 > 0 DO
							UPDATE datos_temporales.sug_final_jpg 
								SET sds_opt = LEAST(IF(extrema = 1,aa1 + (aa1/SDS_Max),aa1), max_mad),	
										carga = IF(ROUND(( LEAST(IF(extrema = 1,aa1 + (aa1/SDS_Max),aa1), max_mad) - sds_actual)*PV6/mid,0)*mid>0, ROUND(( LEAST(IF(extrema = 1,aa1 + (aa1/SDS_Max),aa1),max_mad) - sds_actual)*PV6/mid,0)*mid, 0)
								WHERE sku = uid; 

							#UPDATE datos_temporales.sug_final_jpg 
							#	SET sds_opt = aa1 + 1.5,	
							#			carga = IF(ROUND(( aa1 + 1.5 - sds_actual)*PV6/mid,0)*mid>0, ROUND((aa1 + 1.5 - sds_actual)*PV6/mid,0)*mid, 0)
							#	WHERE sku = uid AND id_tienda IN (79,3,41,65); 

							UPDATE datos_temporales.sug_final_jpg 
								SET sds_opt = aa1 + 1,	
										carga = IF(ROUND(( aa1 + 1 - sds_actual)*PV6/mid,0)*mid>0, ROUND((aa1 + 1 - sds_actual)*PV6/mid,0)*mid, 0)
								WHERE sku = uid AND id_tienda =79; 

							SET @aux2 = @aux1;										
							SET @aux1 = 0;			
							
							SET @aux1 := (SELECT SUM(www.carga) 
								FROM datos_temporales.sug_final_jpg AS www
								WHERE www.sku = uid GROUP BY www.sku);

							SET aa2 = aa1;
							
							IF @aux1 <= saldo  THEN # para madera
								 #SELECT 	@aux1, uid, @aux2;
									
								 SET aa1 = 0;
							ELSE
								 SET aa1 = aa2;
							END IF;
	
							IF saldo <= 0  THEN # para madera
								UPDATE datos_temporales.sug_final_jpg 
								SET sds_opt = 0,	
										carga = 0
								WHERE sku = uid; 	
							END IF;

							SET aa1 = aa1 - 0.01;
						
							#SET aux1 = 	sum_carga;

							#SELECT sum_carga;#, saldo, uid;	

							
				END WHILE;
				#SELECT @aux1, saldo, uid,	aa2;
		END LOOP sku_loop;  
		# Termino lectura secuencial
    CLOSE puntsku;  
END;

call SDS_Test(4);

UPDATE datos_temporales.sug_final_jpg 
	SET sds_actual_post_carga =  ((if(inv_tda.saldo_disponible IS NULL OR inv_tda.saldo_disponible < 0,0,inv_tda.                                                                  saldo_disponible) + 
																if(inv_tda.saldo_pendiente IS NULL,0,inv_tda.saldo_pendiente )+)/
				(( venta.venta_unidades_L6W - 
					GREATEST(venta.venta_unidades_6,
					venta.venta_unidades_5,
					venta.venta_unidades_4,
					venta.venta_unidades_3,
					venta.venta_unidades_2,
					venta.venta_unidades_1))/5));

   