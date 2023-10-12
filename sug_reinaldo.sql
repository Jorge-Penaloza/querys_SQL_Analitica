#Matriz Temporal
DROP TABLE IF EXISTS datos_temporales.sug_temp_jpg ;
CREATE TABLE datos_temporales.sug_temp_jpg (
 id_tienda INT(11),	
 CodProd INT(25),
 dvProd CHAR(1),
 formato CHAR(5),
 extrema tinyint(1),
 sku VARCHAR(25) NOT NULL,
 PRIMARY KEY  (id_tienda,sku)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO datos_temporales.sug_temp_jpg
	SELECT
		tienda_escasos.id_tienda,
		push.CodProd,
		push.dvProd,
		tienda_escasos.formato,
		tienda_escasos.Extrema,
		Concat(push.CodProd, push.dvProd)
	FROM
		cargar_tienda AS push ,
		tienda_escasos
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
 sds_actual DOUBLE,	
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
				temporal.cant AS Disp_bo,
				#saldo.saldo_disponible AS Disp_bo,
				mid.MID,
				0 AS sds_opt,
				((if(inv_tda.saldo_disponible IS NULL OR inv_tda.saldo_disponible < 0,0,inv_tda.saldo_disponible) + if(inv_tda.saldo_pendiente IS NULL,0,inv_tda.saldo_pendiente ))/
				(( venta.venta_unidades_L6W - 
					GREATEST(venta.venta_unidades_6,
					venta.venta_unidades_5,
					venta.venta_unidades_4,
					venta.venta_unidades_3,
					venta.venta_unidades_2,
					venta.venta_unidades_1))/5)) AS sds_actual,

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
				tdas.nombre_tienda, 
				tdas.region
				FROM
				datos_temporales.sug_temp_jpg AS ccc
				LEFT JOIN retail_grt.productos_maestro AS mae ON mae.sku = ccc.sku
				LEFT JOIN retail_grt.assorment_sku_tiendas AS ass ON ass.sku = ccc.sku AND ass.id_tienda = ccc.id_tienda
				LEFT JOIN retail_grt.inventario_diario AS inv_tda ON inv_tda.sku = ccc.sku AND inv_tda.id_sucursal = ccc.id_tienda
				LEFT JOIN retail_grt.MID_sku_tiendas AS mid ON mid.sku = ccc.sku AND mid.id_tienda = ccc.id_tienda
				LEFT JOIN retail_grt.ventas_detalle_semanal_l6w AS venta ON venta.sku = ccc.sku AND venta.id_tienda = ccc.id_tienda
				LEFT JOIN retail_grt.tiendas AS tdas ON tdas.id_tienda = ccc.id_tienda
				#LEFT JOIN datos_temporales.cargar_tienda_madera AS saldo ON saldo.CodProd = ccc.CodProd
				LEFT JOIN datos_temporales.cargar_tienda AS temporal ON temporal.sku = ccc.sku
				LEFT JOIN (
						SELECT
							id.sku,
							SUM(IF(id.saldo_disponible < 0 OR id.saldo_disponible iS NULL,0,id.saldo_disponible)) AS saldo_disponible
							#SUM(id.saldo_pendiente) AS saldo_pendiente
						FROM
							retail_grt.inventario_diario AS id
						WHERE
							id.id_sucursal IN (SELECT bod.id_bodega FROM retail_grt.bodegas AS bod WHERE ((bod.retail = 1 OR bod.tramo = 1) AND bod.MD_asociado = 3) OR bod.id_bodega = 117)
						GROUP BY
						id.sku) AS saldo ON ccc.sku = saldo.sku
				WHERE	ass.assorment = 'Si' #AND mae.push = 1
				ORDER BY	ccc.CodProd,ccc.id_tienda
				);

	
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
								SET sds_opt = IF(extrema = 1,aa1 + (aa1/SDS_Max),aa1),	
										carga = IF(ROUND(( IF(extrema = 1,aa1 + (aa1/SDS_Max),aa1) - sds_actual)*PV6/mid,0)*mid>0, ROUND(( IF(extrema = 1,aa1 + (aa1/SDS_Max),aa1) - sds_actual)*PV6/mid,0)*mid, 0)
								WHERE sku = uid; 

							SET @aux2 = @aux1;										
							SET @aux1 = 0;			
							
							SET @aux1 := (SELECT SUM(www.carga) 
								FROM datos_temporales.sug_final_jpg AS www
								WHERE www.sku = uid GROUP BY www.sku);

							SET aa2 = aa1;
							
							IF @aux1 <= 0.9*saldo  THEN # para madera
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
call SDS_Test(3);

   