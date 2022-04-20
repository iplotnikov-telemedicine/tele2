REPLACE PROCEDURE UAT_PRODUCT.MARKET_TELE2_INSERT (IN in_period VARCHAR(10), IN from_date DATE, IN till_date DATE)
--daily or weekly or monthly
--call UAT_PRODUCT.MARKET_TELE2_INSERT ('monthly', date'2019-01-01', date'2020-02-29')

SQL SECURITY INVOKER

BEGIN

	DECLARE trunc_period VARCHAR(10);	
	DECLARE loop_start_date DATE;
	DECLARE loop_end_date DATE;
	SET loop_start_date = from_date;
	
	
	IF in_period = 'daily' THEN
		SET trunc_period = 'ddd';	
		SET loop_end_date = loop_start_date;
	ELSEIF in_period = 'weekly' THEN
		SET trunc_period = 'iw';
		SET loop_end_date = loop_start_date + INTERVAL '6' DAY;
	ELSEIF in_period = 'monthly' THEN
		SET trunc_period = 'mon';
		SET loop_end_date = last_day(loop_start_date);
	END IF;
	
	
	
	WHILE loop_start_date <= till_date DO 
	
		BEGIN		
		
		DELETE FROM UAT_PRODUCT.MARKET_TELE2
		WHERE period = in_period
			and report_date between loop_start_date and loop_end_date;
		
		INSERT INTO UAT_PRODUCT.MARKET_TELE2

		--sellers
		SELECT	
		        Cast(in_period AS VARCHAR(255)) AS period,   
		        Cast('sellers' AS VARCHAR(255)) AS table_name,
			Trunc(LOT_PLACEMENT_DATE, trunc_period) AS report_date,
			Trunc(lot_purchase_date, trunc_period) AS purchase_date,
			CASE subs_flag WHEN 1 THEN 'Subs' ELSE 'Tele2' end AS seller_type,
			traffic_type,
			
			BRANCH_ID_SELLER,
			Cast(NULL AS INTEGER) AS BRANCH_ID_PURCHASER,

		        tp_id_seller,
		        Cast(NULL AS INTEGER) AS tp_id_purchaser,
			
			CASE WHEN NOT lot_purchase_date IS NULL AND SYS_REVOCATION_FLAG IS NULL THEN 'sold'
				WHEN NOT subs_revocation_date IS NULL AND SYS_REVOCATION_FLAG IS NULL THEN 'revoked'
				WHEN subs_revocation_date IS NULL AND NOT SYS_REVOCATION_FLAG IS NULL AND lot_purchase_date IS NULL THEN 'expired'
				WHEN subs_revocation_date IS NULL AND SYS_REVOCATION_FLAG IS NULL AND lot_purchase_date IS NULL THEN 'active'
				ELSE 'undefined' END AS status,
			
			Count(DISTINCT SUBS_ID_SELLER) AS sellers_count,
			Cast(NULL AS INTEGER) AS purchasers_count,		
			Count(LOT_ID) AS lot_count,
			Sum(LOT_VOLUME) AS lot_volume,
			Sum(LOT_VALUE) AS sold_value,
			Current_Date AS insert_date
				

		FROM PRD_RDS_V.MARKET_TELE2
		WHERE LOT_PLACEMENT_DATE between loop_start_date and loop_end_date
		GROUP BY 3,4,5,6,7,8,9,10,11


		UNION ALL

		--purchasers
		SELECT	
		        Cast(in_period AS VARCHAR(255)) AS period,
		        Cast('purchasers' AS VARCHAR(255)) AS table_name,
			Trunc(LOT_PLACEMENT_DATE, trunc_period) AS report_date,
			Trunc(lot_purchase_date, trunc_period) AS purchase_date,
			CASE subs_flag WHEN 1 THEN 'Subs' ELSE 'Tele2' end AS seller_type,
			traffic_type,
			
			Cast(NULL AS INTEGER) AS BRANCH_ID_SELLER,
			BRANCH_ID_PURCHASER,
			
		        Cast(NULL AS INTEGER) AS tp_id_seller,
		        tp_id_purchaser,

			CASE WHEN NOT lot_purchase_date IS NULL AND SYS_REVOCATION_FLAG IS NULL THEN 'sold'
				WHEN NOT subs_revocation_date IS NULL AND SYS_REVOCATION_FLAG IS NULL THEN 'revoked'
				WHEN subs_revocation_date IS NULL AND NOT SYS_REVOCATION_FLAG IS NULL AND lot_purchase_date IS NULL THEN 'expired'
				WHEN subs_revocation_date IS NULL AND SYS_REVOCATION_FLAG IS NULL AND lot_purchase_date IS NULL THEN 'active'
				ELSE 'undefined' END AS status,
			
			Cast(NULL AS INTEGER) AS sellers_count,
			Count(DISTINCT SUBS_ID_PURCHASER) AS purchasers_count,		
			Count(LOT_ID) AS lot_count,
			Sum(LOT_VOLUME) AS lot_volume,
			Sum(LOT_VALUE) AS sold_value,
			Current_Date AS insert_date
				

		FROM PRD_RDS_V.MARKET_TELE2
		WHERE LOT_PLACEMENT_DATE between loop_start_date and loop_end_date
			and purchase_date is not null
		GROUP BY 3,4,5,6,7,8,9,10,11



		UNION ALL


		--base
		SELECT	
			cast(in_period AS VARCHAR(255)) AS period,
		    Cast('base' AS VARCHAR(255)) AS table_name,
			Trunc(report_date, trunc_period) as report_date,
			
			Cast(NULL AS DATE) AS purchase_date,
			Cast('Subs' AS VARCHAR(255)) AS seller_type,
			Cast(NULL AS VARCHAR(255)) as traffic_type,
			
			branch_id AS BRANCH_ID_SELLER,
			branch_id as BRANCH_ID_PURCHASER,

		    tp_id AS tp_id_seller,
		    tp_id as tp_id_purchaser,
			
			Cast(NULL AS VARCHAR(255)) AS status,
			
			sum(FLASH_ACTIVE_COUNT) AS sellers_count,
			sum(FLASH_ACTIVE_COUNT) AS purchasers_count,		
			NULL AS lot_count,
			NULL AS lot_volume,
			NULL AS sold_value,	
			Current_Date AS insert_date

		from PRD_RDS_V.PRODUCT_AGG_D_SAP_BO
		where 1=1
			and CALC_PLATFORM_ID in (-1,1,2)
			and report_date = loop_end_date
		GROUP BY 3,4,5,6,7,8,9,10,11

		;
		END;
	
	IF in_period = 'daily' THEN
		SET loop_start_date = loop_start_date + interval '1' DAY;
		SET loop_end_date = loop_start_date;
	ELSEIF in_period = 'weekly' THEN
		SET loop_start_date = loop_start_date + interval '7' DAY;
		SET loop_end_date = loop_start_date + interval '6' DAY;
	ELSEIF in_period = 'monthly' THEN
		SET loop_start_date = loop_start_date + interval '1' MONTH;
		SET loop_end_date = last_day(loop_start_date);
	END IF;
	
	
	END WHILE;
	
	COLLECT STATISTICS
		COLUMN(period)
		,COLUMN(table_name)
		,COLUMN(report_date)
		,COLUMN(purchase_date)
		,COLUMN(seller_type)
		,COLUMN(traffic_type)
		,COLUMN(branch_id_seller)
		,COLUMN(branch_id_purchaser)
		,COLUMN(tp_id_seller)
		,COLUMN(tp_id_purchaser)
		,COLUMN(insert_date)
		ON UAT_PRODUCT.MARKET_TELE2;

END;

