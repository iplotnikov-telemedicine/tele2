REPLACE PROCEDURE UAT_PRODUCT.MARKET_TRADERS_INSERT (IN from_date DATE, IN till_date DATE)
--call UAT_PRODUCT.MARKET_TRADERS_INSERT (date'2020-12-01', date'2020-12-06')

SQL SECURITY INVOKER

BEGIN

	DECLARE loop_date DATE;
	DECLARE end_date DATE;
	
	SET loop_date = from_date;
	SET end_date = till_date;	
	
	WHILE loop_date <= end_date DO 
	
		BEGIN
		
		DELETE FROM UAT_PRODUCT.MARKET_TRADERS
		WHERE report_date=loop_date;
		
		INSERT INTO UAT_PRODUCT.MARKET_TRADERS

			WITH clrd AS (      
			                SEL
			                    subs_clr_d.report_date,
			                    subs_clr_d.subs_id,
			                    subs_clr_d.branch_id,
			                    CASE WHEN tp.name_report IN ('Премиум','Мой разговор','Мой онлайн',
			                            'Безлимит','Везде онлайн','Премиум') THEN tp.name_report
										WHEN name_report IN ('Лайт', 'Мой Tele2') THEN 'Лайт/Мой Tele2'
			                            WHEN tp.name_report IN ('Мой онлайн +', 'Мой онлайн+') THEN 'Мой онлайн+'       
			                            WHEN tp.name_report IN ('Везде онлайн +', 'Везде онлайн+') THEN 'Везде онлайн+'
			                            ELSE 'Other' end AS name_report
			                FROM prd2_bds_v2.subs_clr_d
			                LEFT JOIN  PRD2_DIC_V.PRICE_PLAN tp
			                    ON tp.tp_id = subs_clr_d.tp_id
			                --left join branch on branch.branch_id = subs_clr_d.branch_id
			                WHERE 1=1
			                    AND report_date = loop_date
			                    AND calc_platform_id IN (-1,1,2)
				
			                )


			SEL
				lot_placement_date AS report_date,
				--branch_id_seller,
			    --branch_id_purchaser,
				CASE subs_flag WHEN 1 THEN 'Subs' ELSE 'Tele2' end AS seller_type,
				clrd_sellers.name_report AS seller_name_report,
				clrd_purchasers.name_report AS purchaser_name_report,
				traffic_type,
				CASE WHEN NOT subs_id_purchaser IS NULL THEN 'yes' ELSE 'no' END is_sold,
				
				Count(DISTINCT subs_id_seller) AS sellers_count,
				Count(DISTINCT subs_id_purchaser) AS purchasers_count,
				
				Count(lot_placement_date) AS lot_count, 
			    Sum(lot_volume) AS lot_volume,
				Sum(lot_value) AS sold_value
				
			    --ZeroIfNull(Sum(sys_revocation_flag)) AS revocation_count,       
			    --Count(CASE WHEN lot_purchase_date IS NULL AND subs_revocation_date IS NULL
			        --AND NOT sys_revocation_flag IS NULL THEN 1 end) AS expiration_count,        
			    
				
			FROM PRD_RDS_V.MARKET_TELE2 m
			LEFT JOIN clrd AS clrd_sellers
			    ON m.subs_id_seller = clrd_sellers.subs_id
			LEFT JOIN clrd AS clrd_purchasers
			    ON m.subs_id_purchaser = clrd_purchasers.subs_id
			WHERE lot_placement_date = loop_date
				AND (seller_type = 'Tele2' OR clrd_sellers.name_report IS NOT NULL)
			GROUP BY 1,2,3,4,5,6

		;
		END;
	
	SET loop_date = loop_date + INTERVAL '1' DAY;
	END WHILE;
	
	COLLECT STATISTICS
		COLUMN(report_date)
		,COLUMN(seller_type)
		,COLUMN(seller_name_report)
		,COLUMN(purchaser_name_report)
		,COLUMN(traffic_type)
		ON UAT_PRODUCT.MARKET_TRADERS;

END;