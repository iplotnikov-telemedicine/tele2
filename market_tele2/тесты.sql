-------------------------MARKET_TRAFFIC------------------------------



INSERT INTO UAT_PRODUCT.MARKET_TRAFFIC
SEL TOP 10 * FROM UAT_PRODUCT.MARKET_TRAFFIC

DROP TABLE UAT_PRODUCT.MARKET_TRAFFIC
CREATE MULTISET TABLE UAT_PRODUCT.MARKET_TRAFFIC AS (
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
                    AND report_date = ?loop_date
                    AND calc_platform_id IN (-1,1,2)
			  	)
	SEL
	    lot_placement_date AS report_date,
	    branch_id_seller,
	    branch_id_purchaser,
	    CASE subs_flag WHEN 1 THEN 'Subs' ELSE 'Tele2' end AS seller_type,
		clrd_sellers.name_report AS seller_name_report,
		clrd_purchasers.name_report AS purchaser_name_report,
	    --branch_id_seller,
	    --branch_id_seller,
	    --subs_id_seller,   
	    traffic_type,
	    CASE traffic_type
	        WHEN 'DATA' THEN
	            CASE WHEN lot_volume BETWEEN 1 AND 5 THEN '1 to 5'
	                WHEN lot_volume<=10 THEN '6 to 10'
	                WHEN lot_volume>10 THEN '>10' 
	                ELSE lot_volume end
	        WHEN 'VOICE' THEN
	            CASE WHEN lot_volume BETWEEN 50 AND 200 THEN '50 to 200'
	                WHEN lot_volume<=600 THEN '201 to 600'
	                WHEN lot_volume>600 THEN '>600' 
	                ELSE lot_volume end
	        WHEN 'SMS' THEN
	            CASE WHEN lot_volume BETWEEN 50 AND 100 THEN '50 to 100'
	                    WHEN lot_volume<=200 THEN '101 to 200'
	                    WHEN lot_volume>200 THEN '>200' 
	                    ELSE lot_volume end
	        ELSE lot_volume end AS traffic_range,
	    Count(lot_placement_date) AS lot_count, 
	    Sum(lot_volume) AS lot_volume,
	    Sum(lot_value) AS lot_value,
	        
	    ZeroIfNull(Sum(sys_revocation_flag)) AS revocation_count,   
	    
	    Count(CASE WHEN lot_purchase_date IS NULL AND subs_revocation_date IS NULL
	        AND NOT sys_revocation_flag IS NULL THEN 1 end) AS expiration_count,
	        
	    Count(lot_purchase_date) AS sold_count, 
	    Sum(CASE WHEN NOT lot_purchase_date IS NULL THEN lot_volume end) AS sold_volume,    
	    Sum(CASE WHEN NOT lot_purchase_date IS NULL THEN lot_value end) AS sold_value
	    
	FROM PRD_RDS_V.MARKET_TELE2 m
	LEFT JOIN clrd AS clrd_sellers
	    ON m.subs_id_seller = clrd_sellers.subs_id
	LEFT JOIN clrd AS clrd_purchasers
	    ON m.subs_id_purchaser = clrd_purchasers.subs_id
	WHERE lot_placement_date = ?loop_date
	GROUP BY 1,2,3,4,5,6,7,8
)
WITH NO DATA
PRIMARY INDEX(report_date,
				branch_id_seller,
				branch_id_purchaser,
				seller_type,
				seller_name_report,
				purchaser_name_report,
				traffic_type,
				traffic_range)

;




/*DROP TABLE UAT_PRODUCT.MARKET_TRAFFIC;*/
/*ALTER TABLE UAT_PRODUCT.MARKET_TRAFFIC RENAME purchase_count TO sold_count;*/





WITH 

market AS (
		SELECT	LOT_ID, SUBS_ID_SELLER, SUBS_ID_PURCHASER, BRANCH_ID_SELLER,
				BRANCH_ID_PURCHASER,
				LOT_PLACEMENT_DATE,
				LOT_PURCHASE_DATE,
				SUBS_REVOCATION_DATE,
				SYS_REVOCATION_FLAG,
				TRAFFIC_TYPE,
				LOT_VOLUME,
				LOT_VALUE,
				SUBS_FLAG
		FROM	PRD_RDS_V.MARKET_TELE2
		WHERE lot_placement_date = DATE'2020-09-01'
		),

clrd AS (		
				SEL
					
					subs_clr_d.subs_id,
					CASE WHEN tp.name_report IN ('Премиум','Мой разговор','Мой Tele2','Мой онлайн',
							'Безлимит','Везде онлайн','Премиум') THEN tp.name_report
							WHEN tp.name_report IN ('Мой онлайн +', 'Мой онлайн+') THEN 'Мой онлайн+'		
							WHEN tp.name_report IN ('Везде онлайн +', 'Везде онлайн+') THEN 'Везде онлайн+'
							ELSE 'Other' end AS name_report
				FROM prd2_bds_v.subs_clr_d
				LEFT JOIN  PRD2_DIC_V.PRICE_PLAN tp
					ON tp.tp_id = subs_clr_d.tp_id
			  	--left join branch on branch.branch_id = subs_clr_d.branch_id
			  	WHERE 1=1
			  		AND report_date = DATE'2020-09-01'
			  		AND calc_platform_id IN (-1,1,2)
			  		AND subs_id IN (SELECT subs_id FROM market)
			  	),

	
SEL
	lot_placement_date AS report_date,
	CASE subs_flag WHEN 1 THEN 'Subs' ELSE 'Tele2' end AS seller_type,
	clrd.name_report,	
	traffic_type,
	CASE traffic_type
		WHEN 'DATA' THEN
			CASE WHEN lot_volume BETWEEN 1 AND 5 THEN '1 to 5'
				WHEN lot_volume<=10 THEN '6 to 10'
				WHEN lot_volume>10 THEN '>10' 
				ELSE lot_volume end
		WHEN 'VOICE' THEN
			CASE WHEN lot_volume BETWEEN 50 AND 200 THEN '50 to 200'
				WHEN lot_volume<=600 THEN '201 to 600'
				WHEN lot_volume>600 THEN '>600' 
				ELSE lot_volume end
		WHEN 'SMS' THEN
			CASE WHEN lot_volume BETWEEN 50 AND 100 THEN '50 to 100'
					WHEN lot_volume<=200 THEN '101 to 200'
					WHEN lot_volume>200 THEN '>200' 
					ELSE lot_volume end
		ELSE lot_volume end AS traffic_range,			
	Count(lot_placement_date) AS lot_count,	
	Sum(sys_revocation_flag) AS revocation_count,	
	Count(CASE WHEN lot_purchase_date IS NULL AND subs_revocation_date IS NULL
		AND NOT sys_revocation_flag IS NULL THEN 1 end) AS expiration_count,	
	Count(lot_purchase_date) AS purchase_count,	
	Sum(lot_volume) AS total_volume,
	Sum(lot_value) AS total_value
	
FROM PRD_RDS_V.MARKET_TELE2
INNER JOIN 
WHERE lot_placement_date = DATE'2020-09-01'
GROUP BY 1,2,3,4









/*

SEL
	*
FROM PRD_RDS_V.MARKET_TELE2
WHERE lot_placement_date BETWEEN DATE'2020-09-01' AND DATE'2020-09-30'
	AND sys_revocation_flag = 1 AND NOT subs_id_purchaser IS NULL*/

/*
	
SEL
	*
FROM PRD_RDS_V.MARKET_TELE2
WHERE lot_placement_date BETWEEN DATE'2020-01-01' AND DATE'2020-09-30'
	AND lot_sale_comission > 0 OR premium_comission > 0
	
	*/
	

/*
SEL Trunc(lot_placement_date, 'mon') AS report_month,
	Count(*)
FROM PRD_RDS_V.MARKET_TELE2
WHERE lot_placement_date BETWEEN DATE'2019-01-01' AND DATE'2020-01-01'
GROUP BY 1
*/


/*
SEL *
FROM PRD_RDS_V.MARKET_TELE2
WHERE lot_placement_date BETWEEN DATE'2019-09-01' AND DATE'2019-09-30'*/












-------------------------MARKET_TRADERS------------------------------




CREATE MULTISET TABLE UAT_PRODUCT.MARKET_TRADERS AS 
(

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
		                    AND report_date = ?some_date
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
		WHERE lot_placement_date = ?some_date
			AND (seller_type = 'Tele2' OR clrd_sellers.name_report IS NOT NULL)
		GROUP BY 1,2,3,4,5,6

)
WITH NO DATA
PRIMARY INDEX(report_date, seller_type, seller_name_report, purchaser_name_report, traffic_type)
;




WITH a AS (
	SEL
		TOP 30 lot_placement_date
	FROM PRD_RDS_V.MARKET_TELE2
	GROUP BY 1
	ORDER BY 1 DESC
	)
	



SHOW VIEW PRD_RDS_V.MARKET_TELE2





SEL
    subs_clr_d.report_date,
    subs_clr_d.branch_id,
	CASE bundle_flag WHEN 1 THEN 'Bundle' ELSE 'PAYG' END AS bundle_or_payg,
    CASE WHEN tp.name_report IN ('Премиум','Мой разговор','Мой онлайн',
            'Безлимит','Везде онлайн','Премиум') THEN tp.name_report
			WHEN name_report IN ('Лайт', 'Мой Tele2') THEN 'Лайт/Мой Tele2'
            WHEN tp.name_report IN ('Мой онлайн +', 'Мой онлайн+') THEN 'Мой онлайн+'       
            WHEN tp.name_report IN ('Везде онлайн +', 'Везде онлайн+') THEN 'Везде онлайн+'
            ELSE 'Other' end AS name_report,
	Count(subs_clr_d.subs_id) AS subs_count
FROM prd2_bds_v2.subs_clr_d
LEFT JOIN  PRD2_DIC_V.PRICE_PLAN tp
    ON tp.tp_id = subs_clr_d.tp_id
--left join branch on branch.branch_id = subs_clr_d.branch_id
WHERE 1=1
    AND report_date BETWEEN ?some_date AND ?another_date
    AND calc_platform_id IN (-1,1,2)
GROUP BY 1,2,3,4



SEL TOP 10 * FROM UAT_PRODUCT.MARKET_TRAffic


HELP VIEW PRD_RDS_V.MARKET_TELE2

SEL
		lot_placement_date AS report_date,
		CASE subs_flag WHEN 1 THEN 'Subs' ELSE 'Tele2' end AS seller_type,
		traffic_type,
		
		Count(DISTINCT subs_id_seller) AS sellers_count,
		Count(DISTINCT subs_id_purchaser) AS purchasers_count,
		
		Count(lot_placement_date) AS lot_count, 
	    Sum(lot_volume) AS lot_volume,
		Sum(lot_value) AS sold_value,
		
	    ZeroIfNull(Sum(sys_revocation_flag)) AS revocation_count,       
	    Count(CASE WHEN lot_purchase_date IS NULL AND subs_revocation_date IS NULL
		        AND NOT sys_revocation_flag IS NULL THEN 1 end) AS expiration_count        
		    
	
FROM PRD_RDS_V.MARKET_TELE2 m
WHERE lot_placement_date = DATE'2020-09-01'
	AND (seller_type = 'Tele2')
GROUP BY 1,2,3