WITH days AS (
        SEL
            max(lot_placement_date) as max_date, 
            max_date - 9 as days_from, --10 дней
            trunc(max_date - 70,'IW') as weeks_from, --10 недель
            trunc(add_months(max_date,-12),'mon') as months_from	--13 мес€цев
	FROM PRD_RDS_V.MARKET_TELE2
	where lot_placement_date >= Current_Date - 10
	)

------------------------------------------------------------------------------------------дни

SELECT	
        Cast('daily' AS VARCHAR(255)) AS period,   
        Cast('sellers' AS VARCHAR(255)) AS table_name,
	LOT_PLACEMENT_DATE AS report_date,
	lot_purchase_date AS purchase_date,
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
	Sum(LOT_VALUE) AS sold_value	
		

FROM PRD_RDS_V.MARKET_TELE2
WHERE LOT_PLACEMENT_DATE >= (SEL days_from FROM days)
GROUP BY 3,4,5,6,7,8,9,10,11


UNION ALL


SELECT	
        Cast('daily' AS VARCHAR(255)) AS period,
        Cast('purchasers' AS VARCHAR(255)) AS table_name,
	LOT_PLACEMENT_DATE AS report_date,
	lot_purchase_date AS purchase_date,
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
	Sum(LOT_VALUE) AS sold_value	
		

FROM PRD_RDS_V.MARKET_TELE2
WHERE LOT_PLACEMENT_DATE >= (SEL days_from FROM days)
GROUP BY 3,4,5,6,7,8,9,10,11





UNION ALL



SELECT	
	cast('daily' AS VARCHAR(255)) AS period,
    Cast('base' AS VARCHAR(255)) AS table_name,
	report_date as report_date,
	
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
	NULL AS sold_value	

from PRD_RDS_V.PRODUCT_AGG_D_SAP_BO
where 1=1
	and CALC_PLATFORM_ID in (-1,1,2)
	and report_date >= (SEL days_from FROM days)
GROUP BY 3,4,5,6,7,8,9,10,11




UNION ALL

--------------------------------------------------------------------------------------недели



SELECT	
        Cast('weekly' AS VARCHAR(255)) AS period,   
        Cast('sellers' AS VARCHAR(255)) AS table_name,
	Trunc(LOT_PLACEMENT_DATE,'IW') AS report_date,
	Trunc(lot_purchase_date,'IW') AS purchase_date,
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
	Sum(LOT_VALUE) AS sold_value	
		

FROM PRD_RDS_V.MARKET_TELE2
WHERE LOT_PLACEMENT_DATE >= (SEL weeks_from FROM days)
GROUP BY 3,4,5,6,7,8,9,10,11


UNION ALL


SELECT	
        Cast('weekly' AS VARCHAR(255)) AS period,
        Cast('purchasers' AS VARCHAR(255)) AS table_name,
	Trunc(LOT_PLACEMENT_DATE,'IW') AS report_date,
	Trunc(lot_purchase_date,'IW') AS purchase_date,
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
	Sum(LOT_VALUE) AS sold_value	
		

FROM PRD_RDS_V.MARKET_TELE2
WHERE LOT_PLACEMENT_DATE >= (SEL weeks_from FROM days)
GROUP BY 3,4,5,6,7,8,9,10,11



UNION ALL



SELECT	
	cast('weekly' AS VARCHAR(255)) AS period,
    Cast('base' AS VARCHAR(255)) AS table_name,
	Trunc(report_date, 'iw') as report_date,
	
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
	NULL AS sold_value	

from PRD_RDS_V.PRODUCT_AGG_D_SAP_BO
where 1=1
	and CALC_PLATFORM_ID in (-1,1,2)
	and report_date = Trunc(report_date, 'iw') + 6
	and report_date >= (SEL weeks_from FROM days)
GROUP BY 3,4,5,6,7,8,9,10,11





UNION ALL

-------------------------------------------------------------------------------мес€цы



SELECT	
        Cast('monthly' AS VARCHAR(255)) AS period,   
        Cast('sellers' AS VARCHAR(255)) AS table_name,
	Trunc(LOT_PLACEMENT_DATE,'mon') AS report_date,
	Trunc(lot_purchase_date,'mon') AS purchase_date,
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
	Sum(LOT_VALUE) AS sold_value	
		

FROM PRD_RDS_V.MARKET_TELE2
WHERE LOT_PLACEMENT_DATE >= (SEL months_from FROM days)
GROUP BY 3,4,5,6,7,8,9,10,11


UNION ALL


SELECT	
        Cast('monthly' AS VARCHAR(255)) AS period,
        Cast('purchasers' AS VARCHAR(255)) AS table_name,
	Trunc(LOT_PLACEMENT_DATE,'mon') AS report_date,
	Trunc(lot_purchase_date,'mon') AS purchase_date,
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
	Sum(LOT_VALUE) AS sold_value	
		

FROM PRD_RDS_V.MARKET_TELE2
WHERE LOT_PLACEMENT_DATE >= (SEL months_from FROM days)
GROUP BY 3,4,5,6,7,8,9,10,11


UNION ALL



SELECT	
	cast('monthly' AS VARCHAR(255)) AS period,
    Cast('base' AS VARCHAR(255)) AS table_name,
	Trunc(report_date, 'mon') as report_date,
	
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
	NULL AS sold_value	

from PRD_RDS_V.PRODUCT_AGG_D_SAP_BO
where 1=1
	and CALC_PLATFORM_ID in (-1,1,2)
	and report_date in (
	date'2020-01-31',
	date'2020-02-29',
	date'2020-03-31',
	date'2020-04-30',
	date'2020-05-31',
	date'2020-06-30',
	date'2020-07-31',
	date'2020-08-31',
	date'2020-09-30',
	date'2020-10-31',
	date'2020-11-30',
	date'2020-12-31'
	)
GROUP BY 3,4,5,6,7,8,9,10,11

