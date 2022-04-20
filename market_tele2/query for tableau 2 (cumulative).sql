SELECT	
    Cast('sellers_count' AS VARCHAR(255)) AS metric,   	
	Count(DISTINCT SUBS_ID_SELLER) AS metric_value
FROM PRD_RDS_V.MARKET_TELE2
WHERE subs_flag = 1

	union all

SELECT	
    Cast('purchasers_count' AS VARCHAR(255)) AS metric,   	
	Count(DISTINCT SUBS_ID_PURCHASER) AS metric_value
FROM PRD_RDS_V.MARKET_TELE2
WHERE subs_flag = 1

	union all

SELECT		
	Cast('traders_count' AS VARCHAR(255)) AS metric,   	
	Count(DISTINCT subs_id_trader) AS metric_value
		
FROM (
        sel SUBS_ID_SELLER as subs_id_trader
        FROM PRD_RDS_V.MARKET_TELE2 sellers
        WHERE subs_flag = 1

        union all

        sel SUBS_ID_PURCHASER as subs_id_trader
        FROM PRD_RDS_V.MARKET_TELE2 purchasers
        WHERE subs_flag = 1
        ) all_traders

