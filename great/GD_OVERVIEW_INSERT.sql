REPLACE PROCEDURE UAT_PRODUCT.GD_OVERVIEW_INSERT (
	IN input_month DATE, 
	IN months_forward_from_input_month INTEGER,
	IN rolling_value VARCHAR(10)
	)

--show procedure UAT_PRODUCT.GD_OVERVIEW_INSERT
--call UAT_PRODUCT.GD_OVERVIEW_INSERT('2020-12-01', 1, 'F3_2020_01')
--sel min(report_date) from UAT_PRODUCT.GD_OVERVIEW
--delete from UAT_PRODUCT.GD_OVERVIEW
--delete from UAT_PRODUCT.GD_OVERVIEW where KPI = 'Market Shares' and report_date = date'2018-04-01'
--UPDATE UAT_PRODUCT.GD_OVERVIEW SET BU = null WHERE report_date < date'2020-01-01'
-- sel -1 * --select * from UAT_PRODUCT.GD_OVERVIEW
--sel report_date, sum(AC) from UAT_PRODUCT.GD_OVERVIEW where KPI = 'Service revenue' group by 1
--sel * from UAT_PRODUCT.GD_OVERVIEW where KPI = 'Market Shares'
--sel distinct param_2 from UAT_PRODUCT.PRODUCT_PARAMETERS where param_1 = 'BU'
--sel top 100 * from UAT_PRODUCT.GD_OVERVIEW
--sel min(report_date) from UAT_PRODUCT.GD_OVERVIEW
--sel * from UAT_PRODUCT.GD_OVERVIEW where region_name like '%Горно%'
--sel count(*) from UAT_PRODUCT.GD_OVERVIEW where LY is null

--alter table UAT_PRODUCT.GD_OVERVIEW drop LY
--sel * from UAT_PRODUCT.GD_OVERVIEW where LY is not null
--Churn
--Variable recurring costs
--Content costs
--Gross Margin 1
--Total Minutes technical
--Recurring Revenue
--Service Revenue (w/o IC&Content)
--Revenue
--Gross Intake
--Service revenue
--Number of subscribers
--Average number of subscribers
--DATA traffic
--Service revenue (w/o interconnect)

--sel distinct param_1 from uat_product.product_parameters


SQL SECURITY INVOKER

BEGIN

	DECLARE month_start_date DATE;
	DECLARE month_end_date DATE;
	DECLARE last_month DATE;
	
	SET month_start_date = input_month;
	SET month_end_date = Last_Day(month_start_date);
	SET last_month = Add_Months(input_month, months_forward_from_input_month);

--show table UAT_PRODUCT.GD_OVERVIEW
WHILE month_start_date < last_month DO 
	
	BEGIN
	DELETE FROM uat_product.GD_OVERVIEW WHERE report_date BETWEEN month_start_date AND month_end_date;
	
			
		INSERT INTO UAT_PRODUCT.GD_OVERVIEW

		WITH kpis AS (
		
			SEL
				report_month AS report_month,
				branch_id,
				Cast('Payments' AS CHAR(30)) AS KPI,
				'Tele2' AS mobile_operator,
				Sum(PAYMENT_TOTAL_SUM) AS AC,
				NULL AS BU,
				NULL AS R1
			FROM PRD_RDS_V.PRODUCT_AGG_M_SAP_BO
			WHERE 1=1 
				AND report_month BETWEEN month_start_date AND month_end_date
			    AND CALC_PLATFORM_ID IN (-1,1,2)
			GROUP BY 1,2,3
			
			UNION ALL

			SEL
				Trunc(report_date,'mon') AS report_month,
				branch_id,
				Cast('Gross Intake' AS CHAR(30)) AS KPI,
				'Tele2' AS mobile_operator,
				Sum(CASE WHEN param_1='AC' THEN param_value end) AS AC,
				Sum(CASE WHEN param_1='BU' THEN param_value end) AS BU,
				Sum(CASE WHEN param_1 = rolling_value THEN param_value end) AS R1
			FROM uat_product.product_parameters
			WHERE 1=1 
			    AND param_2 IN ('Gross Intake')
				AND report_date BETWEEN month_start_date AND month_end_date
			GROUP BY 1,2,3
		
		
		UNION ALL
		
		SEL
			Trunc(report_date,'mon') AS report_month,
			branch_id,
			Cast('Churn' AS CHAR(30)) AS KPI,
			'Tele2' AS mobile_operator,
			Sum(CASE WHEN param_1='AC' THEN param_value end) AS AC,
			Sum(CASE WHEN param_1='BU' THEN param_value end) AS BU,
			Sum(CASE WHEN param_1 = rolling_value THEN param_value end) AS R1
		FROM uat_product.product_parameters
		WHERE 1=1
		    AND param_2 IN ('Churn')
			AND report_date BETWEEN month_start_date AND month_end_date
		GROUP BY 1,2,3
		
		UNION ALL
		
		SEL
			Trunc(report_date,'mon') AS report_month,
			branch_id,
			Cast('Net Intake' AS CHAR(30)) AS KPI,
			'Tele2' AS mobile_operator,
			Sum(CASE WHEN param_1='AC' THEN param_value end) AS AC,
			Sum(CASE WHEN param_1='BU' THEN param_value end) AS BU,
			Sum(CASE WHEN param_1 = rolling_value THEN param_value end) AS R1
		FROM uat_product.product_parameters
		WHERE 1=1
			AND report_date BETWEEN month_start_date AND month_end_date
		    AND param_2 IN ('Gross Intake','Churn')
		GROUP BY 1,2,3
		
		UNION ALL
		
			SEL
				report_month AS report_month,
				branch_id,
				Cast('Flash' AS CHAR(30)) AS KPI,
			'Tele2' AS mobile_operator,
			Sum(FLASH_ACTIVE_FLAG) AS AC,
			NULL AS BU,
			NULL AS R1
		FROM PRD_RDS_V.PRODUCT_AGG_M_SAP_BO
		WHERE 1=1 
			AND report_month = month_start_date
			AND CALC_PLATFORM_ID IN (-1,1,2)
		GROUP BY 1,2,3
			
		UNION ALL
		
		
		SEL Trunc(report_date,'mon') AS report_month,
			branch_id,
			'Market Shares' AS KPI,
			param_3 AS mobile_operator,
			Sum(param_value) AS AC,
			NULL AS BU,
			NULL AS R1
		FROM uat_product.product_parameters pp
		WHERE param_2 = 'Market Shares'
		    AND report_date BETWEEN month_start_date AND month_end_date
		GROUP BY 1,2,3,4
		
		
		UNION ALL
		
		
			SEL
				Trunc(report_date,'mon') AS report_month,
			branch_id,
			'Service revenue' AS KPI,
			'Tele2' AS mobile_operator,
			Sum(CASE WHEN param_1='AC' THEN param_value end) AS AC,
			Sum(CASE WHEN param_1='BU' THEN param_value end) AS BU,
			Sum(CASE WHEN param_1 = rolling_value THEN param_value end) AS R1
		FROM uat_product.product_parameters pp
		WHERE param_2 = 'Service revenue'
			AND report_date BETWEEN month_start_date AND month_end_date
		GROUP BY 1,2,3
		
		
		UNION ALL
		  
		
			SELECT
				Trunc(report_date,'mon') AS report_month,
			branch_id,
		    'Product Margin' AS KPI,
		    'Tele2' AS mobile_operator,
			Sum(CASE WHEN param_1='AC' THEN param_value end) AS AC,
			Sum(CASE WHEN param_1='BU' THEN param_value end) AS BU,
			Sum(CASE WHEN param_1 = rolling_value THEN param_value end) AS R1		
		FROM UAT_PRODUCT.PRODUCT_PARAMETERS
		WHERE 1=1 
		    AND param_2 IN ('Recurring revenue','Variable recurring costs')
			AND report_date BETWEEN month_start_date AND month_end_date
		GROUP BY 1,2,3
		
		
		UNION ALL
		  
		
		
		SEL report_month AS report_month,
			branch_id,
			'Live Subs' AS KPI,
			'Tele2' AS mobile_operator,
			Sum(LIVE_SUBS_COUNT) AS AC,
			NULL AS BU,
			NULL AS R1
		FROM PRD_RDS_V.PRODUCT_AGG_M_SAP_BO
		WHERE 1=1
			AND report_month = month_start_date
			AND CALC_PLATFORM_ID IN (-1,1,2)
		GROUP BY 1,2,3,4

		--	sel
		--		report_month as report_month,
		--		sap.branch_id,
		--		'Talking Subs' as KPI,
		--		'Tele2' as mobile_operator,
		--		sum(talking_subs_flag) as AC,
		--		null as BU,
		--		null as R1
		--	from PRD_RDS_V.PRODUCT_AGG_M_SAP_BO sap
		--	where report_month>=date'2018-01-01'
		--			and CALC_PLATFORM_ID in (-1,1,2)
		--	group by 1,2,3
		
		),
		
		
		branch AS (
			
				SEL DISTINCT
					b.branch_id,
					CASE branch_name WHEN 'Красноярск' THEN 'Сибирь' ELSE b.macro_cc_name end macro_cc_name,
				b.product_cluster_name,
				r.region_name
			FROM PRD2_DIC_V.BRANCH b
			INNER JOIN PRD2_DIC_V.REGION r
				ON b.region_id=r.region_id
			WHERE product_cluster_name IS NOT NULL
				AND branch_id IS NOT NULL
				AND b.branch_name NOT LIKE '%CDMA%'
				AND b.branch_name NOT LIKE '%MVNO%'
				AND b.branch_name NOT LIKE '%LTE450%'
				AND b.product_cluster_name<>'Deferred'
		        ), 
		
		            
		kpis_clean AS (
				SELECT
					branch.region_name,
					branch.macro_cc_name,
					branch.product_cluster_name,
					kpis.report_month,
					kpis.mobile_operator,
					kpis.KPI,
					Sum(kpis.AC) AS AC,
					Sum(kpis.BU) AS BU,
					Sum(kpis.R1) AS R1
				FROM kpis
				INNER JOIN branch
					ON kpis.branch_id=branch.branch_id
				GROUP BY 
					branch.region_name,
					branch.macro_cc_name,
					branch.product_cluster_name,
					kpis.report_month,
					kpis.mobile_operator,
					kpis.KPI
				),
		
		NPS AS (
				SEL
					region AS region_name,
					macroregion AS macro_cc_name,
					total AS total_russia_all,
					month_start_date AS report_date,
					mobile_operator,
					KPI,
					param_value AS AC,
					NULL AS BU,
					NULL AS R1
						--sel *
				FROM UAT_PRODUCT.TRACKING_PARAMETERS_OLD
				WHERE report_quarter = month_start_date
				),
			
		unions AS (
		
				----KPI для регионов все
		
			
			SEL 
				Cast(NULL AS CHAR(50)) AS GEOTYPE,		
				Cast('(All)' AS CHAR(50)) AS AREA,
				
				Cast(region_name AS CHAR(50)) AS region_name,
				Cast(macro_cc_name AS CHAR(50)) AS macro_cc_name,
				Cast(product_cluster_name AS CHAR(50)) AS product_cluster_name,
				
				Cast(report_month AS DATE) AS report_date,
				Cast(mobile_operator AS CHAR(5)) AS mobile_operator,
				Cast(KPI AS CHAR(30)) AS KPI,
				Sum(AC) AS AC,
				Sum(BU) AS BU,
				Sum(R1) AS R1
			FROM kpis_clean
			WHERE 1=1 --report_date >= date'2019-01-01'
				AND kpi<>'Market Shares'
			GROUP BY 1,2,3,4,5,6,7,8
			
			
			
			----специально для Market Shares, чтобы оконную функцию применить к территории целиком ниже
			UNION ALL
			
			
		(
			SEL --россия
				Cast('total' AS CHAR(50)) AS GEOTYPE,		
				Cast('(All)' AS CHAR(50)) AS AREA,
				
				Cast(NULL AS CHAR(50)) AS region_name,
				Cast(NULL AS CHAR(50)) AS macro_cc_name,
				Cast(NULL AS CHAR(50)) AS product_cluster_name,
				
				Cast(report_month AS DATE) AS report_date,
				Cast(mobile_operator AS CHAR(5)) AS mobile_operator,
				Cast(KPI AS CHAR(30)) AS KPI,
				Sum(AC) AS AC,
				Sum(BU) AS BU,
				Sum(R1) AS R1
			FROM kpis_clean
			WHERE 1=1 --report_date >= date'2019-01-01'
				AND kpi='Market Shares'
			GROUP BY 1,2,3,4,5,6,7,8
			
			
			UNION ALL
			
			SEL --макрорегионы
				Cast('macroregion' AS CHAR(50)) AS GEOTYPE,		
				Cast(macro_cc_name AS CHAR(50)) AS AREA,
				
				Cast(NULL AS CHAR(50)) AS region_name,
				Cast(macro_cc_name AS CHAR(50)) AS macro_cc_name,
				Cast(NULL AS CHAR(50)) AS product_cluster_name,
				
				Cast(report_month AS DATE) AS report_date,
				Cast(mobile_operator AS CHAR(5)) AS mobile_operator,
				Cast(KPI AS CHAR(30)) AS KPI,
				Sum(AC) AS AC,
				Sum(BU) AS BU,
				Sum(R1) AS R1
			FROM kpis_clean
			WHERE 1=1 --report_date >= date'2019-01-01'
				AND kpi='Market Shares'
			GROUP BY 1,2,3,4,5,6,7,8
			
			
			UNION ALL
			
			
			SEL --кластера
				Cast('cluster' AS CHAR(50)) AS GEOTYPE,		
				Cast(product_cluster_name AS CHAR(50)) AS AREA,
				
				Cast(NULL AS CHAR(50)) AS region_name,
				Cast(NULL AS CHAR(50)) AS macro_cc_name,
				Cast(product_cluster_name AS CHAR(50)) AS product_cluster_name,
		
				Cast(report_month AS DATE) AS report_date,
				Cast(mobile_operator AS CHAR(5)) AS mobile_operator,
				Cast(KPI AS CHAR(30)) AS KPI,
				Sum(AC) AS AC,
				Sum(BU) AS BU,
				Sum(R1) AS R1
			FROM kpis_clean
			WHERE 1=1 --report_date >= date'2019-01-01'
				AND kpi='Market Shares'
			GROUP BY 1,2,3,4,5,6,7,8
			
			UNION ALL
				
			SEL --регионы
				Cast('region' AS CHAR(50)) AS GEOTYPE,		
				Cast(region_name AS CHAR(50)) AS AREA,
				
				Cast(region_name AS CHAR(50)) AS region_name,
				Cast(NULL AS CHAR(50)) AS macro_cc_name,
				Cast(NULL AS CHAR(50)) AS product_cluster_name,
				
				Cast(report_month AS DATE) AS report_date,
				Cast(mobile_operator AS CHAR(5)) AS mobile_operator,
				Cast(KPI AS CHAR(30)) AS KPI,
				Sum(AC) AS AC,
				Sum(BU) AS BU,
				Sum(R1) AS R1
			FROM kpis_clean
			WHERE 1=1 --report_date >= date'2019-01-01'
				AND kpi='Market Shares'
			GROUP BY 1,2,3,4,5,6,7,8
			
		)
			
			
			
			
			
			---NPS, VFM, Consideration
			UNION ALL
			
			
		(
			SEL --регионы
				Cast('region' AS CHAR(50)) AS GEOTYPE,	
				Cast(region_name AS CHAR(50)) AS AREA,
				
				region_name,
				Cast(NULL AS CHAR(50)) AS macro_cc_name,
				Cast(NULL AS CHAR(50)) AS product_cluster_name,
				
				
				report_date,
				mobile_operator,
				KPI,
				AC,
				BU,
				R1
			FROM NPS
			WHERE 1=1 --report_date >= date'2018-01-01'	
				AND REGION_NAME IS NOT NULL
			
			UNION ALL
			
			
			SEL --макрорегионы
				Cast('macroregion' AS CHAR(50)) AS GEOTYPE,		
				Cast(macro_cc_name AS CHAR(50)) AS AREA,
				
				Cast(NULL AS CHAR(50)) AS region_name,
				macro_cc_name,
				Cast(NULL AS CHAR(50)) AS product_cluster_name,
				
				report_date,
				mobile_operator,
				KPI,
				AC,
				BU,
				R1
			FROM NPS
			WHERE 1=1 --report_date >= date'2018-01-01'	
				AND macro_cc_name IS NOT NULL
			
			UNION ALL
			
			SEL --тотал
				Cast('total' AS CHAR(50)) AS GEOTYPE,	
				Cast('(All)' AS CHAR(50)) AS AREA,
				
				Cast(NULL AS CHAR(50)) AS region_name,
				Cast(NULL AS CHAR(50)) AS macro_cc_name,
				Cast(NULL AS CHAR(50)) AS product_cluster_name,
				
				report_date,
				mobile_operator,
				KPI,
				AC,
				BU,
				R1
			FROM NPS
			WHERE 1=1 --report_date >= date'2018-01-01'	
					AND total_russia_all IS NOT NULL
					
				)
			
					
					
					
			),
			
			
		final_calc AS (		
		SEL
			GEOTYPE,		
			AREA,
			region_name,
			macro_cc_name,
			product_cluster_name,
			report_date,
			mobile_operator,
			KPI,
			
			CASE WHEN KPI='Market Shares'
					THEN Round(AC/(Sum(AC) Over (PARTITION BY GEOTYPE, AREA, report_date, KPI)),4)
					ELSE AC end AS AC,
			BU,
			R1
		FROM unions
		)
		
					SEL
						GEOTYPE,		
						AREA,
						region_name,
						macro_cc_name,
						product_cluster_name,
						report_date,
						mobile_operator,
						KPI,
						AC,
						BU,
						Coalesce(R1, AC) AS R1
					FROM final_calc
					WHERE report_date = month_start_date
	
	
--	)
--	
--with no data
--primary index(GEOTYPE,		
--		AREA,
--		region_name,
--		macro_cc_name,
--		product_cluster_name,
--		report_date,
--		mobile_operator,
--		KPI)
	
;
		END;
	
	SET month_start_date = Add_Months(month_start_date, 1);
	SET month_end_date = Last_Day(month_start_date);
	END WHILE;
	


END;