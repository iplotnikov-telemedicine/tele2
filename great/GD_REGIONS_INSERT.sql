REPLACE PROCEDURE UAT_PRODUCT.GD_REGIONS_INSERT (IN input_month DATE, IN months_forward_from_input_month INTEGER)

--call UAT_PRODUCT.GD_REGIONS_INSERT ('2020-01-01', 12)
--call UAT_PRODUCT.GD_REGIONS_INSERT ('2019-01-01', 22)
--delete from UAT_PRODUCT.GD_REGIONS

SQL SECURITY INVOKER

BEGIN

	DECLARE month_start_date DATE;
	DECLARE month_end_date DATE;
	DECLARE last_month DATE;
	
	SET month_start_date = input_month;
	SET month_end_date = Last_Day(month_start_date);
	SET last_month = Add_Months(input_month, months_forward_from_input_month);

	DELETE FROM uat_product.GD_REGIONS WHERE report_month>=month_start_date AND report_month < last_month;
	WHILE month_start_date < last_month DO 
	
		BEGIN
			

INSERT INTO uat_product.GD_REGIONS
--

--sel distinct channel from uat_product.gd_sales_mix

--sel 	
--	'total' as total,
--	report_month,
--	sum(case KPI when 'Recurring Revenue' then AC end),
--	sum(case KPI when 'Revenue' then AC end)
--from uat_product.GD_REGIONS
--group by 1,2



--GM1
--GM1 %

--EBITDA n/c
--EBITDA n/c %

--ADU % (Active Data Users)



WITH kpis AS (

		
		SEL
			report_date,
			param_1,
			param_2,
			branch_id,
			Lower(base_type) AS base_type,
			tariff_1,
			param_value
		--help table uat_product.product_parameters
		--sel top 100 *
		--sel distinct param_2 FROM uat_product.product_parameters
		FROM uat_product.product_parameters
		WHERE 1=1 --report_date>=date'2018-01-01'
			AND report_date BETWEEN month_start_date AND month_end_date
			AND param_1 IN ('AC','BU')
			AND param_2 IN (
					'Churn',
					'Service revenue',
					'Service Revenue (w/o IC&Content)',
					'Variable recurring costs',
					'Total Minutes technical',
					'Recurring Revenue',
					'Gross Intake',
					'Number of subscribers',
					'Average number of subscribers',
					'Gross Margin 1',
					'Revenue',
					'DATA traffic',
					'Service revenue (w/o interconnect)'
					)
			
					
		UNION ALL		
		
		
		SEL --Payments
			report_month AS report_date,
			'AC' AS param_1,
			'Payments' AS param_2,
			branch_id,
			Lower(base_type) AS base_type,
			Cast(NULL AS CHAR(400)) AS tariff_1,
			Sum(PAYMENT_TOTAL_SUM) AS param_value
				--help view PRD_RDS_V.PRODUCT_AGG_M_SAP_BO
		FROM PRD_RDS_V.PRODUCT_AGG_M_SAP_BO
		WHERE 1=1 --report_date>=date'2018-01-01'
		    AND CALC_PLATFORM_ID IN (-1,1,2)
		    AND report_date=month_start_date
		GROUP BY 1,2,3,4,5
		
		
		UNION ALL		
		
		
		SEL --REVENUE_TOTAL_WO_IC
			report_month AS report_date,
			'AC' AS param_1,
			'REVENUE_TOTAL_WO_IC' AS param_2,
			branch_id,
			Lower(base_type) AS base_type,
			Cast(NULL AS CHAR(400)) AS tariff_1,
			Sum(REVENUE_TOTAL_WO_IC) AS param_value
				--help view PRD_RDS_V.PRODUCT_AGG_M_SAP_BO
		FROM PRD_RDS_V.PRODUCT_AGG_M_SAP_BO
		WHERE 1=1 --report_date>=date'2018-01-01'
		    AND CALC_PLATFORM_ID IN (-1,1,2)
		    AND report_date=month_start_date
		GROUP BY 1,2,3,4,5          
		
		
	
		UNION ALL
		
		
		
		SEL --ADU 100
			report_month AS report_date,
			'AC' AS param_1,
			'ADU 100' AS param_2,
			branch_id,
			Lower(base_type) AS base_type,
			Cast(NULL AS CHAR(400)) AS tariff_1,
			Sum(DATA_ACTIVE_USER_100_COUNT) AS param_value
				--help view PRD_RDS_V.PRODUCT_AGG_M_SAP_BO
		FROM PRD_RDS_V.PRODUCT_AGG_M_SAP_BO
		WHERE 1=1 --report_date>=date'2018-01-01'
		    AND CALC_PLATFORM_ID IN (-1,1,2)
		    AND report_date=month_start_date
		GROUP BY 1,2,3,4,5


		
		UNION ALL
		
		--Live Subs
		/*SEL
			report_month AS report_date,
			'AC' AS param_1,
			'Live Subs' AS param_2,
			branch_id,
			Cast(NULL AS CHAR(4)) AS base_type,
			Cast(NULL AS CHAR(400)) AS tariff_1,
			Sum(cnt_subs) AS param_value
		FROM UAT_PRODUCT.DV_LIVE_SUBS
		WHERE 1=1
			AND report_date=month_start_date
		GROUP BY 1,2,3,4*/
		
		SEL report_month AS report_date,
			'AC' AS param_1,
			'Live Subs' AS param_2,
			branch_id,
			Cast(NULL AS CHAR(4)) AS base_type,
			Cast(NULL AS CHAR(400)) AS tariff_1,
			Sum(LIVE_SUBS_COUNT) AS param_value
		FROM PRD_RDS_V.PRODUCT_AGG_M_SAP_BO
		WHERE 1=1
			AND report_month = month_start_date
			AND CALC_PLATFORM_ID IN (-1,1,2)
		GROUP BY 1,2,3,4
		
		
		UNION ALL
		
		--Talking Subs
			SEL
				sap.report_month AS report_date,
				'AC' AS param_1,
				'Talking Subs' AS param_2,
				sap.branch_id,
				Lower(base_type) AS base_type,
				Cast(NULL AS CHAR(400)) AS tariff_1,
				Sum(talking_subs_flag) AS param_value
				--help table PRD_RDS_V.PRODUCT_AGG_M_SAP_BO 
				--sel top 100 *
			FROM PRD_RDS_V.PRODUCT_AGG_M_SAP_BO sap
			WHERE 1=1
					AND CALC_PLATFORM_ID IN (-1,1,2)
					AND report_month=month_start_date
			GROUP BY 1,2,3,4,5,6
		
			UNION ALL
			
			--Survived in the 3rd month talking subs
			SEL
				sap.gross_month AS report_date,
				'AC' AS param_1,
				'Survived talking subs' AS param_2,
				sap.branch_id,
				Lower(base_type) AS base_type,
				Cast(NULL AS CHAR(400)) AS tariff_1,
				Sum(talking_subs_flag) AS param_value
				--help table PRD_RDS_V.PRODUCT_AGG_SUBS_M
			FROM PRD_RDS_V.PRODUCT_AGG_SUBS_M sap
			WHERE 1=1
				AND CALC_PLATFORM_ID IN (-1,1,2)
				AND gross_month=Add_Months(month_start_date, -2)
				AND report_month=month_start_date
				AND gross_month>=DATE'2018-01-01'
			GROUP BY 1,2,3,4,5,6

			UNION ALL
			
			--Survived in the 3rd month live subs
			SEL
				sap.gross_month AS report_date,
				'AC' AS param_1,
				'Survived live subs' AS param_2,
				sap.branch_id,
				Lower(base_type) AS base_type,
				Cast(NULL AS CHAR(400)) AS tariff_1,
				Sum(live_subs) AS param_value
				--help table PRD_RDS_V.PRODUCT_AGG_SUBS_M
			FROM PRD_RDS_V.PRODUCT_AGG_SUBS_M sap
			WHERE 1=1
				AND CALC_PLATFORM_ID IN (-1,1,2)
				AND gross_month=Add_Months(month_start_date, -2)
				AND report_month=month_start_date
				AND gross_month>=DATE'2018-01-01'
			GROUP BY 1,2,3,4,5,6
			
			
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
        )

            
		SELECT
			Trunc(kpis.report_date,'mon') AS report_month,
			
			'total' AS total,
			branch.product_cluster_name,
			branch.macro_cc_name,
			branch.region_name,
			branch.branch_id,
			
			kpis.param_2 AS KPI,
			kpis.tariff_1 AS bundle_or_not,
			kpis.base_type,
			
			Sum(CASE WHEN kpis.param_1='AC' THEN kpis.param_value end) AS AC,
			Sum(CASE WHEN kpis.param_1='BU' THEN kpis.param_value end) AS BU
		FROM kpis
		INNER JOIN branch
			ON kpis.branch_id=branch.branch_id
		WHERE 1=1
		GROUP BY 
			1,2,3,4,5,6,7,8,9
			
			

;
		END;
	
	SET month_start_date = Add_Months(month_start_date, 1);
	SET month_end_date = Last_Day(month_start_date);
	END WHILE;

END;

--drop table uat_product.GD_REGIONS;
--create multiset table uat_product.GD_REGIONS as 
--
--	(
--
--
--
--with kpis as (
--
--		sel
--			report_date,
--			param_1,
--			param_2,
--			branch_id,
--			lower(base_type) as base_type,
--			tariff_1,
--			param_value
--		--help table uat_product.product_parameters
--		--sel top 100 *
--		--sel distinct param_2
--		from uat_product.product_parameters
--		where 1=1 --report_date>=date'2018-01-01'
--			and report_date>=date'2018-01-01'
--			and param_1 in ('AC','BU')
--			and param_2 in (
--					'Churn',
--					'Service revenue',
--					'Service Revenue (w/o IC&Content)',
--					'Variable recurring costs',
--					'Total Minutes technical',
--					'Recurring Revenue',
--					'Gross Intake',
--					'Number of subscribers',
--					'Average number of subscribers',
--					'Gross Margin 1',
--					'Revenue'
--					)
--			
--		union all
--		
--		--Live Subs
--		sel
--			report_month as report_date,
--			'AC' as param_1,
--			'Live Subs' as param_2,
--			branch_id,
--			cast(null as char(4)) as base_type,
--			cast(null as char(400)) as tariff_1,
--			sum(cnt_subs) as param_value
--			--sel top 100 *
--			--help table uat_product.dv_ts_new_result_2
--		from uat_product.dv_ts_new_result_2
--		where 1=1
--			and report_month>=date'2018-01-01'
--		group by 1,2,3,4,5,6
--			
--		
--		union all
--		
--		--Talking Subs
--			sel
--				sap.report_month as report_date,
--				'AC' as param_1,
--				'Talking Subs' as param_2,
--				sap.branch_id,
--				lower(base_type) as base_type,
--				cast(null as char(400)) as tariff_1,
--				sum(talking_subs_flag) as param_value
--				--help table PRD_RDS_V.PRODUCT_AGG_M_SAP_BO 
--				--sel top 100 *
--			from PRD_RDS_V.PRODUCT_AGG_M_SAP_BO sap
--			where 1=1
--					and CALC_PLATFORM_ID in (-1,1,2)
--					and report_month>=date'2018-01-01'
--			group by 1,2,3,4,5,6
--		
--			union all
--			
--			--Survived in the 3rd month
--			sel
--				sap.gross_month as report_date,
--				'AC' as param_1,
--				'Survived in the 3rd month' as param_2,
--				sap.branch_id,
--				lower(base_type) as base_type,
--				cast(null as char(400)) as tariff_1,
--				sum(talking_subs_flag) as param_value
--				--LAG(gross_subs_flag, 2, 0)  OVER (PARTITION BY subs_id ORDER BY report_month) AS sal_prev --IGNORE NULLS
--				--help table PRD_RDS_V.PRODUCT_AGG_SUBS_M
--			from PRD_RDS_V.PRODUCT_AGG_SUBS_M sap
--			where 1=1
--				and CALC_PLATFORM_ID in (-1,1,2)
--				and gross_month=ADD_MONTHS(sap.report_month, -2)
--				and gross_month >= date'2018-01-01'
--			group by 1,2,3,4,5,6
--			
----			union all
----			
----			--Acive Data Users (ADU) >100                                                             
----			sel
----				sap.report_month as report_date,
----				'AC' as param_1,
----				'Active Data Users (100MB)' as param_2,
----				sap.branch_id,
----				lower(base_type) as base_type,
----				cast(null as char(400)) as tariff_1,
----				sum(DATA_ACTIVE_USER_100_FLAG) as param_value
----				--help view PRD_RDS_V.PRODUCT_AGG_SUBS_M
----			from PRD_RDS_V.PRODUCT_AGG_SUBS_M sap
----			where 1=1
----				and CALC_PLATFORM_ID in (-1,1,2)
----				and report_month >= date'2018-01-01'
----			group by 1,2,3,4,5,6
--
--
--			
--			
--		),
--		
--branch as (
--	
--		sel DISTINCT
--			b.branch_id,
--			case branch_name when 'Красноярск' then 'Сибирь' else b.macro_cc_name end macro_cc_name,
--		b.product_cluster_name,
--		r.region_name
--	from PRD2_DIC_V.BRANCH b
--	inner join PRD2_DIC_V.REGION r
--		on b.region_id=r.region_id
--	where product_cluster_name is not null
--		and branch_id is not null
--		and b.branch_name not like '%CDMA%'
--		and b.branch_name not like '%MVNO%'
--		and b.branch_name not like '%LTE450%'
--		and b.product_cluster_name<>'Deferred'
--        )
--
--            
--		select
--			trunc(kpis.report_date,'mon') as report_month,
--			
--			'total' as total,
--			branch.product_cluster_name,
--			branch.macro_cc_name,
--			branch.region_name,
--			branch.branch_id,
--			
--			kpis.param_2 as KPI,
--			kpis.tariff_1 as bundle_or_not,
--			kpis.base_type,
--			
--			sum(case when kpis.param_1='AC' then kpis.param_value end) as AC,
--			sum(case when kpis.param_1='BU' then kpis.param_value end) as BU
--		from kpis
--		inner join branch
--			on kpis.branch_id=branch.branch_id
--		where 1=1
--		group by 
--			1,2,3,4,5,6,7,8,9
--		
--				)
--with no data
--primary index(report_month, region_name, KPI, bundle_or_not, base_type)
--
--;

