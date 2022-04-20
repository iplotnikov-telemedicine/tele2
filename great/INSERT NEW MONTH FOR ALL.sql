CALL UAT_PRODUCT.GD_MONTHLY_MIGRATIONS_INSERT(DATE'2021-04-01', 1);
--источники: 
--миграции 2 раза в месяц
	--1 в первых чилах следующего
	--2 когда обновятся флаги CBM
	
CALL UAT_PRODUCT.GD_WEEKLY_INSERT();
--источники: 
	--каждую неделю
	
CALL UAT_PRODUCT.GD_OVERVIEW_INSERT(DATE'2021-04-01', 1, 'F1_2021_01'); --поменять на F, если есть R, иначе AC
--источники: 
	--2 раза в месяц
		--1 в первых числах месяца
		--2 после обновления SAP
		
CALL UAT_PRODUCT.GD_REGIONS_INSERT (DATE'2021-04-01', 1);
--источники: 
	--2 раза в месяц
		--1 в первых числах месяца
		--2 после обновления SAP
			
CALL UAT_PRODUCT.GD_SALES_PRODUCT_INSERT(DATE'2021-04-01', 1);
--источники: 
	--1 раз в месяц в первых числах
	
CALL UAT_PRODUCT.GD_SUBS_BASE_INSERT (DATE'2021-04-01', 1);
--источники: 
	--1 раз в месяц в первых числах



/*

sel Max(report_month) from UAT_PRODUCT.GD_MONTHLY_MIGRATIONS
union all
sel Max(report_date) from UAT_PRODUCT.GD_OVERVIEW
union all 
sel Max(report_month) from UAT_PRODUCT.GD_REGIONS
union all
sel Max(report_month) from UAT_PRODUCT.GD_SALES_MIX
union all
sel Max(report_month) from UAT_PRODUCT.GD_SALES_PRODUCT
*/



--CALL UAT_PRODUCT.GD_SALES_PRODUCT_BRAND_NEW_INSERT(DATE'2020-10-01', 1);

/*
SELECT		
	report_date,
	KPI,
	Sum(AC),
	Sum(BU),
	Sum(R1)
FROM UAT_PRODUCT.GD_OVERVIEW
WHERE report_date = DATE'2020-11-01'
GROUP BY  1,2
ORDER BY 1,2*/





--
--sel 
--	*
--from UAT_PRODUCT.GD_OVERVIEW
--where 1=1
--	and report_date>=date'2019-10-01'
--	and KPI = 'Market Shares'
--	and area = '(All)'
--group by 1,2
--order by 1,2
--
--
--
--
--sel 
--	trunc(report_date,'mon') as report_month,
--	param_2,
--	sum(param_value)
--from UAT_PRODUCT.product_parameters
--where report_month = date'2020-09-01'
--	and param_2 = 'Market Shares'
--group by 1,2
--
--
--sel
--	trunc(report_date,'mon') as report_date,
--	cast('Market Shares' as char(30)) as KPI,
--	'Tele2' as mobile_operator,
--	sum(case when param_1='AC' then param_value end) as AC,
--	sum(case when param_1='BU' then param_value end) as BU,
--	sum(case when param_1 like '1' then param_value end) as R1
--from uat_product.product_parameters
--where 1=1
--	and report_date>= date'2020-01-01'
--    and param_2 in ('Market Shares')
--group by 1,2,3
--	
--
--
--select distinct param_2 from uat_product.product_parameters
--
--
--sel report_date,
--	sum(param_value)
--from uat_product.product_parameters
--where param_2 in ('Market Shares')
--group by 1
--order by 1
--
--
--
--
--sel trunc(report_date,'mon') as report_month,
--			'Market Shares' as KPI,
--			sum(param_value) as AC,
--			null as BU,
--			null as R1
--		from uat_product.product_parameters pp
--		where param_2 = 'Market Shares'
--		    and report_month>=date'2018-05-01'
--		group by 1,2