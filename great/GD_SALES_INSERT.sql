REPLACE procedure UAT_PRODUCT.GD_SALES_INSERT (in input_month date, in months_forward_from_input_month integer)

--sel top 100 * from UAT_PRODUCT.GD_SALES
--sel report_month, count(*) from UAT_PRODUCT.GD_SALES group by 1 order by 1
--call UAT_PRODUCT.GD_SALES_INSERT ('2018-01-01', 31)
--call UAT_PRODUCT.GD_SALES_INSERT ('2020-01-01', 9)
--sel count(*) from UAT_PRODUCT.GD_SALES
--sel min(report_month) from UAT_PRODUCT.GD_SALES
--delete from UAT_PRODUCT.GD_SALES where report_month=date'2019-10-01'
--sel report_month, count(*) from UAT_PRODUCT.GD_SALES group by 1 order by 1
--sel max(report_date) from UAT_PRODUCT.GD_OVERVIEW

--select database_name, table_name, max_report_date
----select *
--from PRD2_TMD_V.BDS_LOAD_STATUS
--WHERE DATABASE_NAME in ('prd2_bds_v') 
--	and TABLE_NAME in ('subs_clr_d')
--
--union
--
--select database_name, table_name, max_report_date
--from PRD2_TMD_V.DDS_LOAD_STATUS
--WHERE DATABASE_NAME in ('prd2_dds_v','prd2_dds_v2') 
--and TABLE_NAME in ('roaming','usage_billing')




SQL SECURITY INVOKER

BEGIN

	DECLARE month_start_date date;
	DECLARE month_end_date date;
	DECLARE last_month date;
	
	SET month_start_date = input_month;
	SET month_end_date = last_day(month_start_date);
	SET last_month = add_months(input_month, months_forward_from_input_month);

	delete from uat_product.GD_SALES where report_month>=month_start_date and report_month<last_month;
	
	WHILE month_start_date < last_month DO 
	
		BEGIN
			
		insert into uat_product.GD_SALES	
		with clrd as (
		
		        select
						report_date,
						SEGM_ARPU_RUS_NAME,
						subs_id
				from PRD2_BDS_V.SUBS_CLR_D
				--inner join PRD2_BDS_V.SUBS_CLR_D clrd
					--on months.report_month = clrd.report_date
				where CALC_PLATFORM_ID in (-1,1,2)
					and report_date = month_end_date
        		)
        		
--        		,
--    
--		content_rev as (		
--				sel trunc(report_date,'mon') as report_month,
--					subs_id,
--					sum(revenue_vas_content) as content_rev
--				from PRD_RDS_V.PRODUCT_SUBS_REG_D
--				where report_date between month_start_date and month_end_date
--					and calc_platform_id in (-1,1,2)
--				group by 1,2
--				)
		
					
				sel
					agg.report_month,
					case when agg.gross_month < date'2019-06-01' then null else agg.gross_month end as gross_month,
					--география
					agg.branch_id,
					agg.region_name,
					agg.macroregion,
					agg.product_cluster_name,
					
					--месяц жизни
					agg.cohort,
					
					
			
					case agg.cohort
						when '1' then --если 1, то смотрим тариф в день продажи, если нет, то тариф на конец месяца
							CASE WHEN d2.name_report IN ('Классический','Мой разговор','Мой онлайн','Безлимит','Везде онлайн') THEN d2.name_report
							WHEN d2.name_report IN ('Лайт', 'Мой Tele2','Лайт/Мой Tele2') THEN 'Мой Tele2'
						    WHEN d2.name_report IN ('Мой онлайн +', 'Мой онлайн+') THEN 'Мой онлайн+'       
						    WHEN d2.name_report IN ('Везде онлайн +', 'Везде онлайн+') THEN 'Везде онлайн+'
						    ELSE 'Other' end
						else
							CASE WHEN d1.name_report IN ('Классический','Мой разговор','Мой онлайн','Безлимит','Везде онлайн') THEN d1.name_report
							WHEN d1.name_report IN ('Лайт', 'Мой Tele2','Лайт/Мой Tele2') THEN 'Мой Tele2'
						    WHEN d1.name_report IN ('Мой онлайн +', 'Мой онлайн+') THEN 'Мой онлайн+'       
						    WHEN d1.name_report IN ('Везде онлайн +', 'Везде онлайн+') THEN 'Везде онлайн+'
						    ELSE 'Other' end
						end as NAME_REPORT,  
						
					case when clrd.SEGM_ARPU_RUS_NAME like '%High%' then 'High'
							when clrd.SEGM_ARPU_RUS_NAME like '%Low%' then 'Low'
							when clrd.SEGM_ARPU_RUS_NAME like '%Medium%' then 'Medium'
							else 'New'
							end as SEGM_ARPU_RUS_NAME,
				
					--признаки MNP
					--agg.MNP_PORTATION_IN_COUNT ,                                                             
					--agg.MNP_PORTATION_OUT_COUNT ,                                                                  
				 
					--канал продаж			
					--MB, LA, LS, FA, FS, Internet			
					case	agg.SALES_CHANNEL_GRP	
						when	'Tele2Start' then 'T2ST'
						when	'On-Line'	then	'Internet'	
						when	'Monobrand Shops (L)'	then	'MB'
						when	'Monobrand Stands'	then	'Stands'
						when	'Monobrand Modules'	then	'Stands'
						when	'Federal Alternative Dealer'	then	'FA'	
						when	'Local Telecom Dealer'	then	'LS'	
						when	'Federal Telecom Dealer'	then	'FS'	
						when	'Local Alternative Dealer'	then	'LA'
						else	'Other' --Без указания канала сбыта + Unknown SCH
					end as SALES_CHANNEL_SHORT,

					sum(case agg.FLASH_ACTIVE_FLAG
						when 1 then agg.REVENUE_TOTAL_WO_IC else 0 end) as ARPU_SUM,
					sum(case agg.FLASH_ACTIVE_FLAG
						when 0 then agg.REVENUE_TOTAL_WO_IC else 0 end) as ARPU_NON_FLASH_SUM,
					
						sum(agg.PREV_FLASH_ACTIVE_FLAG) as PREV_FLASH_SUBS_COUNT,  
					
					--FLASH + NON FLASH = SUBS COUNT
					sum(agg.FLASH_ACTIVE_FLAG) AS FLASH_ACTIVE_COUNT,
					count(case agg.FLASH_ACTIVE_FLAG when 0 then 1 end) AS NON_FLASH_ACTIVE_COUNT,
					
					sum(case when agg.disconnect_count>agg.reconnect_count then agg.churn_flag end) as FLASH_CHURN,
					sum(agg.churn_flag) as TOTAL_CHURN
					
				from PRD_RDS_V.PRODUCT_AGG_SUBS_M agg
				left join PRD2_DIC_V.PRICE_PLAN d1
					on agg.tp_id=d1.tp_id
				left join PRD2_DIC_V.PRICE_PLAN d2
					on agg.gross_tp_id=d2.tp_id	
				left join clrd
					on agg.subs_id = clrd.subs_id
--				left join content_rev
--					on content_rev.subs_id=agg.subs_id
				where 1=1
					and agg.calc_platform_id in (-1,1,2)
					and agg.cohort not in ('0')
					and agg.report_month = month_start_date
				group by 1,2,3,4,5,6,7,8,9,10
			
				
		
		;
		END;
	
	SET month_start_date = add_months(month_start_date, 1);
	SET month_end_date = last_day(month_start_date);
	END WHILE;

END;