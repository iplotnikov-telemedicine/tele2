REPLACE procedure UAT_PRODUCT.GD_SALES_PRODUCT_BRAND_NEW_INSERT (in input_month date, in months_forward_from_input_month integer)
--call UAT_PRODUCT.GD_SALES_PRODUCT_BRAND_NEW_INSERT(date'2020-01-01', 9)
--show procedure UAT_PRODUCT.GD_SALES_PRODUCT_BRAND_NEW_INSERT
--sel distinct report_month from UAT_PRODUCT.GD_SALES_PRODUCT_BRAND_NEW order by 1

--sel * from UAT_PRODUCT.GD_SALES_PRODUCT_BRAND_NEW where gross_count is not null sample 10

--total russia
--cluster
--macroregion
--region
--
--report month
--lifetime
--segment
--channel
--tariff
--bundle/PAYG
--flash/non-flash
--live/non-live
--voice_user
--data_user
--
--sum(subs_count)
--sum(MINUTES)
--sum(DATA)
--sum(REVENUE_TOTAL_WO_IC) ARPU
--sum(SERVICE REVENUE) ARPU with IC
--sum(GM1)
--sum(PM)



SQL SECURITY INVOKER

BEGIN

	DECLARE loop_month date;
	DECLARE last_month date;	
	
	SET loop_month = input_month;
	SET last_month = add_months(input_month, months_forward_from_input_month);

	delete from UAT_PRODUCT.GD_SALES_PRODUCT_BRAND_NEW where report_month >= loop_month and report_month < last_month;
	
	WHILE loop_month < last_month DO 
	
		BEGIN
	
			
		insert into UAT_PRODUCT.GD_SALES_PRODUCT_BRAND_NEW
with branch as (
			
				sel DISTINCT
					b.branch_id,
					case branch_name when 'Красноярск' then 'Сибирь' else b.macro_cc_name end macro_cc_name,
				b.product_cluster_name,
				r.region_name
			from PRD2_DIC_V.BRANCH b
			inner join PRD2_DIC_V.REGION r
				on b.region_id=r.region_id
			where product_cluster_name is not null
				and branch_id is not null
				and b.branch_name not like '%CDMA%'
				and b.branch_name not like '%MVNO%'
				and b.branch_name not like '%LTE450%'
				and b.product_cluster_name<>'Deferred'
		        ),
		        
unioned as (
			sel
				calc_month as report_month,
				(months_between(calc_month, act_month) + 1) as lifetime,
				
				branch_id,
				gross_tp_id,
				tp_id, 
				sales_channel_grp,
				
				sum(gross) as gross_count,
				sum(reconnect) as reconnect_count,
				sum(disconnect) as disconnect_count,
				sum(reconnect+disconnect) as churn_count,
				
				sum(subs_flash) as flash_count,
				sum(live_subs) as live_count,
				
				sum(ser_rev_without_ic) as serv_rev_wo_itc, --ARPU
				sum(service_revenue) as serv_rev,
				
				sum(out_t2_reg + out_mob + out_fix + out_mg + out_mn + in_mob + in_fix) as voice_min,
				sum(data_mb) as data_mb
				
				--help table uat_product.dv_kpi_new_subs
			from UAT_PRODUCT.DV_KPI_NEW_SUBS
			where calc_month = loop_month
				and lifetime between 1 and 12
			group by 1,2,3,4,5,6
			
			
--			union all
--			
--			
--			sel
--				gross_month as report_month,
--				(months_between(report_month, gross_month) + 1) as lifetime,					
--				branch_id,
--				gross_tp_id,
--				tp_id, 
--				sales_channel_grp,
--				
--				sum(agg.gross_subs_flag) as gross_count,
--				null as flash_count,
--				null as live_count,
--				
--				null as serv_rev_wo_itc, --ARPU
--				null as serv_rev,
--				
--				null as voice_min,
--				null as data_mb
--				
--				--help view PRD_RDS_V.PRODUCT_AGG_SUBS_M
--			from PRD_RDS_V.PRODUCT_AGG_SUBS_M agg
--			where  agg.gross_month = loop_month
--				and agg.gross_month = agg.report_month
--				and agg.calc_platform_id in (-1,1,2)
--			group by 1,2,3,4,5,6
			
			
)	

	
	
	
		        
sel
	report_month,
	lifetime,
	
	branch.region_name,
	branch.macro_cc_name,
	branch.product_cluster_name,
				
	CASE WHEN d2.name_report IN ('Классический','Мой разговор','Мой онлайн','Безлимит','Везде онлайн') THEN d2.name_report
	WHEN d2.name_report IN ('Лайт', 'Мой Tele2','Лайт/Мой Tele2') THEN 'Мой Tele2'
    WHEN d2.name_report IN ('Мой онлайн +', 'Мой онлайн+') THEN 'Мой онлайн+'       
    WHEN d2.name_report IN ('Везде онлайн +', 'Везде онлайн+') THEN 'Везде онлайн+'
    ELSE 'Other' end gross_name_report,  
	
	CASE WHEN d1.name_report IN ('Классический','Мой разговор','Мой онлайн','Безлимит','Везде онлайн') THEN d1.name_report
	WHEN d1.name_report IN ('Лайт', 'Мой Tele2','Лайт/Мой Tele2') THEN 'Мой Tele2'
    WHEN d1.name_report IN ('Мой онлайн +', 'Мой онлайн+') THEN 'Мой онлайн+'       
    WHEN d1.name_report IN ('Везде онлайн +', 'Везде онлайн+') THEN 'Везде онлайн+'
    ELSE 'Other' end name_report,  
	
	case d1.BUNDLE_FLAG when 1 then 'Bundle' else 'PAYG' end bundle_payg,	
	
	case SALES_CHANNEL_GRP		
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
	end sales_channel_short,
	
	gross_count,
	reconnect_count,
	disconnect_count,
	churn_count,
	flash_count,
	live_count,
	serv_rev_wo_itc, --ARPU
	serv_rev,	
	voice_min,
	data_mb

from unioned
left join branch
	on unioned.branch_id=branch.branch_id
left join PRD2_DIC_V.PRICE_PLAN d1
	on unioned.tp_id=d1.tp_id
left join PRD2_DIC_V.PRICE_PLAN d2
	on unioned.gross_tp_id=d2.tp_id	


;

END;
	
	SET loop_month = add_months(loop_month, 1);
	END WHILE;
	
END;