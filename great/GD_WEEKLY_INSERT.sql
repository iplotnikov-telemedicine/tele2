REPLACE procedure UAT_PRODUCT.GD_WEEKLY_INSERT()

--sel distinct NAME_REPORT from UAT_PRODUCT.GD_WEEKLY
--sel distinct param from UAT_PRODUCT.GD_WEEKLY
--sel top 100 * from UAT_PRODUCT.GD_WEEKLY
--sel count(*) from UAT_PRODUCT.GD_WEEKLY_INSERT
--call UAT_PRODUCT.GD_WEEKLY_INSERT()
--delete from UAT_PRODUCT.GD_WEEKLY where report_date between (date'2019-07-01') and (date'2019-09-01')



SQL SECURITY INVOKER

BEGIN

	DECLARE week_start_date date;
	DECLARE week_end_date date;
	DECLARE iterations integer;
	DECLARE last_sunday date;
	
	
	--находим последнее загруженное воскресенье
	sel trunc(max(report_date),'iw')-1 
	into last_sunday
	from uat_product.DP_TP_MIGRATION;

	--берем последнюю целую неделю
	SET week_end_date = last_sunday;
	SET week_start_date = last_sunday - 6;
	SET iterations = 0;
	
	delete from uat_product.GD_WEEKLY; 
	WHILE iterations < 4 DO 
	
		BEGIN
		
		insert into uat_product.GD_WEEKLY	
		with 
			
			
		branch as (
		
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
	
	        ) 
	   
   
	    
		
		select
				cast('MIGR' as varchar(5)) as param,
				weeknumber_of_year(ch.report_date, 'ISO') as week_number,        
				trunc(ch.report_date, 'iw')  as report_week,              
				branch.region_name,
				branch.macro_cc_name,
				branch.product_cluster_name,
				   
				--case when tpprev.name_report in ('Классический','Мой разговор','Везде онлайн','Мой Tele2','Мой онлайн',
										--'Мой онлайн+','Безлимит') then tpprev.name_report
									--else 'Other' end as name_report_prev,
											
				CASE WHEN tp.name_report IN ('Классический','Мой разговор','Мой онлайн','Безлимит','Везде онлайн') THEN tp.name_report
				WHEN tp.name_report IN ('Лайт', 'Мой Tele2','Лайт/Мой Tele2') THEN 'Мой Tele2'
			    WHEN tp.name_report IN ('Мой онлайн +', 'Мой онлайн+') THEN 'Мой онлайн+'       
			    WHEN tp.name_report IN ('Везде онлайн +', 'Везде онлайн+') THEN 'Везде онлайн+'
			    ELSE 'Other' end as name_report,
							
				count(ch.subs_id) as subs_count
		
		   from uat_product.DP_TP_MIGRATION ch --бывший prd2_bds_v.subs_tp_change
		   left join  PRD2_DIC_V.PRICE_PLAN tp on tp.tp_id = ch.tp_id
		   --left join  PRD2_DIC_V.PRICE_PLAN tpprev on tpprev.tp_id = ch.prev_tp_id
		   left join branch on branch.branch_id = ch.branch_id
		   where 1=1
		   		 and ch.calc_platform_id in (-1,1,2)
		         and ch.report_date between week_start_date and week_end_date
		         --and ch.tp_change_flg = 1
		         --and ch.change_count = ch.change_rn
		         --and ch.report_date = ch.last_change_date
		         and ch.tech = 0 and ch.simpl = 0 and ch.daysTP <> 1
		   group by 1,2,3,4,5,6,7
		   
		   
		   union
		   
		   
		   sel
		   		'GROSS' as PARAM,
		   		weeknumber_of_year(agg.report_date, 'ISO') as week_number,  
				trunc(agg.report_date, 'iw') as report_week,
				agg.region_name,
				agg.macroregion as macro_cc_name,
				agg.product_cluster_name,
		
				CASE WHEN d1.name_report IN ('Классический','Мой разговор','Мой онлайн','Безлимит','Везде онлайн') THEN d1.name_report
				WHEN d1.name_report IN ('Лайт', 'Мой Tele2','Лайт/Мой Tele2') THEN 'Мой Tele2'
			    WHEN d1.name_report IN ('Мой онлайн +', 'Мой онлайн+') THEN 'Мой онлайн+'       
			    WHEN d1.name_report IN ('Везде онлайн +', 'Везде онлайн+') THEN 'Везде онлайн+'
			    ELSE 'Other' end as NAME_REPORT,                              
				
				sum(agg.GROSS_SUBS_COUNT) as subs_count
				
			from PRD_RDS_V.PRODUCT_AGG_D_SAP_BO agg
			left join PRD2_DIC_V.PRICE_PLAN d1
				on agg.gross_tp_id=d1.tp_id
			where 1=1
				and agg.calc_platform_id in (-1,1,2)
				and agg.report_date between week_start_date and week_end_date
		         	and agg.product_cluster_name <> 'Deferred'
			group by 1,2,3,4,5,6,7
			
				
		
		;
		END;
	
	SET week_start_date = week_start_date - 7;
	SET week_end_date = week_end_date - 7;
	SET iterations = iterations + 1;
	END WHILE;

END;