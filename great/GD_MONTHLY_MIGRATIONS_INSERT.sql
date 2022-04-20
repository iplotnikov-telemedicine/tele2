REPLACE procedure UAT_PRODUCT.GD_MONTHLY_MIGRATIONS_INSERT(in input_month date, in months_forward_from_input_month integer)

--sel distinct param from UAT_PRODUCT.GD_MONTHLY_MIGRATIONS
--sel distinct param from UAT_PRODUCT.GD_MONTHLY_MIGRATIONS
--sel top 100 * from UAT_PRODUCT.GD_MONTHLY_MIGRATIONS
--sel count(*) from UAT_PRODUCT.GD_MONTHLY_MIGRATIONS
--call UAT_PRODUCT.GD_MONTHLY_MIGRATIONS_INSERT(date'2018-01-01', 31)
--call UAT_PRODUCT.GD_MONTHLY_MIGRATIONS_INSERT(date'2020-07-01', 2)
--delete from UAT_PRODUCT.GD_MONTHLY_MIGRATIONS where report_date between (date'2019-07-01') and (date'2019-09-01')
--sel min(report_date) from uat_product.DP_TP_MIGRATION
--sel report_date from prd2_bds_v.subs_tp_change where report_date < date'2019-01-01' group by report_date

SQL SECURITY INVOKER

BEGIN

	DECLARE month_start_date date;
	DECLARE month_end_date date;
	DECLARE last_month date;
	
	SET month_start_date = input_month;
	SET month_end_date = last_day(month_start_date);
	SET last_month = add_months(input_month, months_forward_from_input_month);

	delete from uat_product.GD_MONTHLY_MIGRATIONS where report_month >= month_start_date and report_month < last_month;
	
	WHILE month_start_date < last_month DO 
	
		BEGIN
		
		insert into uat_product.GD_MONTHLY_MIGRATIONS	
		
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
	
	        ) ,
	      
	    
	by_subs_id as ( --находим уникальные (последние) переходы на тарифные планы
		
					select
							cast('MIGR' as varchar(5)) as param,      
							trunc(ch.report_date, 'mon')  as report_month,              
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
										
							ch.subs_id,
							ch.cbm,
							RANK() OVER (PARTITION BY ch.subs_id ORDER BY ch.report_date DESC) as max_report_date
					--help table uat_product.DP_TP_MIGRATION
					--help view prd2_bds_v.subs_tp_change
					   from uat_product.DP_TP_MIGRATION ch --бывший prd2_bds_v.subs_tp_change
					   left join  PRD2_DIC_V.PRICE_PLAN tp on tp.tp_id = ch.tp_id
					   --left join  PRD2_DIC_V.PRICE_PLAN tpprev on tpprev.tp_id = ch.prev_tp_id
					   left join branch on branch.branch_id = ch.branch_id
					   where 1=1
					   		 and ch.calc_platform_id in (-1,1,2)
					         and ch.report_date between month_start_date and month_end_date
					         --and ch.tp_change_flg = 1
					         --and ch.change_count = ch.change_rn
					         --and ch.report_date = ch.last_change_date
					         and ch.tech = 0 and ch.simpl = 0 and ch.daysTP <> 1
					   QUALIFY RANK() OVER (PARTITION BY ch.subs_id ORDER BY ch.report_date DESC) = 1
					   
					   )
					   
					 select
					 	param,
					 	report_month,
					 	region_name,
					 	macro_cc_name,
					 	product_cluster_name,
					 	name_report,
					 	cbm,
					 	count(subs_id) as subs_count
					 from by_subs_id
					 group by 1,2,3,4,5,6,7
		   
				
		
		;
		END;
	
	SET month_start_date = add_months(month_start_date, 1);
	SET month_end_date = last_day(month_start_date);
	END WHILE;

END;