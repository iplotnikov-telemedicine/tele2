REPLACE procedure UAT_PRODUCT.GD_PRODUCT_INSERT (in input_month date, in months_forward_from_input_month integer)

--call UAT_PRODUCT.GD_PRODUCT_INSERT ('2018-01-01', 31)
--call UAT_PRODUCT.GD_PRODUCT_INSERT ('2020-08-01', 1)
--delete from UAT_PRODUCT.GD_PRODUCT

SQL SECURITY INVOKER

BEGIN

	DECLARE month_start_date date;
	DECLARE month_end_date date;
	DECLARE last_month date;
	
	SET month_start_date = input_month;
	SET month_end_date = last_day(month_start_date);
	SET last_month = add_months(input_month, months_forward_from_input_month);

	delete from uat_product.GD_PRODUCT where report_month>=month_start_date and report_month < last_month;
	WHILE month_start_date < last_month DO 
	
		BEGIN
			
		insert into uat_product.GD_PRODUCT	
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
        		),
        		
		agg_clrd as (
			
				sel
					agg.report_month,			
					agg.branch_id,
					case PRICE_PLAN.BUNDLE_FLAG when 1 then 'Bundle' else 'PAYG' end BUNDLE_PAYG,	
					agg.BASE_TYPE,
					
					case when PRICE_PLAN.name_report in ('Классический','Мой разговор','Мой Tele2','Мой онлайн',
													'Мой онлайн+','Безлимит','Везде онлайн') then PRICE_PLAN.name_report
									when PRICE_PLAN.name_report in ('Везде онлайн +', 'Везде онлайн+') then 'Везде онлайн+'
												else 'Other' end as NAME_REPORT,
						 
					coalesce(clrd.SEGM_ARPU_RUS_NAME, 'Undefined') as SEGM_ARPU_RUS_NAME,
					
					count(agg.subs_id) as total_subs_count,
					sum(agg.FLASH_ACTIVE_FLAG) as flash_subs_count,
					sum(agg.talking_subs_flag) as talking_subs_count,
					
					count(case when agg.total_min_technical>0 then 1 end) as voice_users_count,
					sum(agg.total_min_technical) as voice_min,
					
					sum(agg.data_user_flag) as data_users_count,
					sum(agg.data_active_user_1000_FLAG) as data_active_users_count,
					sum(agg.DATA_TRAFFIC_TOTAL_MB) as data_mb
					--help view PRD_RDS_V.PRODUCT_AGG_SUBS_M
				from PRD_RDS_V.PRODUCT_AGG_SUBS_M as agg
				left join PRD2_DIC_V.PRICE_PLAN
					on agg.tp_id=PRICE_PLAN.tp_id
				left join clrd
					on agg.subs_id = clrd.subs_id
				where agg.CALC_PLATFORM_ID in (-1,1,2)	
					and report_month = month_start_date
				group by 1,2,3,4,5,6
			
				),

			
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
		
		        )
        
		        
	 			select
	 				branch.region_name,
	 				branch.macro_cc_name,
	 				branch.product_cluster_name,
	 				agg_clrd.*
	 				
	 			from agg_clrd
	 			left join branch
					on agg_clrd.branch_id=branch.branch_id
		

		
		;
		END;
	
	SET month_start_date = add_months(month_start_date, 1);
	SET month_end_date = last_day(month_start_date);
	END WHILE;

END;