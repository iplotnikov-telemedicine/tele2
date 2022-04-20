
-- ќ“ќ«¬јЌ
-- 10 фев разместил
-- 20 фев обновилс€ тариф
-- 28 фев смотрим

-- ќ“ќ«¬јЌ
-- 28 фев разместил
-- 10 мар обновилс€ тариф
-- 20 мар смотрим

-- ј “»¬≈Ќ
-- 10 фев разместил
-- 20 фев смотрим
-- 28 фев обновитс€ тариф

-- ј “»¬≈Ќ
-- 28 фев разместил
-- 10 мар смотрим
-- 20 мар обновитс€ тариф

--если день обновлени€ >= день размещени€, то кладем в этот мес€ц относительно дн€ размещени€
--если день размещени€ < день обновлени€ , то кладем в след мес€ц относительно дн€ размещени€


with market as (
/*  на дату смотрим кол-во не проданных и не отозванных абонентом лотов*/
    sel
        Date'2021-01-31' as report_date,
        subs_id_seller,
        lot_placement_date
        
    FROM PRD_RDS_V.MARKET_TELE2 mar
    where 1=1
        and lot_placement_date >= add_months(Date'2021-01-31', -1)
        and lot_placement_date <= Date'2021-01-31'
        AND subs_revocation_date IS NULL
        AND lot_purchase_date IS NULL
),


clrd as (
/*смотрим дату подключени€ абонента*/
    sel
        subs_id,
        cast(activation_dttm as date) as activation_date
    --help view prd2_bds_v.subs_clr_d
    from prd2_bds_v.subs_clr_d clrd
    where report_date = Date'2021-01-31'
        and SUBS_ID in (select subs_id_seller from market)
),


migr as (

/*смотрим последнюю дату смены тарифа*/
    sel
        subs_id,
        last_change_date
    from prd2_bds_v.subs_tp_change
    where report_date = Date'2021-01-31'
        and SUBS_ID in (select subs_id_seller from market)
)



sel 
    market.report_date,
    market.subs_id_seller,
    market.lot_placement_date,
    extract(DAY from lot_placement_date) as lot_placement_day, --дата размещени€
    extract(DAY from coalesce(migr.last_change_date, clrd.activation_date)) as renewal_day, --день обновлени€ пакетов
    extract(DAY from last_day(add_months(lot_placement_date,1))) as next_month_last_day, --последний день следующего мес€ца (справочно)
    case when lot_placement_day >= renewal_day then 1 else 0 end is_next_month_renewal, --флаг обновлени€ пакетов в следующем мес€це, а не текущем           
    
-------ключевые пол€
    trunc(add_months(lot_placement_date,is_next_month_renewal),'mon')
        + LEAST(next_month_last_day, renewal_day - is_next_month_renewal) as next_renewal_date, --конкретна€ дата обновлени€ пакетов (отзыва системой)
        
    case when next_renewal_date <= market.report_date then 1 else 0 end as is_sys_revocated --флаг отзыва системой на дату report_date
--------------------
    
from market
left join clrd
    on market.subs_id_seller = clrd.subs_id
left join migr
    on market.subs_id_seller = migr.subs_id
where 1=1

/*
sel is_sys_revocated,
	next_renewal_date,
	count(subs_id_seller) as subs_count
from final
group by 1,2*/