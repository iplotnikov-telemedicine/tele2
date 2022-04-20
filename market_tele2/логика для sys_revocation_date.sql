
-- �������
-- 10 ��� ���������
-- 20 ��� ��������� �����
-- 28 ��� �������

-- �������
-- 28 ��� ���������
-- 10 ��� ��������� �����
-- 20 ��� �������

-- �������
-- 10 ��� ���������
-- 20 ��� �������
-- 28 ��� ��������� �����

-- �������
-- 28 ��� ���������
-- 10 ��� �������
-- 20 ��� ��������� �����

--���� ���� ���������� >= ���� ����������, �� ������ � ���� ����� ������������ ��� ����������
--���� ���� ���������� < ���� ���������� , �� ������ � ���� ����� ������������ ��� ����������


with market as (
/*  �� ���� ������� ���-�� �� ��������� � �� ���������� ��������� �����*/
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
/*������� ���� ����������� ��������*/
    sel
        subs_id,
        cast(activation_dttm as date) as activation_date
    --help view prd2_bds_v.subs_clr_d
    from prd2_bds_v.subs_clr_d clrd
    where report_date = Date'2021-01-31'
        and SUBS_ID in (select subs_id_seller from market)
),


migr as (

/*������� ��������� ���� ����� ������*/
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
    extract(DAY from lot_placement_date) as lot_placement_day, --���� ����������
    extract(DAY from coalesce(migr.last_change_date, clrd.activation_date)) as renewal_day, --���� ���������� �������
    extract(DAY from last_day(add_months(lot_placement_date,1))) as next_month_last_day, --��������� ���� ���������� ������ (���������)
    case when lot_placement_day >= renewal_day then 1 else 0 end is_next_month_renewal, --���� ���������� ������� � ��������� ������, � �� �������           
    
-------�������� ����
    trunc(add_months(lot_placement_date,is_next_month_renewal),'mon')
        + LEAST(next_month_last_day, renewal_day - is_next_month_renewal) as next_renewal_date, --���������� ���� ���������� ������� (������ ��������)
        
    case when next_renewal_date <= market.report_date then 1 else 0 end as is_sys_revocated --���� ������ �������� �� ���� report_date
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