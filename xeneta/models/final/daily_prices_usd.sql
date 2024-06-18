with join_contract as (
select 
    ch.d_id,
    dp.origin_pid,
    dp.destination_pid,
    dp.equipment_id,
    ch.charge_value,
    ch.currency,
    dp.valid_from,
    dp.valid_to,
from {{ ref('charges') }} as ch
inner join {{ ref('datapoints') }} as dp
    on ch.d_id = dp.d_id
), unify_charges as (
select 
    jc.d_id,
    jc.origin_pid,
    jc.destination_pid,
    jc.equipment_id,
    er.rate * jc.charge_value as usd_charge,
    er."day"
from join_contract as jc
inner join {{ ref('exchange_rates') }} as er
    on jc.currency = er.currency
    and er."day" between jc.valid_from and jc.valid_to
), sum_per_contract as (
    select 
        uc.origin_pid,
        uc.destination_pid,
        uc.equipment_id,
        sum(usd_charge) as total_usd_charge,
        uc."day"
    from unify_charges as uc
    group by d_id, origin_pid,
        uc.destination_pid,
    	uc.equipment_id,
    	uc."day"
), median_prep as (
    select
        x.origin_pid,
        x.destination_pid,
        x.equipment_id,
        x."day",
        avg(total_usd_charge) as median_daily_charge
    from (
        select
            spc.origin_pid,
            spc.destination_pid,
            spc.equipment_id,
            spc."day",
            total_usd_charge,
        ROW_NUMBER() OVER (
            PARTITION BY spc.origin_pid, spc.destination_pid, spc.equipment_id, spc."day" 
            order by total_usd_charge asc) as RowAsc,
        ROW_NUMBER() OVER (
            PARTITION BY origin_pid, spc.destination_pid, spc.equipment_id, spc."day" 
            order by total_usd_charge desc) as RowDesc
        from sum_per_contract as spc
    ) as x
    where RowAsc in (RowDesc, RowDesc - 1, RowDesc + 1)
    group by origin_pid,
        x.destination_pid,
    	x.equipment_id,
    	x."day"
), avg_prep as(
select 
	uc.origin_pid,
	uc.destination_pid,
	uc.equipment_id,
	uc."day",
	avg(total_usd_charge) as average_daily_charge
from sum_per_contract as uc
group by origin_pid, uc.destination_pid, uc.equipment_id, uc."day"
)
select 
	ap.*,
	mp.median_daily_charge
from median_prep as mp
inner join avg_prep as ap
on 	ap.origin_pid= mp.origin_pid
and ap.destination_pid = mp.destination_pid
and ap.equipment_id = mp.equipment_id
and	ap."day" = mp."day"
