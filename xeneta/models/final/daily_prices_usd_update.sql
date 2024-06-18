
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
        dp.company_id,
        dp.supplier_id
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
        er."day",
        jc.company_id,
        jc.supplier_id
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
        uc."day",
        uc.company_id,
        uc.supplier_id
    from unify_charges as uc
    group by d_id, origin_pid,
        uc.destination_pid,
        uc.equipment_id,
        uc."day", 
        uc.company_id, 
        uc.supplier_id
), count_suppliers as (
    select 
        spc.origin_pid,
        spc.destination_pid,
        spc.equipment_id,
        spc."day",
        case when count(spc.supplier_id)>1 
            then True
            else false end as dq_supplier
    from sum_per_contract as spc
    group by spc.origin_pid,
        spc.destination_pid,
        spc.equipment_id,
        spc."day"
), count_companies as (
    select 
        spc.origin_pid,
        spc.destination_pid,
        spc.equipment_id,
        spc."day", 
        case when count(company_id)>4 
            then True else false
            end as dq_company
    from sum_per_contract as spc
    group by spc.origin_pid,
        spc.destination_pid,
        spc.equipment_id,
        spc."day"
), median_prep as (
    select
        x.origin_pid,
        x.destination_pid,
        x.equipment_id,
        x."day",
        avg(usd_charge) as median_daily_charge
    from (
        select
            ucm.origin_pid,
            ucm.destination_pid,
            ucm.equipment_id,
            ucm."day",
            usd_charge,
            row_number() over (partition by ucm.origin_pid, 
                ucm.destination_pid, ucm.equipment_id, ucm."day" 
                order by usd_charge asc) as RowAsc,
            row_NUMBER() OVER (PARTITION BY origin_pid, 
                ucm.destination_pid, ucm.equipment_id, ucm."day" 
                order by usd_charge desc) as RowDesc
        FROM unify_charges as ucm
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
        avg(usd_charge) as average_daily_charge
    from unify_charges as uc
    group by origin_pid, 
        uc.destination_pid, 
        uc.equipment_id, 
        uc."day"
)
select 
    ap.*,
    mp.median_daily_charge,
    case when cs.dq_supplier=TRUE and dq_company =TRUE
        then true else false 
        end as dq_ok
from median_prep as mp
inner join avg_prep as ap
    on 	ap.origin_pid= mp.origin_pid
    and ap.destination_pid = mp.destination_pid
    and ap.equipment_id = mp.equipment_id
    and	ap."day" = mp."day"
inner join count_companies as cc
    on 	cc.origin_pid= mp.origin_pid
    and cc.destination_pid = mp.destination_pid
    and cc.equipment_id = mp.equipment_id
    and	cc."day" = mp."day"
inner join count_suppliers as cs
    on 	cs.origin_pid= mp.origin_pid
    and cs.destination_pid = mp.destination_pid
    and cs.equipment_id = mp.equipment_id
    and	cs."day" = mp."day"


