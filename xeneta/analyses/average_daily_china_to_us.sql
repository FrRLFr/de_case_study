
--Provide a few example queries to the data users. 
--For example, how can they get the average container shipping price of 
--equipment type _2_ from China East Main region to US West Coast region?

with ports_from as (
    select 
        * 
    from from {{ ref('ports') }} p 
    where slug_region in ( 
        select 
            slug_region_name
        from {{ ref('regions') }}
		where region_name = 'China East Main')
), ports_to as(
    select 
    	* 
    from {{ ref('ports') }}
    where slug_region in (
        select 
	        slug_region_name
        from {{ ref('regions') }}
        where region_name = 'US West Coast')
)
select 
    average_daily_charge, 
    "day" 
from {{ ref('daily_prices_usd') }} as dpu 
inner join ports_to as pt
    on dpu.destination_pid = pt.pid
inner join ports_from as pf
    on dpu.origin_pid = pf.pid
where equipment_id = 2
