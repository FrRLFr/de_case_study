
--Provide a few example queries to the data users. 
--For example, how can they get the average container shipping price of 
--equipment type _2_ from China East Main region to US West Coast region?

with ports_from as (
	select * 
	from {{ ref('ports') }} p 
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
), choose_point as (
select 
	dp.d_id, 
	dp.valid_from,
	dp.valid_to
from {{ ref('datapoints') }} as dp 
inner join ports_to as pt
on dp.destination_pid = pt.pid
inner join ports_from as pf
on dp.origin_pid = pf.pid
where dp.equipment_id = 2
), conversion as
(
select 
	rate*charge_value as usd_price
from choose_point as cp 
inner join {{ ref('charges') }} as ch
	on ch.d_id = cp.d_id
inner join {{ ref('exchange_rates') }} as er
on ch.currency = er.currency
where er."day" between cp.valid_from and cp.valid_to
) select avg(usd_price)
from conversion
;

