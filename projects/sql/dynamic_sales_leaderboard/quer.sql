with cumulative_sales_cte as(
	select *, 
	sum(revenue) over(partition by user_id order by sales_date asc) as cumulative_sales_per_salesman
	from sales_data as a
	where date_trunc('quarter', sales_date) = date('2024-01-01') -- i've used fucntion over date, this might fail partition pruning so take caution
)

, ranked_salesmen as(
	select *, 
	dense_rank() over(partition by region, sales_date order by cumulative_sales_per_salesman desc) as salesman_rank_cumulative,
	dense_rank() over(partition by region, sales_date order by revenue desc) as salesman_rank_daily
	from cumulative_sales_cte as a
)

select 

		date(sales_date) as sales_date, region, 
		array_agg(user_id) filter(where salesman_rank_cumulative = 1) as "salesman with the highest cumulative sales" ,
		round(max(cumulative_sales_per_salesman) filter(where salesman_rank_cumulative = 1) :: numeric, 2) as total_cumulative_sales,
		array_agg(user_id) filter(where salesman_rank_daily=1) as "salesmen with the highest sales on that specific day",
		round(max(revenue) filter(where salesman_rank_daily=1)::numeric, 2) as daily_sales
		
from ranked_salesmen as a
where (salesman_rank_cumulative = 1 
		or 
	   salesman_rank_daily = 1)
and sales_date < date('2024-04-01')
group by region, sales_date
order by sales_date desc, region