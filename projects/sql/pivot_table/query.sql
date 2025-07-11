with final_data as(
	select 
	region,
	date_trunc('quarter', sales_date) as quarter,
	sum(sold_quantity*unit_price) filter(where product='p1') as p1_sales,
	sum(sold_quantity*unit_price) filter(where product='p2') as p2_sales,
	sum(sold_quantity*unit_price) filter(where product='p3') as p3_sales
	from sql_pivot_table_from_raw_data as a
	where date_trunc('year', sales_date) = date('2024-01-01')
	group by date_trunc('quarter', sales_date), region
)

select region, 
date(quarter) as quarter,
round(p1_sales) as p1_sales,
round(p2_sales) as p2_sales,
round(p3_sales) as p3_sales,

coalesce(round(((p1_sales*1.00/lag(p1_sales,1,null) over(partition by region order by quarter asc) - 1)*100)::numeric, 2), 0.00) as p1_growth_per_quarter,
coalesce(round(((p2_sales*1.00/lag(p2_sales,1,null) over(partition by region order by quarter asc) - 1)*100)::numeric, 2), 0.00) as p2_growth_per_quarter,
coalesce(round(((p3_sales*1.00/lag(p3_sales,1,null) over(partition by region order by quarter asc) - 1)*100)::numeric, 2), 0.00) as p3_growth_per_quarter
from final_data as a
order by region, quarter