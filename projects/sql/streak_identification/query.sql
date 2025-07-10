with usable_data as
(
	select *, event_date - interval '1 day'*(row_number() over(partition by user_id order by event_date asc)) as date_anchor
	from multi_streak_user_activity_table as a
	where engaged_duration > 1
)
, secondary_data as
(
	select user_id, min(event_date) as start_date, 
	max(event_date) as end_date,
	count(distinct event_date) as streak_length,
	sum(revenue) as revenue_per_streak
	from usable_data as a
	group by user_id, date_anchor
)
, teritary_data as
(
	select *, dense_rank() over(partition by user_id order by streak_length desc) as row_handle
	from secondary_data as a
	where revenue_per_streak > 1
)

select user_id, date(start_date) as start_date, date(end_date) as end_date, round(revenue_per_streak::numeric, 2) as revenue, streak_length as max_streak
from teritary_data as a
where row_handle=1
order by user_id