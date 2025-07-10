with sus_sessions_1 as(
	select *, 
	case when 
			extract(epoch from lead(transaction_timestamp,1,null) over(partition by sender_id order by transaction_timestamp asc) - transaction_timestamp) >= 60*5 then 1
		 when
		 	extract(epoch from transaction_timestamp - lag(transaction_timestamp,1,null) over(partition by sender_id order by transaction_timestamp asc)) >= 60*5 then 1 
	else 0 end as new_session_flag
	from crypto_currency_fraud_sessions as a
)

, sus_session_2 as(
	select *, sum(new_session_flag) over(partition by sender_id order by transaction_timestamp asc) as session_id
	from sus_sessions_1 as a
)

, pre_final_data as(
	select sender_id, session_id as chain_id, 
	min(transaction_timestamp) as chain_start_time,
	max(transaction_timestamp) as chain_end_time,
	sum(amount) as total_amount,
	count(distinct transaction_id) as num_txns,
	array_agg(row(transaction_id, transaction_timestamp)) as list_txn_id_involved
	from sus_session_2 as a
	group by sender_id, session_id
	having count(distinct transaction_id) > 1
	and sum(amount) > 150
)

, final_data as(
	select *, dense_rank() over(partition by sender_id order by total_amount desc) as fraud_rank
	from pre_final_data as a
)

select sender_id, 
concat(sender_id, '_', chain_id) as chain_id,
chain_start_time, chain_end_time, total_amount, num_txns, fraud_rank, list_txn_id_involved
from final_data as a
order by sender_id, fraud_rank
