-- Daily fraud KPIs for the Superset dashboard.
-- Snowflake notes: COUNT_IF and IFF replace the PostgreSQL FILTER clause.

select
    date_trunc('day', transaction_timestamp)::date as transaction_date,
    count(*) as total_transactions,
    count_if(is_fraud_flag) as flagged_transactions,
    round(
        100.0 * count_if(is_fraud_flag) / nullif(count(*), 0),
        2
    ) as flagged_transaction_pct,
    round(sum(amount), 2) as total_transaction_amount,
    round(sum(iff(is_fraud_flag, amount, 0)), 2) as flagged_amount,
    round(avg(risk_score), 2) as average_risk_score
from {{ ref('stg_transactions') }}
group by 1
