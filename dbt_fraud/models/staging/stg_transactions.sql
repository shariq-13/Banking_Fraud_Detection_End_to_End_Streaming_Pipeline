with source as (
    select * from {{ source('raw', 'transactions') }}
)

select
    transaction_id,
    customer_id,
    account_id,
    amount::number(14, 2) as amount,
    currency,
    merchant_category,
    country,
    transaction_type,
    transaction_timestamp,
    risk_score,
    risk_level,
    is_fraud_flag,
    rule_reasons,
    loaded_at
from source
