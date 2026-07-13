{{ config(materialized='table') }}

WITH staging AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

account_metrics AS (
    SELECT
        DATE(transaction_timestamp) AS date_transaction,
        sender_account AS account_id,
        transaction_type,
        
        -- Volume e Valor Total
        COUNT(DISTINCT transaction_id) AS qty_transactions,
        SUM(amount_usd) AS total_value_usd,
        
        -- Métricas de Alto Valor usando funções nativas do BigQuery
        COUNTIF(is_high_value) AS qty_high_level,
        SUM(CASE WHEN is_high_value THEN amount_usd ELSE 0 END) AS value_high_level
        
    FROM staging
    GROUP BY 
        date_transaction, 
        account_id, 
        transaction_type
)

SELECT * FROM account_metrics