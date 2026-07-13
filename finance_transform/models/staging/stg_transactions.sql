WITH source AS (
    SELECT * FROM {{ source('lakehouse', 'transactions_raw') }}
),

renamed_and_casted AS (
    SELECT
        CAST(transaction_id AS STRING) AS transaction_id,
        CAST(sender_account AS STRING) AS sender_account,
        CAST(receiver_account AS STRING) AS receiver_account,
        CAST(transaction_type AS STRING) AS transaction_type,
        CAST(timestamp AS TIMESTAMP) AS transaction_timestamp,
        CAST(amount AS FLOAT64) AS amount_usd,
        
        CASE 
            WHEN CAST(amount AS FLOAT64) > 10000 THEN TRUE
            ELSE FALSE
        END AS is_high_value

    FROM source
    WHERE transaction_id IS NOT NULL
)

SELECT * 
FROM renamed_and_casted
-- A MÁGICA DA DESDUPLICAÇÃO AQUI 👇
-- Numera as linhas com o mesmo ID e pega apenas a número 1 (a mais recente)
QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_timestamp DESC) = 1