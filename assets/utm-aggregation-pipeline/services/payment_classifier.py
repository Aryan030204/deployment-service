# This module provides the common SQL snippet for classifying payment types.
# It can be injected into the SELECT queries during the aggregation phase.

PAYMENT_TYPE_CASE_SQL = """
    CASE
        WHEN LOWER(payment_gateway_names) LIKE '%gokwik ppcod%' THEN 'PPCOD'
        WHEN (payment_gateway_names IS NULL 
            OR payment_gateway_names = '' 
            OR payment_gateway_names LIKE '%Cash on Delivery (COD)%' 
            OR payment_gateway_names LIKE '%cash_on_delivery%')
            AND (payment_gateway_names NOT LIKE '%Gokwik PPCOD%' OR payment_gateway_names IS NULL)
            THEN 'COD'
        ELSE 'PREPAID'
    END
"""
