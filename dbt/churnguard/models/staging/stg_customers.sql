with source as (
    select * from read_parquet('../../data/gold/gold_customers.parquet')
),

renamed as (
    select
        customer_id,
        gender,
        senior_citizen,
        partner,
        dependents,
        tenure,
        phone_service,
        multiple_lines,
        internet_service,
        online_security,
        online_backup,
        device_protection,
        tech_support,
        streaming_tv,
        streaming_movies,
        contract,
        paperless_billing,
        payment_method,
        monthly_charges,
        total_charges,
        churn,
        tenure_bucket,
        churn_flag,
        revenue_lost
    from source
)

select * from renamed
