with prices as (
    select * from {{ ref('stg_daily_prices') }}
),

with_lag as (
    select
        *,
        lag(close_price) over (
            partition by ticker order by trade_date
        ) as prev_close_price
    from prices
),

returns_calculated as (
    select
        *,
        case
            when prev_close_price is not null and prev_close_price != 0
            then round((close_price - prev_close_price) / prev_close_price, 6)
        end as daily_return
    from with_lag
)

select * from returns_calculated