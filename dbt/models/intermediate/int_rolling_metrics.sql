with returns as (
    select * from {{ ref('int_daily_returns') }}
),

rolling as (
    select
        *,
        avg(close_price) over (
            partition by ticker
            order by trade_date
            rows between 19 preceding and current row
        ) as moving_avg_20d,

        avg(close_price) over (
            partition by ticker
            order by trade_date
            rows between 49 preceding and current row
        ) as moving_avg_50d,

        stddev_samp(daily_return) over (
            partition by ticker
            order by trade_date
            rows between 19 preceding and current row
        ) as volatility_20d,

        count(*) over (
            partition by ticker
            order by trade_date
            rows between 19 preceding and current row
        ) as trading_days_in_window_20d

    from returns
)

select
    *,
    case
        when trading_days_in_window_20d < 20 then true
        else false
    end as is_partial_window_20d
from rolling