with rolling_metrics as (
    select * from {{ ref('int_rolling_metrics') }}
),

tickers as (
    select * from {{ ref('dim_tickers') }}
),

sector_benchmarks as (
    select * from {{ ref('int_sector_benchmarks') }}
),

joined as (
    select
        rm.ticker,
        t.company_name,
        t.sector,
        rm.trade_date,
        rm.open_price,
        rm.high_price,
        rm.low_price,
        rm.close_price,
        rm.volume,
        rm.daily_return,
        rm.moving_avg_20d,
        rm.moving_avg_50d,
        rm.volatility_20d,
        rm.is_partial_window_20d,
        sb.sector_avg_return as sector_benchmark_return,
        rm.daily_return - sb.sector_avg_return as return_vs_sector
    from rolling_metrics rm
    left join tickers t
        on rm.ticker = t.ticker
    left join sector_benchmarks sb
        on rm.trade_date = sb.trade_date
        and t.sector = sb.sector
)

select * from joined