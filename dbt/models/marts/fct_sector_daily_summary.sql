with sector_benchmarks as (
    select * from {{ ref('int_sector_benchmarks') }}
),

sector_volatility as (
    select
        t.sector,
        f.trade_date,
        avg(f.volatility_20d) as avg_sector_volatility_20d
    from {{ ref('fct_daily_prices') }} f
    inner join {{ ref('dim_tickers') }} t
        on f.ticker = t.ticker
    where f.is_partial_window_20d = false
    group by t.sector, f.trade_date
),

joined as (
    select
        sb.trade_date,
        sb.sector,
        sb.sector_avg_return,
        sb.tickers_in_sector,
        sv.avg_sector_volatility_20d
    from sector_benchmarks sb
    left join sector_volatility sv
        on sb.trade_date = sv.trade_date
        and sb.sector = sv.sector
)

select * from joined