with returns as (
    select * from {{ ref('int_daily_returns') }}
),

tickers as (
    select * from {{ ref('dim_tickers') }}
),

joined as (
    select
        r.trade_date,
        t.sector,
        r.ticker,
        r.daily_return
    from returns r
    inner join tickers t on r.ticker = t.ticker
    where r.daily_return is not null
),

sector_daily_avg as (
    select
        trade_date,
        sector,
        avg(daily_return) as sector_avg_return,
        count(distinct ticker) as tickers_in_sector
    from joined
    group by trade_date, sector
)

select * from sector_daily_avg