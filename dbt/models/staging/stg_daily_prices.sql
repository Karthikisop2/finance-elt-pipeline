with source as (
    select * from {{ source('raw', 'daily_prices') }}
),

cleaned as (
    select
        ticker,
        trade_date,
        open_price::numeric(12,4)   as open_price,
        high_price::numeric(12,4)   as high_price,
        low_price::numeric(12,4)    as low_price,
        close_price::numeric(12,4)  as close_price,
        volume::bigint              as volume,
        source_response_id,
        loaded_at
    from source
    where close_price > 0
      and high_price >= low_price   -- guard against impossible OHLC relationships
)

select * from cleaned