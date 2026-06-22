with source as (
    select * from {{ source('raw', 'raw_api_responses') }}
    where raw_payload ? 'Time Series (Daily)'  -- skip error/rate-limit payloads
),

exploded as (
    select
        r.response_id,
        r.ticker,
        r.fetched_at,
        kv.key::date as trade_date,
        kv.value as day_payload
    from source r,
         jsonb_each(r.raw_payload -> 'Time Series (Daily)') as kv(key, value)
),

parsed as (
    select
        response_id,
        ticker,
        fetched_at,
        trade_date,
        (day_payload ->> '1. open')::numeric(12,4)   as open_price,
        (day_payload ->> '2. high')::numeric(12,4)   as high_price,
        (day_payload ->> '3. low')::numeric(12,4)    as low_price,
        (day_payload ->> '4. close')::numeric(12,4)  as close_price,
        (day_payload ->> '5. volume')::bigint        as volume
    from exploded
)

select * from parsed