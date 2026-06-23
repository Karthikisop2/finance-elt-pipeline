select
    ticker,
    company_name,
    sector
from {{ ref('ticker_reference') }}