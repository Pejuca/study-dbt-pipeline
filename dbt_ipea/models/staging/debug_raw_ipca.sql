select
    *
from {{ source('ipea', 'raw_ipca') }}