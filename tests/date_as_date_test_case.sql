select *
from {{ ref('stg_hello') }}
where try_cast(loaded_at as date) is null
