INSERT INTO {{ sink('search', 'orders_out') }}
select id, name, _id as source_id
from {{ source('search', 'orders_in') }}
order by id
