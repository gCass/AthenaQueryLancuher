create table l3_rep_research.prove_query_launcher_gc_temp
WITH ( format = 'PARQUET',
       parquet_compression = 'SNAPPY',
      external_location = 's3://axa-it-dl-nonprod-dev-l3-reporting-eu-central-1/l3_rep_research/prove_query_launcher_gc_temp')
as 

select 1 as A, now() as B
from l2_portafoglio.polizza
limit 10