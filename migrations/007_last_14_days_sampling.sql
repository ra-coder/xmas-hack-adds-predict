select
    min(date),
    max(date)
from xmas_hack_adds_predict.public.train;

create index on train (break_flight_id);

-- min,max
-- 2023-01-02,2023-10-31
-->
create table last_14_days_sampling (
    id bigint primary key,
    for_test bool not null
);

insert into last_14_days_sampling (id, for_test)
select
    break_flight_id as id,
    date > '2023-10-17'::date as for_test
from train;