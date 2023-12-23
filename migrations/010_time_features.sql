-- 20*60 = 120

create table time_weight (
    time_group int primary key,
    weight float
);
create index on time_weight (time_group);

insert into time_weight
select
    (EXTRACT(epoch FROM real_flight_start) / 600)::int as time_group,
--     count(break_flight_id) as cnt,
--     sum((tvr_index / 0.01499250375)::int + 1) as int_target_sum,
    sum((tvr_index / 0.01499250375)::int + 1)::float / count(break_flight_id) as weight
from
    train
group by
    (EXTRACT(epoch FROM real_flight_start) / 600)::int
order by
    (EXTRACT(epoch FROM real_flight_start) / 600)::int desc
;

drop table if exists time_weight_for_week_day;
create table time_weight_for_week_day (
    week_day_2 int,
    time_group int,
    weight float,
    primary key (week_day_2 ,time_group)
);
create index on time_weight_for_week_day (week_day_2, time_group);

insert into time_weight_for_week_day
select
    week_day_2,
    (EXTRACT(epoch FROM real_flight_start) / 600)::int as time_group,
--     count(break_flight_id) as cnt,
--     sum((tvr_index / 0.01499250375)::int + 1) as int_target_sum,
    sum((tvr_index / 0.01499250375)::int + 1)::float / count(break_flight_id) as weight
from
    train
    join calendar c on train.date = c.date
where
    extra_holiday = FALSE
group by
    week_day_2, (EXTRACT(epoch FROM real_flight_start) / 600)::int
order by
    (EXTRACT(epoch FROM real_flight_start) / 600)::int desc
;
