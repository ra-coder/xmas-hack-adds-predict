drop table if exists time_weight_for_bd_week_day_by30_sec;
create table time_weight_for_bd_week_day_by30_sec (
    break_distribution_id int,
    week_day_2 int,
    time_group int,
    weight float,
    primary key (break_distribution_id, week_day_2 ,time_group)
);
create index on time_weight_for_bd_week_day_by30_sec (break_distribution_id, week_day_2, time_group);
insert into time_weight_for_bd_week_day_by30_sec
select
    break_distribution_id,
    week_day_2,
    (EXTRACT(epoch FROM real_flight_start) / 30)::int as time_group,
--     count(break_flight_id) as cnt,
--     sum((tvr_index / 0.01499250375)::int + 1) as int_target_sum,
    sum((tvr_index / 0.01499250375)::int + 1)::float / count(break_flight_id) as weight
from
    train
    join calendar c on train.date = c.date
    join break_distributions on train.break_distribution_id = break_distributions.id
where
    extra_holiday = FALSE
group by
    break_distribution_id, week_day_2, (EXTRACT(epoch FROM real_flight_start) / 30)::int
order by
    break_distribution_id, (EXTRACT(epoch FROM real_flight_start) / 30)::int desc
;
