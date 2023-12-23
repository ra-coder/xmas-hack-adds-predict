drop table if exists train_003_features;
create table train_003_features (
    id bigint primary key,
    real_flight_start_ts int,
    real_program_start_ts int,
    night_program bool,
    duration_ts int,
    program_duration_ts int,
    int_target int4,
    week_day int,
    real_week_day int,
    week_day_2 int,
    real_week_day_2 int
);
create index on train_003_features (id);

insert into train_003_features
    (
        id,
        real_flight_start_ts,
        real_program_start_ts,
        night_program,
        duration_ts,
        program_duration_ts,
        int_target,
        week_day,
        real_week_day,
        week_day_2,
        real_week_day_2
    )
select
    break_flight_id as id,
    EXTRACT(epoch FROM real_flight_start) as real_flight_start_ts,
    EXTRACT(epoch FROM real_program_start) as real_program_start_ts,
    real_date = date as night_program,
    EXTRACT(epoch FROM duration) as duration_ts,
    EXTRACT(epoch FROM program_duration) as program_duration_ts,
    (tvr_index / 0.01499250375)::int + 1 as int_target,
    extract(dow from date) as week_day,
    extract(dow from date) + 1 as week_day_2,
    extract(dow from real_program_start_date) as real_week_day,
    extract(dow from real_program_start_date) + 1 as real_week_day_2
from train;
