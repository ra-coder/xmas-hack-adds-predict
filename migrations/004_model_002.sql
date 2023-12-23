drop table if exists train_002_features;
create table train_002_features (
    id bigint primary key,
    real_flight_start_ts int,
    real_program_start_ts int,
    night_program bool,
    duration_ts int,
    program_duration_ts int,
    int_target int4
);
create index on train_002_features (id);

insert into train_002_features
    (
        id,
        real_flight_start_ts,
        real_program_start_ts,
        night_program,
        duration_ts,
        program_duration_ts,
        int_target
    )
select
    break_flight_id as id,
    EXTRACT(epoch FROM real_flight_start) as real_flight_start_ts,
    EXTRACT(epoch FROM real_program_start) as real_program_start_ts,
    real_date = date as night_program,
    EXTRACT(epoch FROM duration) as duration_ts,
    EXTRACT(epoch FROM program_duration) as program_duration_ts,
    (tvr_index / 0.01499250375)::int + 1 as int_target
from train;


select int_target, count(*) from train_002_features group by int_target;