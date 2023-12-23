drop table train;

create table train
(
    tvr_index               float,
    date                    date,
    break_flight_id         BIGINT primary key,
--break_flight_start time,
--break_flight_end time,
    break_content           varchar(25), -- todo enum
    break_distribution      varchar(25), -- todo enum
    programme               text,
--programme_flight_start time,
--programme_flight_end time,
    programme_category      varchar(40), -- todo enum
    programme_genre         varchar(40), -- todo enum
    duration                time,
    real_flight_start       time,
    real_date               date,
    program_duration        time,
    real_program_start_date date,
    real_program_start      time

);

-- 0.6146926537,
--     1/2/2023,
--     4870830561,
--     8:17:33,
--     8:21:40,
--     Commercial,
--     Network,
--     "Telekanal ""Dobroe utro""",
--     8:00:13,
--     10:00:14,
--     Morning airplay,
--     Entertainment programs
