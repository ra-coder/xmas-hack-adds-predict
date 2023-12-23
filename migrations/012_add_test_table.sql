drop table if exists test_data;

create table test_data
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
--- load data from csv via pycharm
alter table test_data add column programme_id int references programmes;

update test_data
set programme_id = id
from (
select
    id,
    name
from programmes) as _tmp where _tmp.name = programme;


--             'programme_category',

--alter table test_data drop column programme_id;
alter table test_data add column programme_category_id int references programme_categories;

update test_data
set programme_category_id = id
from (
select
    id,
    name
from programme_categories) as _tmp where _tmp.name = programme_category;


--             'programme_genre'
--alter table test_data drop column programme_id;
alter table test_data add column programme_genre_id int references programme_genres;

update test_data
set programme_genre_id = id
from (
select
    id,
    name
from programme_genres) as _tmp where _tmp.name = programme_genre;


--             'break_distribution',
--alter table test_data drop column programme_id;
alter table test_data add column break_distribution_id int references break_distributions;

update test_data
set break_distribution_id = id
from (
select
    id,
    name
from break_distributions) as _tmp where _tmp.name = break_distribution;


--             'break_content',
--alter table test_data drop column programme_id;
alter table test_data add column break_content_id int references break_contents;

update test_data
set break_content_id = id
from (
select
    id,
    name
from break_contents) as _tmp where _tmp.name = break_content;


insert into calendar (date, week_day_2, extra_holiday, holiday, week_day_3)
select distinct
    real_date,
    extract(dow from real_date) + 1 as week_day_2,
    real_date in (
        '2023-01-01'::date,
        '2023-01-02'::date,
        '2023-01-03'::date,
        '2023-01-04'::date,
        '2023-01-05'::date,
        '2023-01-06'::date,
        '2023-01-07'::date,
        '2023-01-08'::date,
        '2023-02-23'::date,
        '2023-03-08'::date,
        '2023-05-01'::date,
        '2023-05-08'::date,
        '2023-05-09'::date,
        '2023-06-12'::date,
        '2023-11-04'::date,
        '2023-11-06'::date
    ) as extra_holiday,
    extract(dow from real_date) + 1 in (6,7)
        or
    real_date in (
        '2023-01-01'::date,
        '2023-01-02'::date,
        '2023-01-03'::date,
        '2023-01-04'::date,
        '2023-01-05'::date,
        '2023-01-06'::date,
        '2023-01-07'::date,
        '2023-01-08'::date,
        '2023-02-23'::date,
        '2023-03-08'::date,
        '2023-05-01'::date,
        '2023-05-08'::date,
        '2023-05-09'::date,
        '2023-06-12'::date,
        '2023-11-04'::date,
        '2023-11-06'::date
    ) as holiday,
    case when
        real_date in (
        '2023-01-01'::date,
        '2023-01-02'::date,
        '2023-01-03'::date,
        '2023-01-04'::date,
        '2023-01-05'::date,
        '2023-01-06'::date,
        '2023-01-07'::date,
        '2023-01-08'::date,
        '2023-02-23'::date,
        '2023-03-08'::date,
        '2023-05-01'::date,
        '2023-05-08'::date,
        '2023-05-09'::date,
        '2023-06-12'::date,
        '2023-11-04'::date,
        '2023-11-06'::date
    ) then 6
    else
    extract(dow from real_date) + 1
    end as week_day_3
from test_data
order by real_date on conflict do nothing ;

----------
drop table if exists test_data_003_features;
create table test_data_003_features (
    id bigint primary key,
    real_flight_start_ts int,
    real_program_start_ts int,
    night_program bool,
    duration_ts int,
    program_duration_ts int,
    int_target int4,
    non_zero_target int4,
    week_day int,
    real_week_day int,
    week_day_2 int,
    real_week_day_2 int
);
create index on test_data_003_features (id);

insert into test_data_003_features
    (
        id,
        real_flight_start_ts,
        real_program_start_ts,
        night_program,
        duration_ts,
        program_duration_ts,
        int_target,
        non_zero_target,
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
    Null as int_target,
    NULL as non_zero_target,
    extract(dow from date) as week_day,
    extract(dow from date) + 1 as week_day_2,
    extract(dow from real_program_start_date) as real_week_day,
    extract(dow from real_program_start_date) + 1 as real_week_day_2
from test_data;
