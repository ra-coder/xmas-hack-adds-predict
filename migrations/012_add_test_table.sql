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
