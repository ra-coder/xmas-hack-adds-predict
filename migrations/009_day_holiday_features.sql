drop table if exists calendar;
create table calendar (
    date date primary key,
    week_day_2 int,
    extra_holiday bool,
    holiday bool,
    week_day_3 int
);
create index on calendar (date);

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
from train
order by real_date;

--
-- -- 1, 2, 3, 4, 5, 6 and 8 January — New Year Holidays;
-- -- 7 January — Christmas Day;
-- -- 23 February — Defender of the Fatherland Day;
-- -- 8 March — International Women’s Day;
-- -- 1 May — Spring and Labour Holiday;
-- -- 9 May — Victory Day;
-- -- 12 June — Day of Russia;
-- -- 4 November — National Unity Day.
-- --
-- -- The following days are additional holidays in Russia for 2023:
-- -- 24 February — Friday;
-- -- 8 May — Monday;
-- -- 6 November — Monday.
-- --
