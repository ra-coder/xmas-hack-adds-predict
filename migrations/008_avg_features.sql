select count(break_flight_id) from train; --> 30K

select
    count(train.break_flight_id) as blocks_cnt,
    count(distinct date) as dates_count_cnt
from train
join last_7_days_sampling on break_flight_id= last_7_days_sampling.id and for_test = FALSE -- fil target week
;


alter table programmes add column avg_int_target int;
alter table programmes add column effir_rate float;
alter table programmes add column blocks_per_program int;

update programmes set
                      avg_int_target = tmp.avg_int_target,
                      effir_rate =tmp.effir_rate,
                      blocks_per_program = tmp.blocks_per_program
from
    (with all_stat as (select count(train.break_flight_id) as blocks_cnt_total,
                              count(distinct date)         as dates_cnt_total
                       from train
                                join last_7_days_sampling
                                     on break_flight_id = last_7_days_sampling.id and for_test = FALSE -- fil target week
    )
     select
         --     sum(train.tvr_index) as sum,
         --     sum((tvr_index / 0.01499250375)::int + 1) as int_sum,
         --     count(train.break_flight_id) as cnt,
         sum((tvr_index / 0.01499250375)::int + 1) / count(train.break_flight_id) as avg_int_target,
         count(distinct date)::float / dates_cnt_total                            as effir_rate,
         count(train.break_flight_id) / count(distinct date)                      as blocks_per_program,
         programmes.name,
         programmes.id
     from train
              join last_7_days_sampling
                   on break_flight_id = last_7_days_sampling.id and for_test = FALSE -- fil target week
              join programmes on train.programme_id = programmes.id
              join all_stat on true
     group by programmes.id, all_stat.blocks_cnt_total, all_stat.dates_cnt_total
     ) as tmp where programmes.id = tmp.id
;

-------


alter table programme_categories add column avg_int_target int;
alter table programme_categories add column effir_rate float;
alter table programme_categories add column blocks_per_program int;

update programme_categories set
                      avg_int_target = tmp.avg_int_target,
                      effir_rate =tmp.effir_rate,
                      blocks_per_program = tmp.blocks_per_program
from
    (with all_stat as (select count(train.break_flight_id) as blocks_cnt_total,
                              count(distinct date)         as dates_cnt_total
                       from train
                                join last_7_days_sampling
                                     on break_flight_id = last_7_days_sampling.id and for_test = FALSE -- fil target week
    )
     select
         --     sum(train.tvr_index) as sum,
         --     sum((tvr_index / 0.01499250375)::int + 1) as int_sum,
         --     count(train.break_flight_id) as cnt,
         sum((tvr_index / 0.01499250375)::int + 1) / count(train.break_flight_id) as avg_int_target,
         count(distinct date)::float / dates_cnt_total                            as effir_rate,
         count(train.break_flight_id) / count(distinct date)                      as blocks_per_program,
         programme_categories.name,
         programme_categories.id
     from train
              join last_7_days_sampling
                   on break_flight_id = last_7_days_sampling.id and for_test = FALSE -- fil target week
              join programme_categories on train.programme_category_id = programme_categories.id
              join all_stat on true
     group by programme_categories.id, all_stat.blocks_cnt_total, all_stat.dates_cnt_total
     ) as tmp where programme_categories.id = tmp.id
;

alter table programme_categories add column avg_int_target int;
alter table programme_categories add column effir_rate float;
alter table programme_categories add column blocks_per_program int;

update programme_categories set
                      avg_int_target = tmp.avg_int_target,
                      effir_rate =tmp.effir_rate,
                      blocks_per_program = tmp.blocks_per_program
from
    (with all_stat as (select count(train.break_flight_id) as blocks_cnt_total,
                              count(distinct date)         as dates_cnt_total
                       from train
                                join last_7_days_sampling
                                     on break_flight_id = last_7_days_sampling.id and for_test = FALSE -- fil target week
    )
     select
         --     sum(train.tvr_index) as sum,
         --     sum((tvr_index / 0.01499250375)::int + 1) as int_sum,
         --     count(train.break_flight_id) as cnt,
         sum((tvr_index / 0.01499250375)::int + 1) / count(train.break_flight_id) as avg_int_target,
         count(distinct date)::float / dates_cnt_total                            as effir_rate,
         count(train.break_flight_id) / count(distinct date)                      as blocks_per_program,
         programme_categories.name,
         programme_categories.id
     from train
              join last_7_days_sampling
                   on break_flight_id = last_7_days_sampling.id and for_test = FALSE -- fil target week
              join programme_categories on train.programme_category_id = programme_categories.id
              join all_stat on true
     group by programme_categories.id, all_stat.blocks_cnt_total, all_stat.dates_cnt_total
     ) as tmp where programme_categories.id = tmp.id
;

---

alter table programme_genres add column avg_int_target int;
alter table programme_genres add column effir_rate float;
alter table programme_genres add column blocks_per_program int;

update programme_genres set
                      avg_int_target = tmp.avg_int_target,
                      effir_rate =tmp.effir_rate,
                      blocks_per_program = tmp.blocks_per_program
from
    (with all_stat as (select count(train.break_flight_id) as blocks_cnt_total,
                              count(distinct date)         as dates_cnt_total
                       from train
                                join last_7_days_sampling
                                     on break_flight_id = last_7_days_sampling.id and for_test = FALSE -- fil target week
    )
     select
         --     sum(train.tvr_index) as sum,
         --     sum((tvr_index / 0.01499250375)::int + 1) as int_sum,
         --     count(train.break_flight_id) as cnt,
         sum((tvr_index / 0.01499250375)::int + 1) / count(train.break_flight_id) as avg_int_target,
         count(distinct date)::float / dates_cnt_total                            as effir_rate,
         count(train.break_flight_id) / count(distinct date)                      as blocks_per_program,
         programme_genres.name,
         programme_genres.id
     from train
              join last_7_days_sampling
                   on break_flight_id = last_7_days_sampling.id and for_test = FALSE -- fil target week
              join programme_genres on train.programme_genre_id = programme_genres.id
              join all_stat on true
     group by programme_genres.id, all_stat.blocks_cnt_total, all_stat.dates_cnt_total
     ) as tmp where programme_genres.id = tmp.id
;

