drop table if exists test_data_predict_by_p;
create table test_data_predict_by_p
(
    id    bigint not null
        primary key,
    score double precision
);

insert into test_data_predict_by_p select * from test_data_predict_for_p_1;
insert into test_data_predict_by_p select * from test_data_predict_for_p_2;
insert into test_data_predict_by_p select * from test_data_predict_for_p_3;
insert into test_data_predict_by_p select * from test_data_predict_for_p_4;
insert into test_data_predict_by_p select * from test_data_predict_for_p_5;
insert into test_data_predict_by_p select * from test_data_predict_for_p_6;
insert into test_data_predict_by_p select * from test_data_predict_for_p_7;
insert into test_data_predict_by_p select * from test_data_predict_for_p_8;
insert into test_data_predict_by_p select * from test_data_predict_for_p_9;
insert into test_data_predict_by_p select * from test_data_predict_for_p_10;
insert into test_data_predict_by_p select * from test_data_predict_for_p_11;
insert into test_data_predict_by_p select * from test_data_predict_for_p_12;
insert into test_data_predict_by_p select * from test_data_predict_for_p_13;
insert into test_data_predict_by_p select * from test_data_predict_for_p_14;
insert into test_data_predict_by_p select * from test_data_predict_for_p_15;
insert into test_data_predict_by_p select * from test_data_predict_for_p_16;
insert into test_data_predict_by_p select * from test_data_predict_for_p_17;
insert into test_data_predict_by_p select * from test_data_predict_for_p_18;
insert into test_data_predict_by_p select * from test_data_predict_for_p_19;
insert into test_data_predict_by_p select * from test_data_predict_for_p_20;
insert into test_data_predict_by_p select * from test_data_predict_for_p_21;
insert into test_data_predict_by_p select * from test_data_predict_for_p_22;
insert into test_data_predict_by_p select * from test_data_predict_for_p_23;
insert into test_data_predict_by_p select * from test_data_predict_for_p_24;
insert into test_data_predict_by_p select * from test_data_predict_for_p_25;
insert into test_data_predict_by_p select * from test_data_predict_for_p_26;

---
drop table if exists train_predict_by_p;
create table train_predict_by_p
(
    id    bigint not null
        primary key,
    score double precision
);

insert into train_predict_by_p select * from train_predict_for_p_1;
insert into train_predict_by_p select * from train_predict_for_p_2;
insert into train_predict_by_p select * from train_predict_for_p_3;
insert into train_predict_by_p select * from train_predict_for_p_4;
insert into train_predict_by_p select * from train_predict_for_p_5;
insert into train_predict_by_p select * from train_predict_for_p_6;
insert into train_predict_by_p select * from train_predict_for_p_7;
insert into train_predict_by_p select * from train_predict_for_p_8;
insert into train_predict_by_p select * from train_predict_for_p_9;
insert into train_predict_by_p select * from train_predict_for_p_10;
insert into train_predict_by_p select * from train_predict_for_p_11;
insert into train_predict_by_p select * from train_predict_for_p_12;
insert into train_predict_by_p select * from train_predict_for_p_13;
insert into train_predict_by_p select * from train_predict_for_p_14;
insert into train_predict_by_p select * from train_predict_for_p_15;
insert into train_predict_by_p select * from train_predict_for_p_16;
insert into train_predict_by_p select * from train_predict_for_p_17;
insert into train_predict_by_p select * from train_predict_for_p_18;
insert into train_predict_by_p select * from train_predict_for_p_19;
insert into train_predict_by_p select * from train_predict_for_p_20;
insert into train_predict_by_p select * from train_predict_for_p_21;
insert into train_predict_by_p select * from train_predict_for_p_22;
insert into train_predict_by_p select * from train_predict_for_p_23;
insert into train_predict_by_p select * from train_predict_for_p_24;
insert into train_predict_by_p select * from train_predict_for_p_25;
insert into train_predict_by_p select * from train_predict_for_p_26;


select
--     score * 0.01499250375 as score,
--     tvr_index,
--   programme,
    (sum(abs(score  - train.tvr_index) / train.tvr_index)  / count(break_flight_id))::numeric(16,5) as mape_on_all_train
from
    train
join train_predict_by_p on train.break_flight_id = train_predict_by_p.id
where train.tvr_index != 0
-- group by programme
-- order by programme;

