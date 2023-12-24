drop table if exists train_predict_by_p2;
create table train_predict_by_p2
(
    id    bigint not null
        primary key,
    score double precision
);
    
insert into train_predict_by_p2 select * from train_predict_for_p2_1;
insert into train_predict_by_p2 select * from train_predict_for_p2_2;
insert into train_predict_by_p2 select * from train_predict_for_p2_3;
insert into train_predict_by_p2 select * from train_predict_for_p2_4;
insert into train_predict_by_p2 select * from train_predict_for_p2_5;
insert into train_predict_by_p2 select * from train_predict_for_p2_6;
insert into train_predict_by_p2 select * from train_predict_for_p2_7;
insert into train_predict_by_p2 select * from train_predict_for_p2_8;
insert into train_predict_by_p2 select * from train_predict_for_p2_9;
insert into train_predict_by_p2 select * from train_predict_for_p2_10;
insert into train_predict_by_p2 select * from train_predict_for_p2_11;
insert into train_predict_by_p2 select * from train_predict_for_p2_12;
insert into train_predict_by_p2 select * from train_predict_for_p2_13;
insert into train_predict_by_p2 select * from train_predict_for_p2_14;
insert into train_predict_by_p2 select * from train_predict_for_p2_15;
insert into train_predict_by_p2 select * from train_predict_for_p2_16;
insert into train_predict_by_p2 select * from train_predict_for_p2_17;
insert into train_predict_by_p2 select * from train_predict_for_p2_18;
insert into train_predict_by_p2 select * from train_predict_for_p2_19;
insert into train_predict_by_p2 select * from train_predict_for_p2_20;
insert into train_predict_by_p2 select * from train_predict_for_p2_21;
insert into train_predict_by_p2 select * from train_predict_for_p2_22;
insert into train_predict_by_p2 select * from train_predict_for_p2_23;
insert into train_predict_by_p2 select * from train_predict_for_p2_24;
insert into train_predict_by_p2 select * from train_predict_for_p2_25;
insert into train_predict_by_p2 select * from train_predict_for_p2_26;


select
--     score * 0.01499250375 as score,
--     tvr_index,
     programme,
    (sum(abs(score  - train.tvr_index) / train.tvr_index)  / count(break_flight_id))::numeric(16,8) as mape_on_all_train
from
    train
join train_predict_by_p2 on train.break_flight_id = train_predict_by_p2.id
where train.tvr_index != 0
group by programme
order by programme;

---------------

drop table if exists test_data_predict_by_p2;
create table test_data_predict_by_p2
(
    id    bigint not null
        primary key,
    score double precision
);

insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_1;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_2;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_3;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_4;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_5;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_6;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_7;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_8;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_9;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_10;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_11;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_12;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_13;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_14;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_15;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_16;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_17;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_18;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_19;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_20;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_21;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_22;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_23;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_24;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_25;
insert into test_data_predict_by_p2 select * from test_data_predict_for_p2_26;