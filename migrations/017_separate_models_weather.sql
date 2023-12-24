drop table if exists train_predict_by_weather;
create table train_predict_by_weather
(
    id    bigint not null
        primary key,
    score double precision
);
    
insert into train_predict_by_weather select * from train_predict_for_weather_1;
insert into train_predict_by_weather select * from train_predict_for_weather_2;
insert into train_predict_by_weather select * from train_predict_for_weather_3;
insert into train_predict_by_weather select * from train_predict_for_weather_4;
insert into train_predict_by_weather select * from train_predict_for_weather_5;
insert into train_predict_by_weather select * from train_predict_for_weather_6;
insert into train_predict_by_weather select * from train_predict_for_weather_7;
insert into train_predict_by_weather select * from train_predict_for_weather_8;
insert into train_predict_by_weather select * from train_predict_for_weather_9;
insert into train_predict_by_weather select * from train_predict_for_weather_10;
insert into train_predict_by_weather select * from train_predict_for_weather_11;
insert into train_predict_by_weather select * from train_predict_for_weather_12;
insert into train_predict_by_weather select * from train_predict_for_weather_13;
insert into train_predict_by_weather select * from train_predict_for_weather_14;
insert into train_predict_by_weather select * from train_predict_for_weather_15;
insert into train_predict_by_weather select * from train_predict_for_weather_16;
insert into train_predict_by_weather select * from train_predict_for_weather_17;
insert into train_predict_by_weather select * from train_predict_for_weather_18;
insert into train_predict_by_weather select * from train_predict_for_weather_19;
insert into train_predict_by_weather select * from train_predict_for_weather_20;
insert into train_predict_by_weather select * from train_predict_for_weather_21;
insert into train_predict_by_weather select * from train_predict_for_weather_22;
insert into train_predict_by_weather select * from train_predict_for_weather_23;
insert into train_predict_by_weather select * from train_predict_for_weather_24;
insert into train_predict_by_weather select * from train_predict_for_weather_25;
insert into train_predict_by_weather select * from train_predict_for_weather_26;


select
--     score * 0.01499250375 as score,
--     tvr_index,
     programme,
    (sum(abs(score  - train.tvr_index) / train.tvr_index)  / count(break_flight_id))::numeric(16,8) as mape_on_all_train
from
    train
join train_predict_by_weather on train.break_flight_id = train_predict_by_weather.id
where train.tvr_index != 0
group by programme
order by programme;

---------------

drop table if exists test_data_predict_by_weather;
create table test_data_predict_by_weather
(
    id    bigint not null
        primary key,
    score double precision
);

insert into test_data_predict_by_weather select * from test_data_predict_for_weather_1;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_2;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_3;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_4;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_5;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_6;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_7;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_8;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_9;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_10;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_11;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_12;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_13;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_14;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_15;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_16;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_17;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_18;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_19;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_20;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_21;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_22;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_23;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_24;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_25;
insert into test_data_predict_by_weather select * from test_data_predict_for_weather_26;