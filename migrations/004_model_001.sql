create table train_002_features (
    id bigint primary key,
    real_flight_start_ts int
);

insert into train_002_features
    (
        id,
        real_flight_start_ts
    )
select
    break_flight_id as id,
    EXTRACT(epoch FROM real_flight_start) as real_flight_start_ts
from train;
