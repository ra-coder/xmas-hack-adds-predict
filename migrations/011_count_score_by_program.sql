select
--     score * 0.01499250375 as score,
--     tvr_index,
    programme,
    sum(abs(score - train.tvr_index) / train.tvr_index)  / count(break_flight_id) as mapre
from
    train
join predict_on_test on train.break_flight_id = predict_on_test.id
group by programme;


select
--     score * 0.01499250375 as score,
--     tvr_index,
    programme,
    sum(abs(score  - train.tvr_index) / train.tvr_index)  / count(break_flight_id) as mapre
from
    train
join predict_on_learn on train.break_flight_id = predict_on_learn.id
group by programme
order by programme;

