create table programmes (
    id serial primary key,
    name text
);

insert into programmes (name)
select distinct programme from xmas_hack_adds_predict.public.train order by programme;

create index on programmes (id);
create index on programmes using hash (name);

--alter table train drop column programme_id;
alter table train add column programme_id int references programmes;

update train
set programme_id = id
from (
select
    id,
    name
from programmes) as _tmp where _tmp.name = programme;
