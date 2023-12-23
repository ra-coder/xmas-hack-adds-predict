--             'programme_category',


create table programme_categories (
    id serial primary key,
    name text
);

insert into programme_categories (name)
select distinct programme_category from xmas_hack_adds_predict.public.train order by programme_category;

create index on programme_categories (id);
create index on programme_categories using hash (name);

--alter table train drop column programme_id;
alter table train add column programme_category_id int references programme_categories;

update train
set programme_category_id = id
from (
select
    id,
    name
from programme_categories) as _tmp where _tmp.name = programme_category;


--             'programme_genre',

create table programme_genres (
    id serial primary key,
    name text
);

insert into programme_genres (name)
select distinct programme_genre from xmas_hack_adds_predict.public.train order by programme_genre;

create index on programme_genres (id);
create index on programme_genres using hash (name);

--alter table train drop column programme_id;
alter table train add column programme_genre_id int references programme_genres;

update train
set programme_genre_id = id
from (
select
    id,
    name
from programme_genres) as _tmp where _tmp.name = programme_genre;


--             'break_distribution',

create table break_distributions (
    id serial primary key,
    name text
);

insert into break_distributions (name)
select distinct break_distribution from xmas_hack_adds_predict.public.train order by break_distribution;

create index on break_distributions (id);
create index on break_distributions using hash (name);

--alter table train drop column programme_id;
alter table train add column break_distribution_id int references break_distributions;

update train
set break_distribution_id = id
from (
select
    id,
    name
from break_distributions) as _tmp where _tmp.name = break_distribution;


--             'break_content',

create table break_contents (
    id serial primary key,
    name text
);

insert into break_contents (name)
select distinct break_content from xmas_hack_adds_predict.public.train order by break_content;

create index on break_contents (id);
create index on break_contents using hash (name);

--alter table train drop column programme_id;
alter table train add column break_content_id int references break_contents;

update train
set break_content_id = id
from (
select
    id,
    name
from break_contents) as _tmp where _tmp.name = break_content;
