-- #  data source https://rp5.ru/Weather_archive_in_Moscow
--
-- # Weather station Moscow, Russia, WMO_ID=27612,selection from 01.01.2023 till 24.12.2023, all days
-- # Encoding: UTF-8
-- # The data is provided by the website "Reliable Prognosis", rp5.ru
-- # If you use the data, please indicate the name of the website.
-- # For meteorological parameters see the address http://rp5.ru/archive.php?wmo_id=27612&lang=en
-- #


-- "Local time in Moscow";"T";"Po";"P";"Pa";"U";"DD";"Ff";"ff10";"ff3";"N";"WW";"W1";"W2";"Tn";"Tx";"Cl";"Nh";"H";"Cm";"Ch";"VV";"Td";"RRR";"tR";"E";"Tg";"E'";"sss"
-- "24.12.2023 06:00";"-0.6";"724.4";"738.1";"-0.4";"90";"Wind blowing from the west-southwest";"1";"";"";"100%.";"State of sky on the whole unchanged. ";"Snow and/or other types of solid precipitation";"Cloud covering more than 1/2 of the sky throughout the appropriate period.";
-- "-0.6";"";"Stratus fractus or Cumulus fractus of bad weather, or both (pannus), usually below Altostratus or Nimbostratus.";"100%.";"300-600";"";"";"18.0";"-2.1";"1.0";"12";"";"";"";"";

drop table moscow_wather;
create table moscow_wather
(
  datetime text,
  T float,
  Po float,
  P float,
  Pa float,
  U float,
  DD text,
  Ff text,
  ff10 text,
  ff3 text,
  N text,
  WW text,
  W1 text,
  W2 text,
  Tn float,
  Tx float,
  Cl text,
  Nh text,
  H text,
  Cm text,
  Ch text,
  VV float,
  Td float,
  RRR text,
  tR float,
  E text,
  Tg text,
  EE text,
  sss text
);

alter table moscow_wather add column date date;

update moscow_wather  set date = TO_DATE(substr(datetime, 1, 10), 'DD.MM.YYYY') where true;
alter table moscow_wather add column hour int;
update moscow_wather set hour = substr(datetime, 11, 3)::int where true;

select
    moscow_wather.date,
    (extract(epoch from train.real_flight_start) / 60 / 60 / 3)::int,
    hour
from train
left join moscow_wather  on train.date = moscow_wather.date and (extract(epoch from train.real_flight_start) / 60 / 60 / 3)::int =  hour
limit  10;