-- Databricks notebook source
show databases;

-- COMMAND ----------

use f1_processed

-- COMMAND ----------

show tables;

-- COMMAND ----------

select * from drivers;

-- COMMAND ----------

desc drivers;

-- COMMAND ----------

select * from drivers
where lower(nationality) = 'british' and dob >= '1990-01-01'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### sql functions

-- COMMAND ----------

select * ,concat(driver_ref, '-', code) from drivers

-- COMMAND ----------

select * ,split(name, ' ')[0]  forename, split(name, ' ')[1] surname
from drivers

-- COMMAND ----------

select *, date_format(dob, 'dd-MM-yyyy') 
from drivers

-- COMMAND ----------

select max(dob) from drivers

-- COMMAND ----------

select nationality, count(*)
from drivers
group by nationality

-- COMMAND ----------

select nationality, name, dob, rank() over(partition by nationality order by dob desc) as age_rank
from drivers
order by nationality, age_rank

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### joins

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use f1_presentation;
desc driver_standings

-- COMMAND ----------

create or replace temp view v_driver_standings_2018
as
select race_year, driver_name, team, total_points, wins, rank
  from driver_standings
where race_year = 2018;

-- COMMAND ----------

create or replace temp view v_driver_standings_2020
as 
select race_year, driver_name, team, total_points, wins, rank
  from driver_standings
where race_year = 2020;

-- COMMAND ----------

-- only raws having both 2018 and 2020 races
select *
  from v_driver_standings_2018 d_2018
  join v_driver_standings_2020 d_2020
    on (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- all records from 2018
select *
  from v_driver_standings_2018 d_2018
  left join v_driver_standings_2020 d_2020
    on (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- all records from 2020
select *
  from v_driver_standings_2018 d_2018
  right join v_driver_standings_2020 d_2020
    on (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- all both 2018 and 2020 
select *
  from v_driver_standings_2018 d_2018
  full join v_driver_standings_2020 d_2020
    on (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- only data from 2018 none from 2020
select *
  from v_driver_standings_2018 d_2018
  semi join v_driver_standings_2020 d_2020
    on (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- excluded 2018 data from join selection
select *
  from v_driver_standings_2018 d_2018
  anti join v_driver_standings_2020 d_2020
    on (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- cartesian product 2018 vs 2020 and no joining condition
select *
  from v_driver_standings_2018 d_2018
  cross join v_driver_standings_2020 d_2020


-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

