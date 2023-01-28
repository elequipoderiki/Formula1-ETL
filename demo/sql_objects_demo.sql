-- Databricks notebook source
create database if not exists demo;

-- COMMAND ----------

show databases;

-- COMMAND ----------

describe database demo;

-- COMMAND ----------

describe database default;

-- COMMAND ----------

describe database extended demo;

-- COMMAND ----------

describe table extended circuits;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use demo;

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #saving to sql table
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

use demo;

-- COMMAND ----------

show tables;

-- COMMAND ----------

-- desc race_results_python
desc extended race_results_python

-- COMMAND ----------

select * from demo.race_results_python

-- COMMAND ----------

create table race_results_sql
as
select *
  from demo.race_results_python
where race_year = 2020;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

desc extended demo.race_results_python

-- COMMAND ----------

drop table demo.race_results_sql

-- COMMAND ----------

show tables;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### creating external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC presentation_folder_path

-- COMMAND ----------

desc extended race_results_ext_py

-- COMMAND ----------

create table demo.race_results_ext_sql
(race_year int,
race_name string,
race_date timestamp,
circuit_location string,
driver_name string,
driver_number int,
driver_nationality string,
team string,
grid int,
fastest_lap int,
race_time string,
points float,
position int,
created_date timestamp) 
using parquet
location "/mnt/formula1dlriki/presentation/race_results_ext_sql"

-- COMMAND ----------

show tables;

-- COMMAND ----------

insert into race_results_ext_sql
select * from race_results_ext_py
where race_year = 2020

-- COMMAND ----------

select count(1) from race_results_ext_sql

-- COMMAND ----------

drop table race_results_ext_sql

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### views

-- COMMAND ----------

use demo;

-- COMMAND ----------

-- temporary view (active on same session until detach from cluster)
create or replace temp view v_race_results
as 
select  * 
  from demo.race_results_python
where race_year = 2018

-- COMMAND ----------

select * from demo.v_race_results

-- COMMAND ----------

-- global temporary view (active until cluster ends)
create or replace global temp view gv_race_results
as
select * 
  from demo.race_results_python
where race_year = 2018

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------

-- permanent view
create or replace view pv_race_results
as 
select  * 
  from demo.race_results_python
where race_year = 2000

-- COMMAND ----------

show tables;

-- COMMAND ----------

select * from demo.pv_race_results

-- COMMAND ----------



-- COMMAND ----------

