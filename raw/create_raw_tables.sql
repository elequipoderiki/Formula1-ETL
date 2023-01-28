-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### creating tables from csv files

-- COMMAND ----------

create database if not exists f1_raw;

-- COMMAND ----------

drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits(circuitId int,
  circuitRef string,
  name string,
  location string,
  country string,
  lat double,
  lng double,
  alt int,
  url string
)
using csv
options (path "/mnt/formula1dlriki/raw/circuits.csv", header true)

-- COMMAND ----------

select * from f1_raw.circuits

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races(
  raceId int,
  year int,
  round int,
  circuitId int,
  name string,
  date date,
  time string,
  url string
)
using csv
options (path "/mnt/formula1dlriki/raw/races.csv" , header true )


-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### creating tables from json files

-- COMMAND ----------

drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors (
  constructorId int,
  constructorRef string,
  name string,
  nationality string,
  url string
)
using json
options (path "/mnt/formula1dlriki/raw/constructors.json")

-- COMMAND ----------

select * from f1_raw.constructors

-- COMMAND ----------

-- creating from nesting json
drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers (
  driverId int,
  driverRef string,
  number int,
  code string,
  name struct<forename: string, surname: string>,
  dob date,
  nationality string,
  url string
)
using json
options (path "/mnt/formula1dlriki/raw/drivers.json")

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results(
  resultId int,
  raceId int,
  driverId int,
  constructorId int,
  number int,
  grit int,
  position int,
  positionText string,
  positionOrder int,
  points int,
  laps int,
  time string,
  milliseconds int,
  fastestLap int,
  rank int,
  fastestLapTime string,
  fastestLapSpeed float,
  statusId string
)
using json
options (path "/mnt/formula1dlriki/raw/results.json")

-- COMMAND ----------

select * from f1_raw.results

-- COMMAND ----------

-- creating table from multiline json
drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops(
  driverId int,
  duration string,
  lap int,
  milliseconds int,
  raceId int,
  stop int,
  time string
)
using json
options (path "/mnt/formula1dlriki/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

select * from f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### tables joining multiple files

-- COMMAND ----------

-- creating table from folder 
drop table if exists f1_raw.lap_times;
create table if not exists f1_raw.lap_times (
  raceId int,
  driverId int,
  lap int,
  position int,
  time string,
  milliseconds int
)
using csv
options (path "/mnt/formula1dlriki/raw/lap_times")

-- COMMAND ----------

select count(1) from f1_raw.lap_times

-- COMMAND ----------

drop table if exists f1_raw.qualifying ;
create table if not exists f1_raw.qualifying (
  constructorId int,
  driverId int,
  number int,
  position int,
  q1 string,
  q2 string,
  q3 string,
  qualifyId int,
  raceId int
)
using json
options (path "/mnt/formula1dlriki/raw/qualifying", multiLine true)

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

desc extended f1_raw.qualifying

-- COMMAND ----------

-- MAGIC %md
-- MAGIC creating table from parquet

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### processed database

-- COMMAND ----------

create database if not exists f1_processed
location "/mnt/formula1dlriki/processed"

-- COMMAND ----------

desc database f1_processed;

-- COMMAND ----------

select * from f1_processed.circuits

-- COMMAND ----------

desc extended f1_processed.circuits

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### presentation database

-- COMMAND ----------

create database if not exists f1_presentation
location "/mnt/formula1dlriki/presentation"

-- COMMAND ----------

desc extended f1_presentation.constructor_standings
-- show tables in f1_presentation

-- COMMAND ----------

