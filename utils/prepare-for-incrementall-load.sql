-- Databricks notebook source
select current_database()

-- COMMAND ----------

show databases;

-- COMMAND ----------

show tables in f1_processed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### drop databases

-- COMMAND ----------

-- drop the tables
drop database if exists f1_processed cascade;

-- COMMAND ----------

create database if not exists f1_processed
location "/mnt/formula1dlriki/processed"

-- COMMAND ----------

drop database if exists f1_presentation cascade;

-- COMMAND ----------

create database if not exists f1_presentation
location "/mnt/formula1dlriki/presentation"

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

