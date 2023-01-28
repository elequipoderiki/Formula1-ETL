-- Databricks notebook source


-- COMMAND ----------

select *
  from f1_presentation.calculated_race_results

-- COMMAND ----------

use f1_presentation

-- COMMAND ----------

select team_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  round(avg(calculated_points),2) as avg_points
  from calculated_race_results
  group by team_name
  having total_races >= 100
  order by avg_points desc

-- COMMAND ----------

select team_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  round(avg(calculated_points),2) as avg_points
  from calculated_race_results
  group by team_name
  having total_races >= 100
  order by avg_points desc


-- COMMAND ----------

select 
  team_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  round(avg(calculated_points),2) as avg_points
  from calculated_race_results
  where race_year between 2001 and 2010
  group by team_name
  having total_races >= 100
  order by avg_points desc

-- COMMAND ----------



-- COMMAND ----------

