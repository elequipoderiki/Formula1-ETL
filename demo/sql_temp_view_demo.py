# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

race_results_2019_df = spark.sql("select * from v_race_results where race_year = 2019")

# COMMAND ----------

p_race_year = 2019
race_results_2019_df = spark.sql(f"select * from v_race_results where race_year = {p_race_year}")

# COMMAND ----------

display(race_results_2019_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### global view

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results;

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

