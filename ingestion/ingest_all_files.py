# Databricks notebook source
v_results = dbutils.notebook.run('1.ingest_circuits_file',0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run('ingest_race',0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run('ingest_constructor_file',0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run('ingest_driver_file',0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_results

# COMMAND ----------

# v_results = dbutils.notebook.run('ingestion_results_file',0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run('ingest_pit_stop json file',0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run('ingest_lap_time_file',0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run('ingest_qualifying_file',0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_results

# COMMAND ----------

