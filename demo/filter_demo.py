# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# races_filtered_df = races_df.filter("race_year = 2019")
# races_filtered_df = races_df.filter("race_year = 2019 and round <= 5")
races_filtered_df = races_df.filter((col("race_year") == 2019) & (col("round") <= 5))

# COMMAND ----------

# display(races_filtered_df)
display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark join transformation

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("name","circuit_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019") \
    .withColumnRenamed("name","race_name")

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id) \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### outer join

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
    .filter("circuit_id < 70") \
    .withColumnRenamed("name","circuit_name")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019") \
    .withColumnRenamed("name","race_name")

# COMMAND ----------

display(circuits_df)
# display(races_df)

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### semi joins

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country)#, races_df.race_name, races_df.round)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti") \
#     .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country)

races_circuits_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "anti")

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### cross join

# COMMAND ----------

races_circuits_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

print(races_df.count())
print(circuits_df.count())
print(races_circuits_df.count())

# COMMAND ----------

21*69

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in demo

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from global_temp.gv_race_results
# MAGIC select * from demo.v_race_results