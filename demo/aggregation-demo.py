# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

race_results_df.columns

# COMMAND ----------

demo_df = race_results_df.filter("race_year = 2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### count and countdistinct aggregations

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum, lower,col

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

# demo_df.select(count("race_name")).show()
demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

# demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points")).show()
demo_df.filter(lower("driver_name") == "lewis hamilton").select(sum("points")).show()

# COMMAND ----------

demo_df.filter(lower("driver_name") == "lewis hamilton").select(sum("points").alias("sum_points"), countDistinct("race_name").alias("total_race_names")).show()

# COMMAND ----------

demo_df.filter(lower("driver_name") == "lewis hamilton").select(sum("points"), countDistinct("race_name")) \
    .withColumnRenamed("sum(points)", "total_points") \
    .withColumnRenamed("count(distinct race_name)", "number_of_races").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### groupby

# COMMAND ----------

demo_df.groupby("driver_name") \
    .sum("points").show()

# COMMAND ----------

demo_df.groupby("driver_name") \
    .agg(sum("points"), countDistinct("race_name")) \
    .show()

# COMMAND ----------

demo_df.groupby("driver_name") \
    .agg({"points":"sum", "race_name":"count"}) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### window functions

# COMMAND ----------

demo_df = race_results_df.filter("race_year in (2019,2020)")

# COMMAND ----------

demo_grouped_df = demo_df.groupby("race_year", "driver_name") \
    .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")) 

# COMMAND ----------

display(demo_grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank


# COMMAND ----------

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
demo_grouped_df.withColumn("rank", rank().over(driverRankSpec)).show(50)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

