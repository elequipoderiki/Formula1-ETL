# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run
# MAGIC "../includes/common_functions"

# COMMAND ----------

# MAGIC %run
# MAGIC "../includes/configuration"

# COMMAND ----------

race_results_list = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(f"file_date = '{v_file_date}'") \
    .select("race_year") \
    .distinct() \
    .collect()

# COMMAND ----------

race_year_list = []
column_value_list = [row["race_year"] for row in race_results_list]
for race_year in column_value_list:
    race_year_list.append(race_year)
    

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

# COMMAND ----------

# race_results_df.columns

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

driver_standings_df = race_results_df.groupby("race_year","driver_name","driver_nationality") \
    .agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

# display(driver_standings_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

# COMMAND ----------

# ranking based on total_points and wins values, if two items have these same values then they rank the same
driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

# display(final_df.filter("race_year = 2020"))

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")
# overwrite_partition(final_df,"f1_presentation","driver_standings","race_year")

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name and tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_presentation', 'driver_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# %sql
# select * from f1_presentation.driver_standings where race_year >= 2021

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table f1_presentation.driver_standings

# COMMAND ----------

