# Databricks notebook source
#%fs
#ls /mnt/formula1dlriki/raw

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId",IntegerType(),False),
                                   StructField("raceId",IntegerType(),False),
                                   StructField("driverId",IntegerType(),False),
                                   StructField("constructorId",IntegerType(),False),
                                   StructField("number",IntegerType(),True),
                                   StructField("grid",IntegerType(),False),
                                   StructField("position",IntegerType(),True),
                                   StructField("positionText",StringType(),False),
                                   StructField("positionOrder",IntegerType(),False),
                                   StructField("points",FloatType(),False),
                                   StructField("laps",IntegerType(),False),
                                   StructField("time",StringType(),True),
                                   StructField("milliseconds",IntegerType(),True),
                                   StructField("fastestLap",IntegerType(),True),
                                   StructField("rank",IntegerType(),True),
                                   StructField("fastestLapTime",StringType(),True),
                                   StructField("fastestLapSpeed",FloatType(),True),
                                   StructField("statusId",StringType(),True)
                                   ])

# COMMAND ----------

results_df = spark.read \
    .schema(results_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# results_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId","result_id") \
                            .withColumnRenamed("raceId","race_id") \
                            .withColumnRenamed("driverId","driver_id") \
                            .withColumnRenamed("constructorId","constructor_id") \
                            .withColumnRenamed("positionText","position_text") \
                            .withColumnRenamed("positionOrder","position_order") \
                            .withColumnRenamed("fastestLap","fastest_lap") \
                            .withColumnRenamed("fastestLapTime","fastest_lap_time") \
                            .withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
                            .withColumn("ingestion_date",current_timestamp()) \
                            .withColumn("data_source", lit(v_data_source)) \
                            .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

result_final_df = results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

# %sql
# select race_id, driver_id, count(1)
# from f1_processed.results
# group by race_id , driver_id
# having count(1) > 1
# order by race_id, driver_id desc

# COMMAND ----------

#dropping drivers duplicates
results_deduped_df = result_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### method 1 for incremental loading

# COMMAND ----------

# for race_id_list in result_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"alter table f1_processed.results drop if exists partition (race_id = {race_id_list.race_id})")

# COMMAND ----------

# result_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### method 2 for incremental loading

# COMMAND ----------

# spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

# result_final_df = result_final_df.select("result_id","driver_id", "constructor_id", "number", "grid", "position", "position_text",
#                                         "position_order", "points", "laps", "time", "milliseconds", "fastest_lap", "rank", "fastest_lap_time",
#                                         "fastest_lap_speed", "data_source", "file_date", "ingestion_date", "race_id")

# COMMAND ----------

# if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     result_final_df.write.mode("overwrite").insertInto("f1_processed.results")
# else:
#     result_final_df.write.mode("overwrite").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# calling predefined function to apply method 2
# overwrite_partition(result_final_df,"f1_processed","results","race_id")

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id and tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

### display(spark.read.parquet("/mnt/formula1dlriki/processed/results"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table f1_processed.results;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.race_results where race_year = 2021;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table f1_processed.results