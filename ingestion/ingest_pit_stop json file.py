# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                     StructField("driverId",IntegerType(),True),
                                     StructField("stop",StringType(),True),
                                     StructField("lap",IntegerType(),True),
                                     StructField("time",StringType(),True),
                                     StructField("duration",StringType(),True),
                                     StructField("milliseconds",IntegerType(),True)])

# COMMAND ----------

pit_stops_df = spark.read \
    .schema(pit_stops_schema) \
    .option("multiline",True) \
    .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("driverId","driver_id") \
    .withColumnRenamed("raceId","race_id") \
    .withColumn("ingestion_date",current_timestamp()) \
    .withColumn("data_source",lit(v_data_source))  \
    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")
# overwrite_partition(final_df,"f1_processed","pit_stops","race_id")
merge_condition = "tgt.race_id = src.race_id and tgt.driver_id = src.driver_id and tgt.stop = src.stop and tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# display(spark.read.parquet("/mnt/formula1dlriki/processed/pit_stops"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

