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

qualifying_schema = StructType(fields=[StructField("qualifyId",IntegerType(),False),
                                     StructField("raceId",IntegerType(),True),
                                     StructField("driverId",IntegerType(),True),
                                     StructField("constructorId",IntegerType(),True),
                                     StructField("number",IntegerType(),True),
                                     StructField("position",IntegerType(),True),
                                     StructField("q1",StringType(),True),
                                     StructField("q2",StringType(),True),
                                     StructField("q3",StringType(),True)
                                      ])

# COMMAND ----------

qualifying_df = spark.read \
    .schema(qualifying_schema) \
    .option("multiLine",True) \
    .json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

qualifying_df.count()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId","qualify_id") \
    .withColumnRenamed("driverId","driver_id") \
    .withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("constructorId","constructor_id") \
    .withColumn("ingestion_date",current_timestamp()) \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")
# overwrite_partition(final_df,"f1_processed","qualifying","race_id")
merge_condition = "tgt.qualify_id = src.qualify_id and tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# display(spark.read.parquet("/mnt/formula1dlriki/processed/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

v_file_date

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

