# Databricks notebook source
# display(dbutils.fs.mounts())

# COMMAND ----------

#%fs
#ls /mnt/formula1dlriki/raw

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType , TimestampType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                 StructField("year",IntegerType(),True),
                                 StructField("round",IntegerType(),True),
                                 StructField("circuitId",IntegerType(),True),
                                 StructField("name",StringType(),True),
                                 StructField("date",StringType(),True),
                                 StructField("time",StringType(),True),
                                 StructField("url",StringType(),True)])

# COMMAND ----------

races_df = spark.read.schema(races_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv",header=True)

# COMMAND ----------

# display(races_df)

# COMMAND ----------

from pyspark.sql.functions  import col, lit

# COMMAND ----------

races_selected_df = races_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"),col("round"), \
                                    col("circuitId").alias("circuit_id"), col("name"), col("date"),col("time"))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat

# COMMAND ----------

# display(races_selected_df)

# COMMAND ----------

selected_races_df = races_selected_df.withColumn('race_timestamp', to_timestamp(concat(col('date'),lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("ingestion_date",current_timestamp()) \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# selected_races_df.printSchema()

# COMMAND ----------

final_races_df = selected_races_df.drop('date').drop('time')

# COMMAND ----------

# display(final_races_df)

# COMMAND ----------

# final_races_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.races")
final_races_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

#%fs
#ls /mnt/formula1dlriki/processed/races

# COMMAND ----------

# df = spark.read.parquet("/mnt/formula1dlriki/processed/races")

# COMMAND ----------

# display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

v_file_date

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

