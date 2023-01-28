# Databricks notebook source
# MAGIC %md
# MAGIC ### ingest constructor.json file
# MAGIC #### step 1 - read the json file using the spark dataframe reader

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df  = spark.read \
    .schema(constructor_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/constructors.json") 


# COMMAND ----------

# constructor_df.printSchema()

# COMMAND ----------

# display(constructor_df)

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef","constructor_ref") \
    .withColumn("ingestion_data", current_timestamp()) \
    .withColumn("data_source",lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# display(constructor_final_df)

# COMMAND ----------

# constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")
constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")


# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

v_file_date

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

