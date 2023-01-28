# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuit.csv file
# MAGIC #### step 1 - read csv file using the spark dataframe reader

# COMMAND ----------


dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# v_file_date

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# display(dbutils.fs.mounts())

# COMMAND ----------

#%fs
#ls /mnt/formula1dlriki/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId",IntegerType(),False),
                                    StructField("circuitRef",StringType(),True),
                                    StructField("name",StringType(),True),
                                    StructField("location",StringType(),True),
                                    StructField("country",StringType(),True),
                                    StructField("lat",DoubleType(),True),
                                    StructField("lng",DoubleType(),True),
                                    StructField("alt",IntegerType(),True),
                                    StructField("url",StringType(),True)])

# COMMAND ----------

circuits_df = spark.read \
    .option("header",True) \
    .schema(circuits_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

# type(circuits_df)

# COMMAND ----------

# circuits_df.show(5)

# COMMAND ----------

# display(circuits_df)

# COMMAND ----------

# circuits_df.printSchema()

# COMMAND ----------

# display(circuits_df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC #### select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# display(circuits_selected_df)

# COMMAND ----------

# circuits_selected_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id") \
    .withColumnRenamed("circuitRef","circuit_ref") \
    .withColumnRenamed("lat","latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude") \
    .withColumn("data_source",lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 4 - add ingestion date to the dataframe

# COMMAND ----------

# circuits_final_df = circuits_renamed_df.withColumn("ingestion_date",current_timestamp()) 
circuits_final_df = add_ingestion_date(circuits_renamed_df)

    #.withColumn("env",lit("Production"))

# COMMAND ----------

# display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### write data to datalake as parquet

# COMMAND ----------

# saving as table : database.<table name>
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")  

# COMMAND ----------

#%sql 
#select * from f1_processed.circuits

# COMMAND ----------

#%fs
#ls /mnt/formula1dlriki/processed/circuits


# COMMAND ----------

# df = spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

# display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

