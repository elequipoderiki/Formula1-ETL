# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date",current_timestamp())
    return output_df

# COMMAND ----------

def overwrite_partition(dataframe, database, table, partition_key):
    outputdf = reorder_frame_by_key(dataframe,partition_key)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{database}.{table}")):
        outputdf.write.mode("overwrite").insertInto(f"{database}.{table}")
    else:
        outputdf.write.mode("overwrite").partitionBy(partition_key).format("parquet").saveAsTable(f"{database}.{table}")

# COMMAND ----------

# result_final_df.schema.names
def reorder_frame_by_key(dataframe, key):
    columns = []
    for column in dataframe.schema.names:
        if column != key:
            columns.append(column)
    columns.append(key)
    
    output_df = dataframe.select(columns)
    return output_df

# COMMAND ----------

def df_column_to_list(input_df, column_name):
  df_row_list = input_df.select(column_name) \
                        .distinct() \
                        .collect()
  
  column_value_list = [row[column_name] for row in df_row_list]
  return column_value_list

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
  spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")
  from delta.tables import DeltaTable
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
    deltaTable.alias("tgt").merge(
      input_df.alias("src"),
      merge_condition) \
      .whenMatchedUpdateAll() \
      .whenNotMatchedInsertAll() \
      .execute()
  else:
    input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

