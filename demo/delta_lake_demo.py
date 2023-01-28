# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC create database if not exists f1_demo
# MAGIC location '/mnt/formula1dlriki/demo'

# COMMAND ----------

results_df = spark.read \
    .option("inferSchema",True) \
    .json("/mnt/formula1dlriki/raw/2021-03-28/results.json")

# COMMAND ----------

# saving to a table
results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# saving to a file
results_df.write.format("delta").mode("overwrite").save("/mnt/formula1dlriki/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table f1_demo.results_external
# MAGIC using delta
# MAGIC location '/mnt/formula1dlriki/demo/results_external'

# COMMAND ----------

results_external_df = spark.read.format("delta").load("/mnt/formula1dlriki/demo/results_external")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC ### updating delta tables

# COMMAND ----------

# MAGIC %sql 
# MAGIC update f1_demo.results_managed
# MAGIC set points = 11 - position
# MAGIC where position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed 
# MAGIC   where position <= 10

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dlriki/demo/results_managed")
deltaTable.update("position <= 10", {"points": "21 - position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed 
# MAGIC   where position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.results_managed
# MAGIC where position > 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dlriki/demo/results_managed")
deltaTable.delete("points =  0")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC #### upsert on delta lake

# COMMAND ----------

drivers_day1_df = spark.read \
    .option("inferSchema", True) \
    .json("/mnt/formula1dlriki/raw/2021-03-28/drivers.json") \
    .filter("driverId <= 10") \
    .select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read \
    .option("inferSchema",True) \
    .json("/mnt/formula1dlriki/raw/2021-03-28/drivers.json") \
    .filter("driverId between 6 and 15") \
    .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

drivers_day3_df = spark.read \
    .option("inferSchema",True) \
    .json("/mnt/formula1dlriki/raw/2021-03-28/drivers.json") \
    .filter("driverId between 1 and 5 or driverId between 16 and 20") \
    .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.drivers_merge (
# MAGIC   driverId int,
# MAGIC   dob date,
# MAGIC   forename string,
# MAGIC   surname string,
# MAGIC   createdDate date,
# MAGIC   updatedDate date
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into f1_demo.drivers_merge tgt
# MAGIC using drivers_day1 upd
# MAGIC on tgt.driverId = upd.driverId
# MAGIC when matched then
# MAGIC   update set tgt.dob = upd.dob,
# MAGIC         tgt.forename = upd.forename,
# MAGIC         tgt.surname = upd.surname,
# MAGIC         tgt.updatedDate = current_timestamp
# MAGIC when not matched
# MAGIC   then insert (driverId, dob, forename, surname, createdDate) values (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into f1_demo.drivers_merge tgt
# MAGIC using drivers_day2 upd
# MAGIC on tgt.driverId = upd.driverId
# MAGIC when matched then
# MAGIC   update set tgt.dob = upd.dob,
# MAGIC         tgt.forename = upd.forename,
# MAGIC         tgt.surname = upd.surname,
# MAGIC         tgt.updatedDate = current_timestamp
# MAGIC when not matched
# MAGIC   then insert (driverId, dob, forename, surname, createdDate) values (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge;

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dlriki/demo/drivers_merge")

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
    .whenMatchedUpdate(set = {"dob": "upd.dob", "forename": "upd.forename", "surname": "upd.surname", "updatedDate": "current_timestamp()"}) \
    .whenNotMatchedInsert(values = 
         {
            "driverId": "upd.driverId",
             "dob": "upd.dob",
             "forename": "upd.forename",
             "surname": "upd.surname",
             "createdDate": "current_timestamp()"
         }
     ) \
    .execute()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md 
# MAGIC ### history table

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge version as of 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge timestamp as of '2023-01-18T20:03:04.000+0000'

# COMMAND ----------

# %sql
# merge into f1_demo.drivers_merge tgt
# using drivers_day2 upd
# on tgt.driverId = upd.driverId
# when matched then
#   update set tgt.dob = upd.dob,
#         tgt.forename = upd.forename,
#         tgt.surname = upd.surname,
#         tgt.updatedDate = current_timestamp
#         tgt.dummy = current_timestamp
# when not matched
#   then insert (driverId, dob, forename, surname, createdDate) values (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf",'2023-01-18T20:03:04.000+0000').load("/mnt/formula1dlriki/demo/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### removing history

# COMMAND ----------

# will remove the history 7 days after the request
%sql
vacuum f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge timestamp as of '2023-01-18T20:03:04.000+0000'

# COMMAND ----------

# removing the history right now
%sql
set spark.databricks.delta.retentionDurationCheck.enabled = false;
vacuum f1_demo.drivers_merge retain 0 hours

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge timestamp as of '2023-01-18T20:03:04.000+0000'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC desc history f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.drivers_merge where driverId = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge 

# COMMAND ----------

# the last active version of the table is 3 because this is the modified table
%sql
select * from f1_demo.drivers_merge version as of 3

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC inserting the removed row into actual table using history 

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into f1_demo.drivers_merge tgt
# MAGIC using f1_demo.drivers_merge version as of 3 src
# MAGIC   on (tgt.driverId = src.driverId)
# MAGIC when not matched then
# MAGIC   insert *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ### transaction logs

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table if exists f1_demo.drivers_txn;
# MAGIC create table if not exists f1_demo.drivers_txn (
# MAGIC   driverId int,
# MAGIC   dob date,
# MAGIC   forename string,
# MAGIC   surname string,
# MAGIC   createdDate date,
# MAGIC   updatedDate date
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc history f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_txn
# MAGIC select * from f1_demo.drivers_merge 
# MAGIC where driverId = 1; 

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_txn
# MAGIC select * from f1_demo.drivers_merge 
# MAGIC where driverId = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.drivers_txn
# MAGIC where driverId = 1;

# COMMAND ----------

for driver_id in range(3,20):
    spark.sql(f""" insert into f1_demo.drivers_txn 
                select * from f1_demo.drivers_merge
                where driverId = {driver_id}""")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_txn
# MAGIC select * from f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC ### parquet to delta tables

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table if not exists f1_demo.drivers_convert_to_delta (
# MAGIC driverId int,
# MAGIC dob date,
# MAGIC forename string,
# MAGIC surname string,
# MAGIC createdDate date,
# MAGIC updatedDate date
# MAGIC )
# MAGIC using parquet;

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into f1_demo.drivers_convert_to_delta
# MAGIC select * from f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC converting to delta

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta f1_demo.drivers_convert_to_delta

# COMMAND ----------

# MAGIC %md
# MAGIC creating parquet from delta 

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("/mnt/formula1dlriki/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %md
# MAGIC converting parquet to delta

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta parquet.`/mnt/formula1dlriki/demo/drivers_convert_to_delta_new`

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

