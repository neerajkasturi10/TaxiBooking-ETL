# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
#from pyspark import pipelines as sdp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Event Hubs configuration
EH_NAMESPACE                    = "RideApp-Events" #spark.conf.get("iot.ingestion.eh.namespace")
EH_NAME                         = "riding_topic" #spark.conf.get("iot.ingestion.eh.name")

#EH_CONN_STR                     = spark.conf.get("connection_string")
EH_CONN_STR                     = dbutils.secrets.get(scope="rideapp", key="eh_connection_string")
# Kafka Consumer configuration

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : 10000,
  "kafka.session.timeout.ms" : 10000,
  "maxOffsetsPerTrigger"     : 10000,
  "failOnDataLoss"           : 'true',
  "startingOffsets"          : 'earliest'
}

df = spark.readStream.format("kafka") \
        .options(**KAFKA_OPTIONS) \
        .load()

df = df.withColumn("rides", col("value").cast("string"))
# Display the streaming DataFrame (for testing Event Hubs connectivity)
# Note: display() on streaming DataFrames creates a temporary streaming query
display(df, checkpointLocation = "/Volumes/taxi_ridingapp_catalog/bronze/my_volume_2/my_next_volume_v2/", outputMode = "append")

# COMMAND ----------

df.printSchema

# COMMAND ----------

display(df, checkpointLocation = "/Volumes/taxi_ridingapp_catalog/bronze/my_volume/volume_folder/")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from taxi_ridingapp_catalog.silver.bookings_stg;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxi_ridingapp_catalog.bronze.car_type_mapper

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import *

# COMMAND ----------

df_json = spark.read.table("taxi_ridingapp_catalog.bronze.raw_ingestion").select("rides")


display(df_json)
#df_json.printSchema

# COMMAND ----------

df_hist = spark.read.table("taxi_ridingapp_catalog.bronze.historical_bookings")
display(df_hist)


# COMMAND ----------

df_hist.schema

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxi_ridingapp_catalog.bronze.bookings_stg 
# MAGIC where booking_id = '32eeba93-6507-4457-9652-00f0554b1f84'
# MAGIC limit 10000;

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

streaming_data_schema = StructType([StructField('booking_id', StringType(), True), StructField('event_type', StringType(), True), StructField('event_timestamp', StringType(), True), StructField('source', StringType(), True), StructField('ride_status', StringType(), True), StructField('customer', StructType([StructField('avg_rating', DoubleType(), True), StructField('email', StringType(), True), StructField('name', StringType(), True), StructField('phone', StringType(), True)]), True), StructField('pickup_location', StructType([StructField('latitude', DoubleType(), True), StructField('longitude', DoubleType(), True), StructField('name', StringType(), True)]), True), StructField('dropoff_location', StructType([StructField('latitude', DoubleType(), True), StructField('longitude', DoubleType(), True), StructField('name', StringType(), True)]), True), StructField('car_type', StringType(), True), StructField('distance_km', DoubleType(), True), StructField('estimated_duration_min', DoubleType(), True), StructField('price', StructType([StructField('amount', DoubleType(), True), StructField('currency', StringType(), True)]), True), StructField('payment', StructType([StructField('card', StructType([StructField('brand', StringType(), True), StructField('holder_name', StringType(), True), StructField('last4', StringType(), True)]), True), StructField('method', StringType(), True)]), True), StructField('driver', StructType([StructField('driver_id', StringType(), True), StructField('name', StringType(), True), StructField('rating', DoubleType(), True), StructField('vehicle_make', StringType(), True), StructField('vehicle_model', StringType(), True), StructField('vehicle_plate', StringType(), True)]), True), StructField('promo_code', StringType(), True)])

# COMMAND ----------

df_stream = spark.read.table("taxi_ridingapp_catalog.bronze.raw_ingestion")

df_stream_rides = df_stream.withColumn('rides_extracted', from_json('rides', streaming_data_schema))\
                           .select('rides_extracted.*')

# COMMAND ----------

display(df_stream_rides)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxi_ridingapp_catalog.bronze.payment_method_mapper

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxi_ridingapp_catalog.bronze.historical_bookings
# MAGIC where booking_id = '429cf184-5fa6-4832-b067-26e1a67a04a0'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from taxi_ridingapp_catalog.silver.bookings_stg

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxi_ridingapp_catalog.silver.bookings_stg
# MAGIC where source != 'synthetic_generator'
# MAGIC --event_timestamp is not null
# MAGIC --booking_id = '429cf184-5fa6-4832-b067-26e1a67a04a0'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxi_ridingapp_catalog.silver.riding_app_obt
# MAGIC --where booking_id = 'ee505f44-7d61-4430-b8d7-57c5a10d3fe9'

# COMMAND ----------

df_customer = spark.read.table("taxi_ridingapp_catalog.silver.riding_app_obt")\
                              .select('customer.name','customer.phone','customer.email','customer.avg_rating',)

display(df_customer)


# COMMAND ----------

df_driver = spark.read.table("taxi_ridingapp_catalog.silver.riding_app_obt")\
                      .select('driver.driver_id','driver.name','driver.rating','driver.vehicle_make','driver.vehicle_model','driver.vehicle_plate')
display(df_driver)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxi_ridingapp_catalog.gold.dim_passenger

# COMMAND ----------

df_booking  = spark.read.table("taxi_ridingapp_catalog.silver.riding_app_obt")\
                      .select('booking_id','event_type','event_timestamp','ride_status','pickup_location','dropoff_location','car_type','distance_km','estimated_duration_min','price.amount','payment.method','capacity','promo_code')
display(df_booking)

# COMMAND ----------

import pandas as pd
url_historical = f"https://dlridingappdev.blob.core.windows.net/raw/bulk_ingestion/historical_bookings.json?sp=r&st=2026-09-13T14:29:59Z&se=2026-09-29T22:44:59Z&spr=https&sv=2026-02-06&sr=c&sig=rOoD0HPkj94%2BUqB67dzlK0%2BffBU5uo2F%2B9aHI%2Fu4zAQ%3D"

#reading the historical data
df = pd.read_json(url_historical, lines=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS taxi_ridingapp_catalog.bronze.historical_bookings

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS taxi_ridingapp_catalog.bronze.car_type_mapper;
# MAGIC DROP TABLE IF EXISTS taxi_ridingapp_catalog.bronze.card_brand_mapper;
# MAGIC DROP TABLE IF EXISTS taxi_ridingapp_catalog.bronze.location_mapper;
# MAGIC DROP TABLE IF EXISTS taxi_ridingapp_catalog.bronze.payment_method_mapper;
# MAGIC DROP TABLE IF EXISTS taxi_ridingapp_catalog.bronze.vehicle_model_mapper;