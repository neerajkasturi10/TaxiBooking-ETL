from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Configure Azure Blob Storage SAS token for wasbs:// access
SAS_TOKEN = "sp=r&st=2026-09-13T14:29:59Z&se=2026-09-29T22:44:59Z&spr=https&sv=2026-02-06&sr=c&sig=rOoD0HPkj94%2BUqB67dzlK0%2BffBU5uo2F%2B9aHI%2Fu4zAQ%3D"
spark.conf.set("fs.azure.sas.raw.dlridingappdev.blob.core.windows.net", SAS_TOKEN)

# Base paths for Azure Blob Storage
MAPPER_PATH = "wasbs://raw@dlridingappdev.blob.core.windows.net/ingestion"
HISTORICAL_PATH = "wasbs://raw@dlridingappdev.blob.core.windows.net/bulk_ingestion/historical_bookings.json"


@dp.materialized_view(name="taxi_ridingapp_catalog.bronze.car_type_mapper")
def car_type_mapper():
    raw = spark.read.format("text").option("wholetext", "true").load(f"{MAPPER_PATH}/car_type_mapper.json")
    schema = "MAP<STRING, STRUCT<id: BIGINT, category: STRING, capacity: BIGINT, base_fare: DOUBLE, per_km_rate: DOUBLE, multiplier: DOUBLE>>"
    df = raw.select(from_json(col("value"), schema).alias("data"))
    df = df.select(explode("data").alias("car_type", "fields"))
    return df.select("car_type", "fields.*").withColumn("updated_at", current_timestamp())


@dp.materialized_view(name="taxi_ridingapp_catalog.bronze.card_brand_mapper")
def card_brand_mapper():
    raw = spark.read.format("text").option("wholetext", "true").load(f"{MAPPER_PATH}/card_brand_mapper.json")
    schema = "MAP<STRING, STRUCT<id: BIGINT, country_of_origin: STRING, network_type: STRING>>"
    df = raw.select(from_json(col("value"), schema).alias("data"))
    df = df.select(explode("data").alias("card_brand", "fields"))
    return df.select("card_brand", "fields.*").withColumn("updated_at", current_timestamp())


@dp.materialized_view(name="taxi_ridingapp_catalog.bronze.location_mapper")
def location_mapper():
    raw = spark.read.format("text").option("wholetext", "true").load(f"{MAPPER_PATH}/location_mapper.json")
    schema = "MAP<STRING, STRUCT<id: BIGINT, city: STRING, state: STRING, region: STRING, zip: BIGINT, latitude: DOUBLE, longitude: DOUBLE>>"
    df = raw.select(from_json(col("value"), schema).alias("data"))
    df = df.select(explode("data").alias("location_mapper", "fields"))
    return df.select("location_mapper", "fields.*").withColumn("updated_at", current_timestamp())


@dp.materialized_view(name="taxi_ridingapp_catalog.bronze.payment_method_mapper")
def payment_method_mapper():
    raw = spark.read.format("text").option("wholetext", "true").load(f"{MAPPER_PATH}/payment_method_mapper.json")
    schema = "MAP<STRING, STRUCT<id: BIGINT, description: STRING, is_cashless: BOOLEAN, requires_card_details: BOOLEAN>>"
    df = raw.select(from_json(col("value"), schema).alias("data"))
    df = df.select(explode("data").alias("payment_method", "fields"))
    return df.select("payment_method", "fields.*").withColumn("updated_at", current_timestamp())


@dp.materialized_view(name="taxi_ridingapp_catalog.bronze.vehicle_model_mapper")
def vehicle_model_mapper():
    raw = spark.read.format("text").option("wholetext", "true").load(f"{MAPPER_PATH}/vehicle_model_mapper.json")
    schema = "MAP<STRING, STRUCT<id: BIGINT, make: STRING, model: STRING, vehicle_category: STRING, fuel_type: STRING>>"
    df = raw.select(from_json(col("value"), schema).alias("data"))
    df = df.select(explode("data").alias("vehicle_model", "fields"))
    return df.select("vehicle_model", "fields.*").withColumn("updated_at", current_timestamp())


@dp.materialized_view(name="taxi_ridingapp_catalog.bronze.historical_bookings")
def historical_bookings():
    return spark.read.format("json").load(HISTORICAL_PATH)