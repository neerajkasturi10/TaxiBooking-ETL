from pyspark import pipelines as sdp
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Inititalizing a schema for the streaming data
streaming_data_schema = StructType([StructField('booking_id', StringType(), True), StructField('event_type', StringType(), True), StructField('event_timestamp', TimestampType(), True), StructField('source', StringType(), True), StructField('ride_status', StringType(), True), StructField('customer', StructType([StructField('avg_rating', DoubleType(), True), StructField('email', StringType(), True), StructField('name', StringType(), True), StructField('phone', StringType(), True)]), True), StructField('pickup_location', StructType([StructField('latitude', DoubleType(), True), StructField('longitude', DoubleType(), True), StructField('name', StringType(), True)]), True), StructField('dropoff_location', StructType([StructField('latitude', DoubleType(), True), StructField('longitude', DoubleType(), True), StructField('name', StringType(), True)]), True), StructField('car_type', StringType(), True), StructField('distance_km', DoubleType(), True), StructField('estimated_duration_min', DoubleType(), True), StructField('price', StructType([StructField('amount', DoubleType(), True), StructField('currency', StringType(), True)]), True), StructField('payment', StructType([StructField('card', StructType([StructField('brand', StringType(), True), StructField('holder_name', StringType(), True), StructField('last4', StringType(), True)]), True), StructField('method', StringType(), True)]), True), StructField('driver', StructType([StructField('driver_id', StringType(), True), StructField('name', StringType(), True), StructField('rating', DoubleType(), True), StructField('vehicle_make', StringType(), True), StructField('vehicle_model', StringType(), True), StructField('vehicle_plate', StringType(), True)]), True), StructField('promo_code', StringType(), True)])

# Create the target streaming table with schema for append flows
sdp.create_streaming_table(
    name="taxi_ridingapp_catalog.silver.bookings_stg",
    schema=streaming_data_schema
)

#bulk/backfill load
@sdp.append_flow(
    target = "taxi_ridingapp_catalog.silver.bookings_stg",
    once=True
)
def hist_ingestion():
    df_hist = spark.read.table("taxi_ridingapp_catalog.bronze.historical_bookings")
    # Convert JSON string columns back to structs to match streaming_data_schema
    # for field in streaming_data_schema.fields:
    #     if isinstance(field.dataType, StructType):
    #         df_hist = df_hist.withColumn(field.name, from_json(col(field.name), field.dataType))
    
    df_hist = df_hist.withColumn('event_timestamp', to_timestamp(col('event_timestamp'), 'yyyy-MM-dd HH:mm:ss'))
    return df_hist

#streaming load
@sdp.append_flow(
    target = "taxi_ridingapp_catalog.silver.bookings_stg"
)
def stream_ingestion():
    df_stream = spark.readStream.table("taxi_ridingapp_catalog.bronze.raw_ingestion")

    df_stream_rides_extracted = df_stream.withColumn('rides_extracted', from_json('rides', streaming_data_schema))\
                               .select('rides_extracted.*')

    df_stream_rides_extracted = df_stream_rides_extracted.withColumn('event_timestamp', to_timestamp(col('event_timestamp'), 'yyyy-MM-dd HH:mm:ss'))
    return df_stream_rides_extracted



