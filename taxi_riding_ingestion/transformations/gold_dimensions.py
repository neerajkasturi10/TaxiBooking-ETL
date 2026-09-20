from pyspark import pipelines as sdp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@sdp.view
def dim_passenger_view():
  df_passenger = spark.readStream.table('taxi_ridingapp_catalog.silver.riding_app_obt')\
                                 .select('customer.name','customer.phone','customer.email','customer.avg_rating')

  df_passenger = df_passenger.dropDuplicates(subset=['name','phone','email'])
  return df_passenger

sdp.create_streaming_table("taxi_ridingapp_catalog.gold.dim_passenger")

sdp.create_auto_cdc_flow(
    target = "taxi_ridingapp_catalog.gold.dim_passenger",
    source = "dim_passenger_view",
    keys = ['name','phone','email'],
    sequence_by = "name",
    stored_as_scd_type = 2
)

@sdp.view
def dim_rider_view():
  df_rider = spark.readStream.table('taxi_ridingapp_catalog.silver.riding_app_obt')\
                                 .select('driver.driver_id','driver.name','driver.rating','driver.vehicle_make','driver.vehicle_model','driver.vehicle_plate')

  df_rider = df_rider.dropDuplicates(subset=['driver_id'])
  return df_rider

sdp.create_streaming_table("taxi_ridingapp_catalog.gold.dim_rider")

sdp.create_auto_cdc_flow(
    target = "taxi_ridingapp_catalog.gold.dim_rider",
    source = "dim_rider_view",
    keys = ['driver_id'],
    sequence_by = "driver_id",
    stored_as_scd_type = 2
)

@sdp.view
def dim_bookings_view():
  df_bookings = spark.readStream.table('taxi_ridingapp_catalog.silver.riding_app_obt')\
                     .select('booking_id','event_type','event_timestamp','ride_status','pickup_location',  'dropoff_location','car_type','distance_km','estimated_duration_min','price.amount','payment.method','capacity','promo_code')

  df_bookings = df_bookings.dropDuplicates(subset=['booking_id'])
  return df_bookings

sdp.create_streaming_table("taxi_ridingapp_catalog.gold.dim_bookings")

sdp.create_auto_cdc_flow(
    target = "taxi_ridingapp_catalog.gold.dim_bookings",
    source = "dim_bookings_view",
    keys = ['booking_id'],
    sequence_by = "booking_id",
    stored_as_scd_type = 2
)

@sdp.view
def dim_vehicle_view():
  df_vehicle = spark.readStream.table('taxi_ridingapp_catalog.silver.riding_app_obt')\
                     .select('vehicle_id','make','model','vehicle_category','fuel_type', 'vehicle_model_updated_at')

  df_vehicle = df_vehicle.dropDuplicates(subset=['vehicle_id'])
  return df_vehicle

sdp.create_streaming_table("taxi_ridingapp_catalog.gold.dim_vehicle")

sdp.create_auto_cdc_flow(
    target = "taxi_ridingapp_catalog.gold.dim_vehicle",
    source = "dim_vehicle_view",
    keys = ['vehicle_id'],
    sequence_by = "vehicle_model_updated_at",
    stored_as_scd_type = 2
)

## Fact Table #########################
@sdp.view
def fact_taxis_view():
  df_fact = spark.readStream.table('taxi_ridingapp_catalog.silver.riding_app_obt')\
                     .select('booking_id','driver.driver_id', 'customer.name','customer.phone','customer.email',
                             'vehicle_id','distance_km', 'price', 'base_fare', 'customer.avg_rating', 'driver.rating')\
                     .withColumnRenamed('customer.name','customer_name')\
                     .withColumnRenamed('customer.phone','customer_phone')\
                     .withColumnRenamed('customer.email','customer_email')

  return df_fact

sdp.create_streaming_table("taxi_ridingapp_catalog.gold.fact_taxis")

sdp.create_auto_cdc_flow(
    target = "taxi_ridingapp_catalog.gold.fact_taxis",
    source = "fact_taxis_view",
    keys = ['booking_id', 'driver_id', 'name','phone','email'],
    sequence_by = "booking_id",
    stored_as_scd_type = 1
)