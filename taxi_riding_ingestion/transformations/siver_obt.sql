CREATE OR REFRESH STREAMING TABLE taxi_ridingapp_catalog.silver.riding_app_obt 
AS 

SELECT
  
    
      b.*,
    
  
    
      ct.id as car_type_id, category, capacity, base_fare, per_km_rate, multiplier, ct.updated_at as car_type_updated_at,
    
  
      country_of_origin, network_type, cb.updated_at as card_brand_updated_at,
    
  
    
      lp.id as pickup_location_id, lp.city as pickup_city, lp.state as pickup_state, lp.region as pickup_region, lp.zip as pickup_zip, lp.updated_at as pickup_location_updated_at,
    
  
    
      ld.id as dropoff_location_id, ld.city as dropoff_city, ld.state as dropoff_state, ld.region as dropoff_region, ld.zip as dropoff_zip, ld.updated_at as dropoff_location_updated_at,
    
  
    
      vm.id as vehicle_id, make, model, vehicle_category, fuel_type, vm.updated_at as vehicle_model_updated_at
    
  
FROM
  
    
      STREAM (taxi_ridingapp_catalog.silver.bookings_stg)
      WATERMARK event_timestamp DELAY OF INTERVAL 3 MINUTES b
    
  
    
      LEFT JOIN taxi_ridingapp_catalog.bronze.car_type_mapper as ct ON b.car_type = ct.car_type
    
  
    
      LEFT JOIN taxi_ridingapp_catalog.bronze.card_brand_mapper as cb ON b.payment.card.brand = cb.card_brand
    
  
    
      LEFT JOIN taxi_ridingapp_catalog.bronze.location_mapper as lp ON b.pickup_location.latitude = lp.latitude AND b.pickup_location.longitude = lp.longitude
    
  
    
      LEFT JOIN taxi_ridingapp_catalog.bronze.location_mapper as ld ON b.pickup_location.latitude = ld.latitude AND b.pickup_location.longitude = ld.longitude
    
  
    
      LEFT JOIN taxi_ridingapp_catalog.bronze.vehicle_model_mapper as vm ON b.driver.vehicle_make = vm.make AND b.driver.vehicle_model = vm.model
    
  