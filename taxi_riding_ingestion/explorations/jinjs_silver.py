# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import jinja2

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

jinja_config = [
    {
        "table": "taxi_ridingapp_catalog.silver.bookings_stg as b",
        "columns": "b.*"
    },
    {
        "table": "taxi_ridingapp_catalog.bronze.car_type_mapper as ct",
        "columns": ["category", "capacity", "base_fare", "per_km_rate", "multiplier", "ct.updated_at as car_type_updated_at"],
        "on": "b.car_type = ct.car_type"
    },
    {
        "table": "taxi_ridingapp_catalog.bronze.card_brand_mapper as cb",
        "columns": ["country_of_origin", "network_type", "cb.updated_at as card_brand_updated_at"],
        "on": "b.payment.card.brand = cb.card_brand"
    },
    {
        "table": "taxi_ridingapp_catalog.bronze.location_mapper as lp",
        "columns": ["lp.city as pickup_city", "lp.state as pickup_state", "lp.region as pickup_region", "lp.zip as pickup_zip", "lp.updated_at as pickup_location_updated_at"],
        "on": "b.pickup_location.latitude = lp.latitude AND b.pickup_location.longitude = lp.longitude"
    },
    {
        "table": "taxi_ridingapp_catalog.bronze.location_mapper as ld",
        "columns": ["ld.city as dropoff_city" , "ld.state as dropoff_state", "ld.region as dropoff_region", "ld.zip as dropoff_zip", "ld.updated_at as dropoff_location_updated_at"],
        "on": "b.pickup_location.latitude = ld.latitude AND b.pickup_location.longitude = ld.longitude"
    },
    {
        "table": "taxi_ridingapp_catalog.bronze.vehicle_model_mapper as vm",
        "columns": ["make", "model", "vehicle_category", "fuel_type", "vm.updated_at as vehicle_model_updated_at"],
        "on": "b.driver.vehicle_make = vm.make AND b.driver.vehicle_model = vm.model"
    }
]

jinja_sql_query = """
SELECT
  {% for item in config %}
    {% if item.columns is string %}
      {{ item.columns }}{% if not loop.last %},{% endif %}
    {% else %}
      {{ item.columns | join(', ') }}{% if not loop.last %},{% endif %}
    {% endif %}
  {% endfor %}
FROM
  {% for item in config %}
    {% if loop.first %}
      {{ item.table }}
    {% else %}
      LEFT JOIN {{ item.table }} ON {{ item.on }}
    {% endif %}
  {% endfor %}
"""

template = jinja2.Template(jinja_sql_query)
rendered_sql = template.render(config=jinja_config)
print(rendered_sql)

# COMMAND ----------

obt_df = spark.sql(rendered_sql)
display(obt_df)