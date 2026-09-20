# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC select * from taxi_ridingapp_catalog.gold.fact_taxis
# MAGIC --limit 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Total rides by each Driver

# COMMAND ----------

# MAGIC %sql
# MAGIC select driver_id, count(*) from taxi_ridingapp_catalog.gold.fact_taxis
# MAGIC group by driver_id;