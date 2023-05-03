# Databricks notebook source
# MAGIC %md
# MAGIC #### Input

# COMMAND ----------

# MAGIC %run ../config/pcm_common

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETL

# COMMAND ----------

spark.sql(f"delete from {pcm_ppp_tax_calc} where period_cd between '{period_cd_from}' and '{period_cd_to}'")
