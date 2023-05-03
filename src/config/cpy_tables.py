# Databricks notebook source
# MAGIC %md
# MAGIC #### Input

# COMMAND ----------

# MAGIC %run ../config/pcm_common

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETL

# COMMAND ----------

prefixes = ["devl_br9_pcm_ppp_pilot"] # prefix matching on schemas to copy over for layers
dest = "devl_br9_pcm_ppp"


def copy_template(source: str, dest: str) -> str:
  return f"CREATE TABLE IF NOT EXISTS {dest} SHALLOW CLONE {source};"

# `show tables` doesn't show all tables in all schemas, at least for me, hence the need to `show databases` first
schemas = [row["databaseName"] for row in spark.sql("show databases").collect()]
schemas_to_copy = list(filter(lambda schema: schema.lower().startswith(tuple(prefixes)), schemas))

print(schemas_to_copy)
for schema in schemas_to_copy:
  rows = spark.sql(f"show tables in {schema}").collect()
  mappings = [(f"{schema}.{row['tableName']}", f"{schema}.{row['tableName']}") for row in rows]
  copy_statements = [copy_template(source, dest) for (source, dest) in mappings]
  for s in copy_statements:
    print(s) # or `spark.sql(s)` to execute

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation
