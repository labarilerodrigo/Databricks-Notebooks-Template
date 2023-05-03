# Databricks notebook source
# MAGIC %md
# MAGIC #### Input

# COMMAND ----------

# MAGIC %run ../config/pcm_common

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETL

# COMMAND ----------

## args

## steps

# 10
df = spark.sql(f"""
select 
  ppp.period_cd as Periodo,
  ppp.pcmdty_id as FCC,
  rds.stdiz_pcmdty_nm as Des_Longa,
  if(ppp.step_cd in (1, 50, 51), 'REAL','STEP') as Instancia
from 
  {pcm_ppp_prc} ppp 
  join {vw_rds_product_dim} rds on ppp.pcmdty_id = rds.pcmdty_id
where 
  ppp.period_cd = '{period_cd_to}'
  and rds.atc4_cd  != 'Z98A2' 
""")

# 11
df_count = df.count()

# write df to pcm_ppp_leg_prices
print(f"Recording table '{pcm_ppp_leg_prices}' with {df_count} records")
df.write.mode("overwrite").saveAsTable(pcm_ppp_leg_prices)
print("OK")

# write df to azure blob storage
file_output = f"{config['pcmppp_output']}/pcm_ppp_leg_prices.csv"
print(f"Recording csv file to '{file_output}' with {df_count} records")
df.toPandas().to_csv(file_output, index=False)
print("OK")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation
