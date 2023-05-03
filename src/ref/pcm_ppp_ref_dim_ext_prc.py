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
period_cd_from = '20190101'
## steps

# 10
spark.sql(f"""
          insert overwrite table {pcm_ppp_ref_dim_ext_prc_d}
          select
             cast(price/100.00 as decimal(18, 4)) as prc_amt,
             fcc as pcmdty_id,
             left(price_date, 6) as period_cd
          from {lgy_prices}
          where price_type='D' and price_date >= {period_cd_from}
""").show()


spark.sql(f"""
          insert overwrite table {pcm_ppp_ref_dim_ext_prc_m}
              select
                 cast(price/100.00 as decimal(18, 4)) as prc_amt,
                 fcc as pcmdty_id,
                 left(price_date, 6) as period_cd
              from {lgy_prices}
              where price_type='M' and price_date >= {period_cd_from}
""").show()


spark.sql(f"""
          insert overwrite table {pcm_ppp_ref_dim_ext_prc_l}
              select
                  cast(price/100.00 as decimal(18, 4)) as prc_amt,
                  fcc as pcmdty_id,
                  left(price_date, 6) as period_cd
              from {lgy_prices}
              where price_type='L' and price_date >= {period_cd_from}
""").show()



# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------


