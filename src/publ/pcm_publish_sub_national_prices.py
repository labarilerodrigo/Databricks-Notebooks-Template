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
max_periods = 60

period_list = list()
for cnt in range(max_periods):
    period_list.append(spark.sql(f"""select left(cast(date_format(dateadd(month, - {cnt}, to_date(concat('{period_cd_to}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6) as period_cd""").first().period_cd)

period_cd_from = min(period_list)

## steps

# 10
print(f"inserting into {pcm_ppp_publ_prices_sub_natl}")
spark.sql(f""" 
with cte_price_C as
(
    --price_c is calculated price from sub-national table
    select pcmdty_id, geo_lvl_cd, step_cd, cast(round(ppp_prc_amt,2)*100 as int) as price, period_cd, 'C' as price_type
    from {pcm_ppp_wgt_calc}
    where period_cd between '{period_cd_from}' and '{period_cd_to}'
)
insert overwrite {pcm_ppp_publ_prices_sub_natl}
select pcmdty_id as fcc, geo_lvl_cd, price, step_cd, price_type, period_cd as period
from cte_price_C
""").show()

# 11
spark.sql(f"vacuum {pcm_ppp_publ_prices_sub_natl}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------


