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
sqlserver_user = "pcmpusr"
sqlserver_pass = "Pcmuser1"
sqlserver_table = "dbo.Prices"

max_periods = 60
period_cd_from = spark.sql(f"select left(cast(date_format(dateadd(month, -{max_periods}, to_date(concat('{period_cd_to}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6) as period_cd").first().period_cd

## steps

# 10
spark.sql(f"truncate table {pcm_ppp_publ_prices_mthly}")

spark.sql(f""" 
with cte_price_C as
        (
            --price_c is calculated price from national table
            select pcmdty_id, cast(round(ppp_prc_amt,2)*100 as int) as price, period_cd, 'C' as price_type
            from {pcm_ppp_prc}
            where period_cd between '{period_cd_from}' and '{period_cd_to}'
        ),
    cte_all_prices as
        (
            select pcmdty_id, price, period_cd, price_type
            from cte_price_C
        )
insert into {pcm_ppp_publ_prices_mthly} (fcc, price, price_type, period, date)
select a.pcmdty_id as fcc, a.price, if(prod.atc4_cd = 'Z98A2', 'J', a.price_type) as price_type, a.period_cd as period, to_timestamp(concat(a.period_cd, '01'), 'yyyyMMdd') as `date`
from cte_all_prices a
join {pcm_ppp_ref_dim_prod} prod on a.pcmdty_id = prod.pcmdty_id
""").show()

# 11
print(f"Deleting on sql server table: {sqlserver_table}...")
import pymssql
conn = pymssql.connect(server=config['sqlserver_monthly_host'], database=config['sqlserver_monthly_db'], user=sqlserver_user, password=sqlserver_pass)
cursor = conn.cursor()
cursor.execute(f"delete from {sqlserver_table} where price_type in ('C','J')")
conn.commit()
print("OK")

df = spark.sql(f"""
select distinct fcc, price, price_type, `date`, 'M' as period
from {pcm_ppp_publ_prices_mthly}
where left(cast(date_format(`date`, 'yyyyMMdd') as string), 6) between '{period_cd_from}' and '{period_cd_to}'
""")

df = df.repartition(20).cache()

print(f"Inserting into sql server table: {sqlserver_table}...")
df.write.format('jdbc') \
  .mode('append') \
  .option('url', f"jdbc:sqlserver://{config['sqlserver_monthly_host']};database={config['sqlserver_monthly_db']}") \
  .option('dbtable', sqlserver_table) \
  .option('user', sqlserver_user) \
  .option('password', sqlserver_pass) \
  .option('batchsize', 1000000) \
  .option('queryTimeout', 36000) \
  .save()
print("OK")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------


