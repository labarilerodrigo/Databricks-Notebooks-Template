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

# create 2 dummy month prices, current+1 month and current+2 month, copy the price of current month
enddt1 = spark.sql(f"select left(cast(date_format(dateadd(month, 1, to_date(concat('{period_cd_to}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6) as period_cd").first().period_cd
enddt2 = spark.sql(f"select left(cast(date_format(dateadd(month, 2, to_date(concat('{period_cd_to}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6) as period_cd").first().period_cd

## steps
# 10
spark.sql(f"truncate table {pcm_ppp_publ_prices_wkly}")

# 11
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
        ),
    cte_add_dates as
        (
        --copy the price of current month for current+1 month and current+2 month periods
        select pcmdty_id, price, '{enddt1}' as period_cd, price_type
        from cte_all_prices a
        where period_cd = '{period_cd_to}'
        union all
        select pcmdty_id, price, '{enddt2}' as period_cd, price_type
        from cte_all_prices a
        where period_cd = '{period_cd_to}'
        ),
    cte_final as
        (
        select pcmdty_id, price, period_cd, price_type
        from cte_all_prices
        union all
        select pcmdty_id, price, period_cd, price_type
        from cte_add_dates
        )
insert into {pcm_ppp_publ_prices_wkly}
(fcc, price, price_type, period, `date`)
select a.pcmdty_id as fcc, a.price, a.price_type, a.period_cd as period, to_timestamp(concat(a.period_cd, '01'), 'yyyyMMdd') as `date`
from cte_final a
join {pcm_ppp_ref_dim_prod} prod on a.pcmdty_id = prod.pcmdty_id
where prod.atc4_cd  != 'Z98A2'
""").show()

# 12
print(f"Deleting on sql server table: {sqlserver_table}...")
import pymssql
conn = pymssql.connect(server=config['sqlserver_weekly_host'], database=config['sqlserver_weekly_db'], user=sqlserver_user, password=sqlserver_pass)
cursor = conn.cursor()
cursor.execute(f"delete from {sqlserver_table} where price_type in ('C')")
conn.commit()
print("OK")

# TODO: added a distinct here as pcm_ppp_publ_prices_wkly it's returning duplicates
df = spark.sql(f"""
select distinct fcc, price, price_type, `date`, 'M' as period
from {pcm_ppp_publ_prices_wkly}
where left(cast(date_format(`date`, 'yyyyMMdd') as string), 6) between '{period_cd_from}' and '{enddt2}'
""")

print(f"Inserting into sql server table: {sqlserver_table}...")
df.write.format('jdbc') \
  .mode('append') \
  .option('url', f"jdbc:sqlserver://{config['sqlserver_weekly_host']};database={config['sqlserver_weekly_db']}") \
  .option('dbtable', sqlserver_table) \
  .option('user', sqlserver_user) \
  .option('password', sqlserver_pass) \
  .save()
print("OK")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------


