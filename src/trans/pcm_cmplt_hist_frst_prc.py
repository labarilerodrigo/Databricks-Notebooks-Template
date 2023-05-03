# Databricks notebook source
# MAGIC %md
# MAGIC #### Input

# COMMAND ----------

# MAGIC %run ../config/pcm_common

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETL

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame

## args
min_period_cd = spark.sql(f"select min(period_cd) as period_cd from {pcm_ppp_prc}").first().period_cd
max_periods = spark.sql(f"select datediff(month, to_date(concat('{min_period_cd}', '01'), 'yyyyMMdd'), to_date(concat('{period_cd_from}', '01'), 'yyyyMMdd')) as period_cd").first().period_cd

## steps
# 10
cnt = 0
df_list = list()
while cnt < 60:
    df_list.append(spark.sql(f"""select left(cast(date_format(dateadd(month, - {cnt}, to_date(concat('{period_cd_from}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6) as period_cd"""))
    cnt += 1
df_reduced = reduce(DataFrame.unionAll, df_list)
df_reduced.createOrReplaceTempView("temp_periods")

# 11
spark.sql("drop view if exists prc_to_copy")
spark.sql(f"""
cache table prc_to_copy
with cte_pcmdty_id_step1 as (
    select distinct 
        p.pcmdty_id
    FROM {pcm_ppp_prc} p
    where p.step_cd = 1
    and p.period_cd = '{period_cd_from}'
  ), cte_price_with_gap
  as(
      select 
            pcmdty_id,
            min(period_cd) period_cd
      from {pcm_ppp_prc}
      where pcmdty_id in (select pcmdty_id from cte_pcmdty_id_step1)
      group by pcmdty_id
      having count(1) = 1
  ),
  cte_max_prices as (
    select 
        p.period_cd,
        p.pcmdty_id,
        p.ppp_prc_amt
        from {pcm_ppp_prc} p
    join cte_price_with_gap c on p.period_cd = c.period_cd and p.pcmdty_id = c.pcmdty_id
  ), 
  cte_price_replicated as(
    select
        t.period_cd,
        p.pcmdty_id,
        p.ppp_prc_amt
    from cte_max_prices p
    cross join temp_periods t

  )
  select --c.period_cd, c.ppp_prc_amt, c.pcmdty_id 
  c.*, p.period_cd period_price ,p.ppp_prc_amt ppp_price
  from cte_price_replicated c
  left join {pcm_ppp_prc} p on c.period_cd = p.period_cd and c.pcmdty_id = p.pcmdty_id
  where 1 = 1 
  and p.period_cd is null
  order by 1 desc
""")

# 12
spark.sql(f"""
with pcmdty_id_valid as(
    select distinct pcmdty_id 
    from prc_to_copy
), geo_lv as(
    select distinct  geo_lvl_cd
    from {pcm_ppp_wgt_calc}
    where pcmdty_id in (select pcmdty_id from pcmdty_id_valid)
)
-- Sub-national insert
insert into {pcm_ppp_wgt_calc}
(pcmdty_id, geo_lvl_cd, ppp_prc_amt, wgt_units_qty, period_cd, step_cd, sales_units_qty)
select pcmdty_id, geo_lvl_cd, ppp_prc_amt, 0 as wgt_units_qty, period_cd, 53 as step_cd, 0 as sales_units_qty
from prc_to_copy
cross join geo_lv
""").show()

# 13
spark.sql(f"""
-- National insert
insert into {pcm_ppp_prc}
(pcmdty_id, ppp_prc_amt, period_cd, step_cd)
select pcmdty_id, ppp_prc_amt, period_cd, 53 as step_cd
from prc_to_copy
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------


