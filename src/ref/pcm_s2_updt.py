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
spark.sql("drop view if exists pcmdty_chrono")
spark.sql(f"""
cache table pcmdty_chrono
with chrono as (
    select c.pcmdty_id_old, c.pcmdty_id_new 
    from {vw_rds_pcmdty_id_merge} c
    where pcmdty_id_old not in (select distinct pcmdty_id from {pcm_ppp_ref_dim_prod})
    and c.pcmdty_id_old <> c.pcmdty_id_new
), pcmdty_id_valid as(
select distinct
    pcmdty_id
    from {pcm_ppp_prc}
    where step_cd = 1
    and period_cd = '{period_cd_from}'
), pcmdty_cnt as(
select
    pcmdty_id, count(1) cnt
    from {pcm_ppp_prc}
    where pcmdty_id in (select pcmdty_id from pcmdty_id_valid)
    group by pcmdty_id
    having count(1) = 1
)
select *
from chrono c
join pcmdty_cnt p on c.pcmdty_id_new = p.pcmdty_id
""")

# 11
spark.sql(f"""
merge into {pcm_ppp_prc} p
using pcmdty_chrono c
on (p.pcmdty_id = c.pcmdty_id_old)
when matched then
update set p.pcmdty_id = c.pcmdty_id_new
""").show()

# 12
spark.sql(f"""
merge into {pcm_ppp_wgt_calc} p 
using pcmdty_chrono c
on (p.pcmdty_id = c.pcmdty_id_old)
when matched then
update set p.pcmdty_id = c.pcmdty_id_new
""").show()




# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------


