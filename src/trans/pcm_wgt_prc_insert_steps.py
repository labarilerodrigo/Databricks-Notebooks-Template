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
spark.sql(f"delete from {pcm_ppp_wgt_calc} where period_cd between '{period_cd_from}' and '{period_cd_to}' and step_cd > 100")

# 11
print(f"inserting into {pcm_ppp_wgt_calc}")
spark.sql(f"""
with
    cte_states
    as
    (
      select state_abbrev state_cd
      from {pcm_ppp_ref_states}
    ),
    cte_chain
    as
    (
          select 'R' ri
      union all
          select 'I' ri
    ),
    cte_states_ri
    as
    (
      select state_cd, ri, concat(state_cd, ri) geo_lvl_cd
      from cte_states cross 
      join cte_chain
    )
insert into {pcm_ppp_wgt_calc}
select
    pr.pcmdty_id, 
    ri.geo_lvl_cd, 
    pr.ppp_prc_amt, 
    0 as wgt_units_qty, 
    pr.step_cd, 
    0 as sales_units_qty,
    pr.period_cd
from {pcm_ppp_prc} pr  
cross join cte_states_ri ri
where pr.step_cd > 100 and pr.period_cd between '{period_cd_from}' and '{period_cd_to}'
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------


