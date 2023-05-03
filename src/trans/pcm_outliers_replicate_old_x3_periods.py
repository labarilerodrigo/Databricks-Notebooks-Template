# Databricks notebook source
# MAGIC %md
# MAGIC #### Input

# COMMAND ----------

# MAGIC %run ../config/pcm_common

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETL

# COMMAND ----------

# 10
spark.sql(f"delete from {pcm_ppp_tax_calc_fltr_1} where period_cd between left(cast(date_format(dateadd(month, -2, to_date(concat('{period_cd_from}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6) and '{period_cd_from}'")

# 11
spark.sql(f"""
insert into {pcm_ppp_tax_calc_fltr_1}
select
    dsupp_proc_id, trans_dt, seller_otlt_cd, buyer_otlt_cd, buyer_is_otlt_chain_ind, pcmdty_id, ppp_prc_amt,
    '{period_cd_from}', buyer_st_cd, buyer_is_pharmy_ind, pck_is_medcn_ind
from {pcm_ppp_tax_calc_fltr_1}
where period_cd=left(cast(date_format(dateadd(month, 1, to_date(concat('{period_cd_from}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6)
""").show()

# 12
spark.sql(f"""
insert into {pcm_ppp_tax_calc_fltr_1}
select
    dsupp_proc_id, trans_dt, seller_otlt_cd, buyer_otlt_cd, buyer_is_otlt_chain_ind, pcmdty_id, ppp_prc_amt,
    left(cast(date_format(dateadd(month, -1, to_date(concat('{period_cd_from}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6), buyer_st_cd, buyer_is_pharmy_ind, pck_is_medcn_ind
from {pcm_ppp_tax_calc_fltr_1}
where period_cd=left(cast(date_format(dateadd(month, 1, to_date(concat('{period_cd_from}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6)
""").show()

# 13
spark.sql(f"""
insert into {pcm_ppp_tax_calc_fltr_1}
select
    dsupp_proc_id, trans_dt, seller_otlt_cd, buyer_otlt_cd, buyer_is_otlt_chain_ind, pcmdty_id, ppp_prc_amt,
    left(cast(date_format(dateadd(month, -2, to_date(concat('{period_cd_from}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6), buyer_st_cd, buyer_is_pharmy_ind, pck_is_medcn_ind
from {pcm_ppp_tax_calc_fltr_1}
where period_cd=left(cast(date_format(dateadd(month, 1, to_date(concat('{period_cd_from}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6)
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------


