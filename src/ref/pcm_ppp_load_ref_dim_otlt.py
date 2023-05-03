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
spark.sql(f"""
        insert overwrite table {pcm_ppp_ref_dim_otlt}
        select 
          cast(outlet_code-1000000 as int),
          outlet_type,
          cnpj cnpj_cd,
          left(cnpj, 8) as cnpj_root_cd,
          new_type_code as outlet_new_type_code,
          if(other4='I', 0, 1) as is_otlt_chain_ind,
          outlet_fiscal_activity_code fisc_actvty_cd,
          if(outlet_fiscal_activity_code in('FN', 'A2', 'T', 'FA', 'FW', 'B6', 'B7', 'J'), 1, 0) as ppp_incl_calc_ind,
          case
                when outlet_fiscal_activity_code in('FN', 'A2', 'FA', 'FW', 'J') then 'PHARMACY'
                when outlet_fiscal_activity_code in('T') then 'MANUFACTURER'
                when outlet_fiscal_activity_code in('B6', 'B7') then 'WHOLESALER'
                else 'OUTROS'
              end as ppp_fisc_actvty_class_cd,
          st_shrt_nm as st_cd,
          if(substring(cnpj, 1, 2) = 'BN' and outlet_type = 41, 1, 0) as is_otlt_delivery_ind
        from {vw_rds_oac_dim_outlet}
""").show()

# 11
spark.sql(f"""
    insert overwrite table {pcm_ppp_ref_states}
    with states as (
      select st_cd, st_nm, st_shrt_nm from {vw_rds_oac_dim_outlet}
      group by st_cd, st_nm, st_shrt_nm
      order by st_cd asc
    )
    select 
      st_cd as state_code, 
      st_nm as state_desc, 
      st_shrt_nm as state_abbrev
    from states
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------

assert spark.read.table(vw_rds_oac_dim_outlet).count() > 0, f"The table '{vw_rds_oac_dim_outlet}' is empty"
assert spark.read.table(pcm_ppp_ref_states).count() > 0, f"The total of '{pcm_ppp_ref_states}' should be 27"
print('OK')

# COMMAND ----------


