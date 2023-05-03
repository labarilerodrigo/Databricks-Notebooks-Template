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
    INSERT overwrite table {pcm_ppp_ref_dim_whs}
    select
      wholesaler_code
      ,wholesaler_CGC
      ,wholesaler_type
      ,distribution_center
      ,flag1
      ,flag2
      ,flag3
      ,flag4
      ,flag5
      ,flag6
      ,flag7
      ,flag8
      ,if(if(flag1 = ' ', null, flag1) is not null or flag2='D' or flag5='F', 'PHARMACY', if(flag4='W', 'WHOLESALER', 'N/C')) ppp_dsupp_typ_cd
      ,cast(if(flag2='D', 1, 0) as boolean) is_whs_dlvry_ind
      ,oac_nm
    from {vw_rds_oac_dim_whs}
    where wholesaler_code not in (260,261,1261)
   """).show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation
