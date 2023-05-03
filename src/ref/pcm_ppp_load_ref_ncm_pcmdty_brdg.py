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
      INSERT overwrite table {pcm_ppp_ref_ncm_pcmdty_brdg}
      SELECT
          distinct attr.ncm_cd,
          prod.pcmdty_id,
          rt.ipi_pct,
          rt.pis_cofins_typ_cd,
          rt.pis_cofins_pct
      FROM {pcm_ppp_ref_ncm_prod_attr_brdg} attr
      left join {vw_rds_product_dim} prod on attr.nec4_cd = prod.nec4_cd and attr.nfc3_cd = prod.nfc3_cd
      left join {pcm_ppp_ref_tax_no_med_federal_tax_rt} rt on attr.ncm_cd = rt.ncm_cd
""").show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------


