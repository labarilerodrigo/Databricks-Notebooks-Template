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
query = f"""(select period_dt,
                    line_id,
                    dsupp_proc_id,
                    trans_dt,
                    store_cd,
                    origin_whlslr_id,
                    origin_cnpj_fiscal_id,
                    icms_typ_cd,
                    invc_nbr,
                    origin_dsupp_prod_cd,
                    pcmdty_id,
                    units_qty,
                    pcmdty_qty_mult_fct,
                    purchase_prc_amt,
                    icms_tax_pct,
                    icms_tax_amt,
                    price_discount_pct,
                    discount_amt,
                    destination_cnpj_fiscal_id,
                    destination_dsupp_prod_cd,
                    destination_dsupp_prod_ean_cd,
                    SupplierType,
                    bridge_type,
                    proc_eff_ts,
                    NomeProduto,
                    spm_product_Name,
                    NomeFornecedor,
                    src_sys_cd
              from dbo.pcm_ppp_buys_trans with (nolock) 
              where period_dt = '{period_cd_from}') q"""
df = spark.read.format('jdbc')\
               .option('url', 'jdbc:sqlserver://azwlaappsql01P.production.imsglobal.com;database=LAB_SysPPP')\
               .option('driver','com.microsoft.sqlserver.jdbc.SQLServerDriver')\
               .option('user', 'pcmpusr')\
               .option('password', 'Pcmuser1')\
               .option('dbtable', query)\
               .load()

df.createOrReplaceTempView('src_pcm_ppp_buys_trans')

# 11
spark.sql(f"delete from {pcm_ppp_apps_buys_trans} where period_dt = '{period_cd_from}'")

# 12
spark.sql(f"""
insert into {pcm_ppp_apps_buys_trans}
    (line_id,
    dsupp_proc_id,
    trans_dt,
    store_cd,
    origin_whlslr_id,
    origin_cnpj_fiscal_id,
    icms_typ_cd,
    invc_nbr,
    origin_dsupp_prod_cd,
    pcmdty_id,
    units_qty,
    pcmdty_qty_mult_fct,
    purchase_prc_amt,
    icms_tax_pct,
    icms_tax_amt,
    price_discount_pct,
    discount_amt,
    destination_cnpj_fiscal_id,
    destination_dsupp_prod_cd,
    destination_dsupp_prod_ean_cd,
    SupplierType,
    bridge_type,
    proc_eff_ts,
    NomeProduto,
    spm_product_Name,
    NomeFornecedor,
    src_sys_cd,
    period_dt)
select
    line_id,
    dsupp_proc_id,
    trans_dt,
    store_cd,
    origin_whlslr_id,
    origin_cnpj_fiscal_id,
    icms_typ_cd,
    invc_nbr,
    origin_dsupp_prod_cd,
    pcmdty_id,
    units_qty,
    pcmdty_qty_mult_fct,
    purchase_prc_amt,
    icms_tax_pct,
    icms_tax_amt,
    price_discount_pct,
    discount_amt,
    destination_cnpj_fiscal_id,
    destination_dsupp_prod_cd,
    destination_dsupp_prod_ean_cd,
    SupplierType,
    bridge_type,
    proc_eff_ts,
    NomeProduto,
    spm_product_Name,
    NomeFornecedor,
    src_sys_cd,
    period_dt
from src_pcm_ppp_buys_trans
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation
