# Databricks notebook source
# MAGIC %md
# MAGIC #### Input

# COMMAND ----------

# MAGIC %run ../config/pcm_common

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETL

# COMMAND ----------

dbutils.widgets.text('filename', f'pcm_ppp_ref_whs_smooth')
filename = dbutils.widgets.get('filename')

# COMMAND ----------

import os
from datetime import datetime

## args
input_filepath = f"{config['pcmppp_input']}/{filename}.csv"
processed_filepath = f"{config['pcmppp_processed']}/{filename}_{datetime.now().strftime('%Y%m%d')}.csv"
file_path = f"/dbfs{input_filepath}"

isFile = os.path.isfile(file_path)

if(isFile):
    ## steps
    # 10
    print(f"Loading dataframe from file: '{input_filepath}'")
    df = spark.read \
              .format("csv") \
              .option("header", "true") \
              .option("inferSchema", "true") \
              .option("mode", "FAILFAST") \
              .load(input_filepath)
    print(f"Loaded dataframe from file: '{input_filepath}' with {df.count()} records")

    df.write.mode("overwrite").saveAsTable(pcm_ppp_ref_whs_smooth)

    spark.sql(f"""
    with whs_smooth_no_tr as (
        select
            s.*
        from {pcm_ppp_ref_whs_smooth} s
        left join {pcm_ppp_apps_buys_trans} t on t.period_dt = {period_cd_from} and t.dsupp_proc_id = s.dsupp_proc_id
        where t.dsupp_proc_id is null
    )
    insert into {pcm_ppp_apps_buys_trans}
    select
             t.line_id
            ,t.dsupp_proc_id
            ,t.trans_dt
            ,t.store_cd
            ,t.origin_whlslr_id
            ,t.origin_cnpj_fiscal_id
            ,t.icms_typ_cd
            ,t.invc_nbr
            ,t.origin_dsupp_prod_cd
            ,t.pcmdty_id
            ,t.units_qty
            ,t.pcmdty_qty_mult_fct
            ,t.purchase_prc_amt
            ,t.icms_tax_pct
            ,t.icms_tax_amt
            ,t.price_discount_pct
            ,t.discount_amt
            ,t.destination_cnpj_fiscal_id
            ,t.destination_dsupp_prod_cd
            ,t.destination_dsupp_prod_ean_cd
            ,t.SupplierType
            ,t.bridge_type
            ,t.proc_eff_ts
            ,t.NomeProduto
            ,t.spm_product_Name
            ,t.NomeFornecedor
            ,'IMPUTE' as src_sys_cd
            ,{period_cd_from} as period_dt
    from {pcm_ppp_apps_buys_trans} t
    inner join whs_smooth_no_tr w on t.dsupp_proc_id = w.dsupp_proc_id
    where t.period_dt = left(cast(date_format(dateadd(month, -1, to_date(concat('{period_cd_from}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6)
    """).show()

    # 11
    print(f"Moving processed file: '{input_filepath}' to '{processed_filepath}'")
    if dbutils.fs.mv(input_filepath, processed_filepath):
        print("OK")
else:
    print(f"Canceled: The file does not exist {file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation
