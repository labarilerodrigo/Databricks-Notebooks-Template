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
spark.sql(f"""delete from {pcm_ppp_buys_trans} where period_cd between '{period_cd_from}' and '{period_cd_to}'""")

# 11
spark.sql(f"""
insert into {pcm_ppp_buys_trans}
select
  lower(cast(line_id as string)) line_id,
  cast(T.dsupp_proc_id as int) dsupp_proc_id,
  cast(trans_dt as string) trans_dt,
  cast(store_cd as string) store_cd,
  cast(origin_cnpj_fiscal_id as string) orig_cnpj_fiscal_id,
  cast(icms_typ_cd as string) icms_typ_cd,
  cast(invc_nbr as string) invc_nbr,
  cast(origin_dsupp_prod_cd as string) orig_dsupp_prod_cd,
  cast(pcmdty_id as int) pcmdty_id,
  cast(units_qty as decimal(18,4)) units_qty,
  coalesce(w.pcmdty_qty_mult_fct, cast(t.pcmdty_qty_mult_fct as decimal(18,4))) pcmdty_qty_mult_fct,
  cast(purchase_prc_amt as decimal(18,4)) pur_prc_amt,
  cast(icms_tax_pct as decimal(18,4)) icms_tax_pct,
  cast(icms_tax_amt as decimal(18,4)) icms_tax_amt,
  cast(price_discount_pct as decimal(18,4)) prc_disc_pct,
  cast(discount_amt as decimal(18,4)) disc_amt,
  cast(destination_cnpj_fiscal_id as string) dest_cnpj_fiscal_id,
  cast(destination_dsupp_prod_cd as string) dest_dsupp_prod_cd,
  cast(destination_dsupp_prod_ean_cd as string) dest_dsupp_prod_ean_cd,
  if(SupplierType=2,1,0) dsupp_is_assc_ind,
  cast(proc_eff_ts as timestamp) as proc_eff_ts,
  uuid() as btch_id,
  cast(bridge_type as string) prod_brdg_stat_cd,
  origin_whlslr_id as orig_whlslr_id,
  t.period_dt as period_cd
from {pcm_ppp_apps_buys_trans} t
left join {pcm_ppp_ref_whs_qty_mult_fct} w on t.dsupp_proc_id = w.dsupp_proc_id
where period_dt between '{period_cd_from}' and '{period_cd_to}'
""").show()

# 12
spark.sql(f"""truncate table {ref_whs_seller_pcmdty_qty_fct}""")

# 13
spark.sql(f"""
with
  cte_rank_period
  as
  (
    select
      distinct
      --w.wholesaler_code,
      w.wholesaler_CGC,
      pcmdty_id,
      pcmdty_qty_mult_fct,
      period_cd,
      rank() over (partition by w.wholesaler_cgc, pcmdty_id order by period_cd desc, pcmdty_qty_mult_fct desc) as rank
    from {pcm_ppp_buys_trans} t
    join {pcm_ppp_ref_dim_whs} w on w.wholesaler_cgc = t.orig_cnpj_fiscal_id
    where 1=1
      and t.dsupp_is_assc_ind=0 --NON-Febrafar
      and w.ppp_dsupp_typ_cd='PHARMACY' --Only PHARMACY data supplier types
      and pcmdty_id > 0 and pcmdty_id < 999999 --Bridged-OK
  )
insert into {ref_whs_seller_pcmdty_qty_fct}
select
  --wholesaler_code,
  wholesaler_CGC,
  pcmdty_id,
  pcmdty_qty_mult_fct
from cte_rank_period
--get latest month Qty Factor
where rank=1
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------


