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
prc_amt_threshold = 0.05

## steps

# 10
spark.sql(f"""delete from {pcm_ppp_buys_trans_fltr_1} where period_cd between '{period_cd_from}' and  '{period_cd_to}'""")

# 11
spark.sql(f"""
with cte_trans_1 as
(
  select
    line_id,
    dsupp_proc_id,
    trans_dt,
    icms_typ_cd,
    t.pcmdty_id,
    if(t.pcmdty_qty_mult_fct = 0, 1, t.pcmdty_qty_mult_fct) as pcmdty_qty_mult_fct,
    if(sq.pcmdty_qty_mult_fct = 0, 1, sq.pcmdty_qty_mult_fct ) as seller_qty_mult_fct,
    pur_prc_amt,
    icms_tax_pct,
    prc_disc_pct,
    if((dsupp_is_assc_ind = 1), cnpj_180.Outlet_Code, if(w.ppp_dsupp_typ_cd = 'PHARMACY', whs_otlt.outlet_code, cnpj_070.outlet_code)) as buyer_otlt_cd,
    if((dsupp_is_assc_ind = 1), cnpj_070.outlet_code, if(w.ppp_dsupp_typ_cd = 'DISTRIBUTOR', whs_otlt.outlet_code, cnpj_070.outlet_code)) as seller_otlt_cd,
    dsupp_is_assc_ind,
    prod.pck_is_medcn_ind as pck_is_medcn_ind,
    w.ppp_dsupp_typ_cd as ppp_dsupp_typ_cd,
    cast(if(w.ppp_dsupp_typ_cd = 'PHARMACY' or (dsupp_is_assc_ind = 1), 0, 1) as boolean) as dsupp_is_distr_ind,
    period_cd
  from {pcm_ppp_buys_trans} t
    join {pcm_ppp_ref_dim_prod} prod on prod.pcmdty_id = t.pcmdty_id
    join {pcm_ppp_ref_dim_whs} w on w.wholesaler_code = t.dsupp_proc_id
    join {pcm_ppp_ref_dim_otlt} cnpj_070 on cnpj_070.cnpj_cd = t.orig_cnpj_fiscal_id
    left join {pcm_ppp_ref_dim_otlt} cnpj_180 on cnpj_180.cnpj_cd = t.dest_cnpj_fiscal_id
    join {pcm_ppp_ref_dim_otlt} whs_otlt on w.wholesaler_cgc = whs_otlt.cnpj_cd
    left join {ref_whs_seller_pcmdty_qty_fct} sq on sq.wholesaler_CGC = t.orig_cnpj_fiscal_id
        and t.pcmdty_id = sq.pcmdty_id
        and t.dsupp_is_assc_ind = 1
    --non-febrafar(associations)
    where 1 = 1
        and t.pcmdty_id > 0 and t.pcmdty_id < 999999
        and t.period_cd between '{period_cd_from}' and  '{period_cd_to}'
    --Remove APPS UnBridged products transactions.
), cte_new_pur_prc as
(
  select 
    --top 100
    buyer_otlt.is_otlt_chain_ind as is_buyer_chain_ind,
    if( buyer_otlt.ppp_fisc_actvty_class_cd != 'PHARMACY' AND w_cnpj_root.ppp_dsupp_typ_cd='PHARMACY' ,'PHARMACY', buyer_otlt.ppp_fisc_actvty_class_cd) as buyer_ppp_typ_cd,
    seller_otlt.ppp_fisc_actvty_class_cd as seller_ppp_typ_cd,
    cast( if(buyer_otlt.st_cd = seller_otlt.st_cd , 1, 0) as boolean) as trans_is_intra_st_ind,
    seller_otlt.st_cd as seller_st_cd,
    buyer_otlt.st_cd as buyer_st_cd,
    cast(  if(seller_otlt.ppp_fisc_actvty_class_cd = 'MANUFACTURER', 1, 0 ) as boolean) as seller_is_manuf_ind,
    cast(  if(buyer_otlt.ppp_fisc_actvty_class_cd = 'PHARMACY', 1, 0) as boolean) as buyer_is_pharmy_ind,
    --t.*,
    line_id, dsupp_proc_id, trans_dt, icms_typ_cd, pcmdty_id,
    cast (t.pur_prc_amt / if ( dsupp_is_distr_ind=0 and t.dsupp_is_assc_ind=0 and t.seller_qty_mult_fct is not null , t.seller_qty_mult_fct, t.pcmdty_qty_mult_fct)
      as decimal(18,4)) as pur_prc_amt,
    icms_tax_pct, prc_disc_pct, buyer_otlt_cd, seller_otlt_cd, dsupp_is_assc_ind, pck_is_medcn_ind, t.ppp_dsupp_typ_cd, dsupp_is_distr_ind, period_cd
  from cte_trans_1 t
  join {pcm_ppp_ref_dim_otlt} buyer_otlt on buyer_otlt.outlet_code = t.buyer_otlt_cd
  join {pcm_ppp_ref_dim_otlt} seller_otlt on seller_otlt.outlet_code = t.seller_otlt_cd
  left join {pcm_ppp_ref_dim_whs} w_cnpj_root on w_cnpj_root.wholesaler_CGC = buyer_otlt.cnpj_cd and w_cnpj_root.wholesaler_type !=2 
  --This removes duplicated CGC
  where 1=1
    and buyer_otlt.ppp_incl_calc_ind = 1
    and t.period_cd between '{period_cd_from}' and  '{period_cd_to}'
),cte_cnt_trans as(
    select pcmdty_id, period_cd, dsupp_proc_id, count(1) total_a
    from cte_new_pur_prc
    group by pcmdty_id, period_cd, dsupp_proc_id
),
cte_trans_0_05 as(
    select pcmdty_id, period_cd, dsupp_proc_id, count(1) total_b
    from cte_new_pur_prc
    where pur_prc_amt <= 0.05 
    group by pcmdty_id, period_cd, dsupp_proc_id
),
cte_total_trans as(
    select a.pcmdty_id, a.period_cd, a.dsupp_proc_id, round(cast(b.total_b * 100.00 / a.total_a as float), 2) total_trans
    from cte_cnt_trans a
    join cte_trans_0_05 b on a.dsupp_proc_id = b.dsupp_proc_id and a.pcmdty_id = b.pcmdty_id and a.period_cd = b.period_cd
),
cte_trans_0_6 as (
    select a.*, 
    if(b.total_trans < 60 or b.total_trans is null, 0, 1) rem_1
    from cte_new_pur_prc a
    left join cte_total_trans b on 
    a.dsupp_proc_id = b.dsupp_proc_id 
    and a.pcmdty_id = b.pcmdty_id 
    and a.period_cd = b.period_cd
),
cte_dupp as 
(
    select 
        line_id,
        count(1) cnt 
    from cte_trans_0_6
    group by line_id
    having count(*) > 1
),
new_cte_new_pur_prc as
(
    select 
        p.*,
        if(p.line_id = c.line_id and p.buyer_ppp_typ_cd = 'WHOLESALER', 1, 0) rem 
    from cte_trans_0_6 p
    left join cte_dupp c on p.line_id = c.line_id
    where p.rem_1 = 0
),
cte_medicine_list as --Grouping only medicine records to identify records under outlier part 1
(
  select a.is_buyer_chain_ind,
  a.buyer_ppp_typ_cd,
  a.seller_ppp_typ_cd,
  a.trans_is_intra_st_ind,
  a.seller_st_cd,
  a.buyer_st_cd,
  a.seller_is_manuf_ind,
  a.buyer_is_pharmy_ind,
  a.line_id,
  a.dsupp_proc_id,
  a.trans_dt,
  a.icms_typ_cd,
  a.pcmdty_id,
  a.pur_prc_amt,
  a.icms_tax_pct,
  a.prc_disc_pct,
  a.buyer_otlt_cd,
  a.seller_otlt_cd,
  a.dsupp_is_assc_ind,
  a.pck_is_medcn_ind,
  a.ppp_dsupp_typ_cd,
  a.dsupp_is_distr_ind,
  a.period_cd,
    coalesce(b.prc_amt, 0.00) as list_price,
    d.pck_gmrs_class_cd gmrs,
    coalesce(d.gmrs_max_pct, 0) gmrs_max_pct,
    coalesce(d.gmrs_min_pct, 0) gmrs_min_pct
  from new_cte_new_pur_prc a
    left join {pcm_ppp_ref_dim_ext_prc_l} b on a.pcmdty_id= b.pcmdty_id and b.period_cd = a.period_cd
    left join {pcm_ppp_ref_dim_prod} c on a.pcmdty_id= c.pcmdty_id
    left join {pcm_ppp_gmrs_min_max_pct} d on c.pck_gmrs_class_cd= d.pck_gmrs_class_cd
  where a.pck_is_medcn_ind = 1 and rem = 0
), cte_identify_med_outliers as
(
  select a.*,
    if(list_price > 0.05,
      if((pur_prc_amt - (pur_prc_amt * prc_disc_pct /100))> list_price, 1,
        if(abs(((pur_prc_amt - (pur_prc_amt * prc_disc_pct /100))-list_price) * 100/list_price) > gmrs_max_pct, 1,
          if(abs(((pur_prc_amt - (pur_prc_amt * prc_disc_pct /100))-list_price) * 100/list_price) < gmrs_min_pct, 1,
        0))),
      0) outlier_ind --1 is outlier
    --flag for medicine outliers
  from cte_medicine_list a
),
cte_excluding_med_outliers as
(
  select is_buyer_chain_ind, buyer_ppp_typ_cd, seller_ppp_typ_cd, trans_is_intra_st_ind, seller_st_cd, buyer_st_cd, seller_is_manuf_ind, buyer_is_pharmy_ind, line_id, dsupp_proc_id,
    trans_dt, icms_typ_cd, pcmdty_id, pur_prc_amt, icms_tax_pct, prc_disc_pct, buyer_otlt_cd, seller_otlt_cd, dsupp_is_assc_ind, pck_is_medcn_ind, ppp_dsupp_typ_cd,
    dsupp_is_distr_ind, period_cd
  from new_cte_new_pur_prc
  where pck_is_medcn_ind = 0 --Non-medicine records
  and rem = 0
  union
  select is_buyer_chain_ind, buyer_ppp_typ_cd, seller_ppp_typ_cd, trans_is_intra_st_ind, seller_st_cd, buyer_st_cd, seller_is_manuf_ind, buyer_is_pharmy_ind, line_id, dsupp_proc_id,
    trans_dt, icms_typ_cd, pcmdty_id, pur_prc_amt, icms_tax_pct, prc_disc_pct, buyer_otlt_cd, seller_otlt_cd, dsupp_is_assc_ind, pck_is_medcn_ind, ppp_dsupp_typ_cd,
    dsupp_is_distr_ind, period_cd
  from cte_identify_med_outliers
  where outlier_ind= 0 --Medicine records after excluding outliers
)
insert into {pcm_ppp_buys_trans_fltr_1}
select is_buyer_chain_ind, buyer_ppp_typ_cd, seller_ppp_typ_cd, trans_is_intra_st_ind, seller_st_cd, buyer_st_cd, seller_is_manuf_ind, buyer_is_pharmy_ind, line_id, dsupp_proc_id,
    trans_dt, icms_typ_cd, pcmdty_id, pur_prc_amt, icms_tax_pct, prc_disc_pct, buyer_otlt_cd, seller_otlt_cd, dsupp_is_assc_ind, pck_is_medcn_ind, ppp_dsupp_typ_cd, dsupp_is_distr_ind, period_cd
from cte_excluding_med_outliers
where pur_prc_amt >= {prc_amt_threshold}
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation
