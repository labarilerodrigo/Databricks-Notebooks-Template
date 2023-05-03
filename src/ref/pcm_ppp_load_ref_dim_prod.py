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
with ncm_cte as (
        select ncm.pcmdty_id, min(ncm_cd) ncm_cd, min(ipi_pct) ipi_pct, min(pis_cofins_typ_cd) pis_cofins_typ_cd, min(pis_cofins_pct) pis_cofins_pct 
        from {pcm_ppp_ref_ncm_pcmdty_brdg} ncm
        group by pcmdty_id
),
ncm_fcc_cte as (
    select 
    distinct pcmdty_id, ncm_cd
    from {pcm_ppp_ref_ncm_pcmdty_file_brdg}
),
pflg_cte as (
    SELECT PCMDTY_ID, max(pack_pnn_flag_cd) pack_pnn_flag_cd
    from {pcm_ppp_ref_pnn_flag}
    group by PCMDTY_ID
),
ncm_prod_attr_brdg as (
  select max(ncm_cd) ncm_cd, nec4_cd, nfc3_cd from {pcm_ppp_ref_ncm_prod_attr_brdg} group by nec4_cd, nfc3_cd
),
ref_ncm_nomed_exc as (
  select ncm_cd, is_ncm_exclu_ind, max(cst_cd) cst_cd from {pcm_ppp_ref_ncm_nomed_exc} group by ncm_cd, is_ncm_exclu_ind
)

    INSERT overwrite table {pcm_ppp_ref_dim_prod}
    select
      p.pcmdty_id pcmdty_id,
      p.prod_brand_nm as prod_desc,
      p.pack_code as pack_cd,
      p.prod_cd prod_cd,
      p.lab_cd as manuf_cd,
      p.atc4_cd as atc4_cd,
      p.atc3_cd as atc3_cd,
      p.atc2_cd as atc2_cd,
      p.atc1_cd as atc1_cd,
      p.nec4_cd as nec4_cd,
      p.nec3_cd  as nec3_cd,
      p.nec2_cd  as nec2_cd,
      p.nec1_cd  as nec1_cd,
      p.nfc1_cd as app1_cd,
      p.std_unit/100 std_factor,
      ea.PCK_MKT_SEG_CLASS_3 mseg3_cd,
      cast(if(ea.PCK_MEDICINE_IND='MEDICAMENTO',1,0) as boolean) as pck_is_medcn_ind,
      upper(ea.pck_gmrs_class_cd) as pck_gmrs_class_cd,
      if(ncm.ncm_cd is null,ncm_fcc.ncm_cd,ncm.ncm_cd) as ncm_cd,
      ncm.ipi_pct,
      ncm.pis_cofins_typ_cd,
      ncm.pis_cofins_pct,
      upper(pflg.pack_pnn_flag_cd) as pack_pnn_flag_cd,
      p.mkt_seg_cd as otc_seg_cd,
      coalesce(ncm_nomed.cst_cd, 0),
      cast(if(ncm_nomed.is_ncm_exclu_ind='NOMEDICAMENTO',1,0) as boolean) as is_ncm_exclu_ind
    from {vw_rds_product_dim} p
    left join {vw_rds_product_ext_attr} ea on ea.pcmdty_id = p.pcmdty_id
    left join ncm_cte ncm on ncm.pcmdty_id = p.pcmdty_id
    left join pflg_cte pflg on pflg.pcmdty_id = p.pcmdty_id
    left join ncm_prod_attr_brdg ncm_prod on p.nec4_cd = ncm_prod.nec4_cd and p.nfc3_cd = ncm_prod.nfc3_cd
    left join ref_ncm_nomed_exc ncm_nomed on  ncm_prod.ncm_cd = ncm_nomed.ncm_cd
    left join ncm_fcc_cte ncm_fcc on p.pcmdty_id = ncm_fcc.pcmdty_id
""").show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation
