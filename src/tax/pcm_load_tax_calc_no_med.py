# Databricks notebook source
# MAGIC %md
# MAGIC #### Input

# COMMAND ----------

# MAGIC %run ../config/pcm_common

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETL

# COMMAND ----------

spark.sql(
    f"""
with cte_nec_4 as (
  select 
    dp.pcmdty_id,
    tp.ncm_cd,
    tp.tipi_excl_cd 
  from {pcm_ppp_ref_dim_prod} dp
  join {pcm_ppp_ref_tax_no_med_tipi_excl} tp on tp.ncm_cd = dp.ncm_cd and tp.nec_lvl_cd = dp.nec4_cd
),
cte_nec_3 as (
  select 
    dp.pcmdty_id,
    tp.ncm_cd,
    tp.tipi_excl_cd 
  from {pcm_ppp_ref_dim_prod} dp
  join {pcm_ppp_ref_tax_no_med_tipi_excl} tp on tp.ncm_cd = dp.ncm_cd and tp.nec_lvl_cd = dp.nec3_cd
  where dp.pcmdty_id not in (
    select 
      pcmdty_id
    from cte_nec_4
    )
),
cte_nec_2 as (
  select 
    dp.pcmdty_id,
    tp.ncm_cd,
    tp.tipi_excl_cd 
  from {pcm_ppp_ref_dim_prod} dp
  join {pcm_ppp_ref_tax_no_med_tipi_excl} tp on tp.ncm_cd = dp.ncm_cd and tp.nec_lvl_cd = dp.nec2_cd
  where dp.pcmdty_id not in(
    select 
      pcmdty_id
    from cte_nec_4
    union
    select 
      pcmdty_id
    from cte_nec_3
    )
),
cte_nec_1 as (
  select 
    dp.pcmdty_id,
    tp.ncm_cd,
    tp.tipi_excl_cd 
  from {pcm_ppp_ref_dim_prod} dp
  join {pcm_ppp_ref_tax_no_med_tipi_excl} tp on tp.ncm_cd = dp.ncm_cd and tp.nec_lvl_cd = dp.nec1_cd
  where dp.pcmdty_id not in(
  select 
      pcmdty_id
    from cte_nec_4
    union
    select 
      pcmdty_id
    from cte_nec_3
    union
    select 
      pcmdty_id
    from cte_nec_2
    )
),
no_med_tipi as (
  select pcmdty_id, ncm_cd, tipi_excl_cd from cte_nec_4
  union
  select pcmdty_id, ncm_cd, tipi_excl_cd from cte_nec_3
  union
  select pcmdty_id, ncm_cd, tipi_excl_cd from cte_nec_2
  union
  select pcmdty_id, ncm_cd, tipi_excl_cd from cte_nec_1
),
cte_trans_1 as (
  select 
    buys.line_id,
    buys.dsupp_proc_id,
    buys.trans_dt,
    buys.seller_otlt_cd,
    buys.buyer_otlt_cd,
    buys.pcmdty_id,
    buys.is_buyer_chain_ind, -- R (RED) = 1 | Independiente = 0
    buys.buyer_is_pharmy_ind,
    buys.dsupp_is_distr_ind,
    buys.pck_is_medcn_ind,
    buys.period_cd,
    coalesce(dprod.ncm_cd , 'Carga Média') ncm_cd, -- D9
    coalesce(tipi.tipi_excl_cd, 'NAO') ex_na_tipi, -- D14
    buys.seller_is_manuf_ind seller_is_manuf_ind, -- D19	1 = Industria | 0 = Distribuidor
    buys.trans_is_intra_st_ind trans_is_intra_st_ind, -- D28	1 = interna | 0 = interestadual
    dprod.cst_cd, -- D16
    buys.buyer_st_cd buyer_st_cd,
    buys.seller_st_cd seller_st_cd,
    pr_m.prc_amt pcm_prc_amt,
    buys.pur_prc_amt, -- D38
    buys.pur_prc_amt - (BUYS.pur_prc_amt * BUYS.prc_disc_pct /100)
    final_purchase_price_amt, -- D41
    buys.icms_typ_cd flg_icms, -- D20
    dprod.nec4_cd,
    dprod.nec3_cd,
    dprod.nec2_cd,
    dprod.nec1_cd
  from {pcm_ppp_ref_dim_prod} dprod 
  join {pcm_ppp_buys_trans_fltr_1} buys on buys.pcmdty_id = DPROD.pcmdty_id
  left join no_med_tipi tipi on tipi.pcmdty_id = DPROD.pcmdty_id
  left join {pcm_ppp_ref_dim_ext_prc_m} PR_M on PR_M.pcmdty_id = DPROD.pcmdty_id and BUYS.period_cd = PR_M.period_cd and pr_m.period_cd is not null 
  where (BUYS.pck_is_medcn_ind = 0 /*and DPROD.is_ncm_exclu_ind = 0*/)
  and buys.period_cd between '{period_cd_from}' and '{period_cd_to}'
   
),
cte_nec4_cest as (
  select 
    dp.pcmdty_id,
    cest.NCM_cd, 
    nec_lvl_cd, 
    cest_cd
  from {pcm_ppp_ref_dim_prod} dp
  join {pcm_ppp_ref_tax_no_med_cest_excl} cest on cest.ncm_cd = dp.ncm_cd and cest.nec_lvl_cd = dp.nec4_cd
),
cte_nec3_cest as (
  select 
    dp.pcmdty_id,
    cest.NCM_cd,
    nec_lvl_cd, 
    cest_cd
  from {pcm_ppp_ref_dim_prod} dp
  join {pcm_ppp_ref_tax_no_med_cest_excl} cest on cest.ncm_cd = dp.ncm_cd and cest.nec_lvl_cd = dp.nec3_cd
  where dp.pcmdty_id not in(
    select 
      pcmdty_id
    from cte_nec4_cest
  )
),
cte_nec2_cest as (
  select 
    dp.pcmdty_id,
    cest.NCM_cd,
    nec_lvl_cd, 
    cest_cd
  from {pcm_ppp_ref_dim_prod} dp
  join {pcm_ppp_ref_tax_no_med_cest_excl} cest on cest.ncm_cd = dp.ncm_cd and cest.nec_lvl_cd = dp.nec2_cd
  where dp.pcmdty_id not in(
    select 
      pcmdty_id
    from cte_nec4_cest
    union 
    select 
      pcmdty_id
    from cte_nec3_cest
  )
),
cte_nec1_cest as (
  select 
    dp.pcmdty_id,
    cest.NCM_cd,
    nec_lvl_cd, 
    cest_cd
  from {pcm_ppp_ref_dim_prod} dp
  join {pcm_ppp_ref_tax_no_med_cest_excl} cest on cest.ncm_cd = dp.ncm_cd and cest.nec_lvl_cd = dp.nec1_cd
  where dp.pcmdty_id not in(
  select 
      pcmdty_id
    from cte_nec4_cest
    union 
    select 
      pcmdty_id
    from cte_nec3_cest
    union
    select 
      pcmdty_id
    from cte_nec2_cest
  )
),
cte_all_nec_cest as (
  select pcmdty_id, ncm_cd, nec_lvl_cd, cest_cd from cte_nec4_cest
  union
  select pcmdty_id, ncm_cd, nec_lvl_cd, cest_cd from cte_nec3_cest
  union
  select pcmdty_id, ncm_cd, nec_lvl_cd, cest_cd from cte_nec2_cest
  union
  select pcmdty_id, ncm_cd, nec_lvl_cd, cest_cd from cte_nec1_cest
),
non_med_icms_rt as (
  select 
      row_number() over (partition by state_cd, ncm_cd order by cest_cd asc)
      rowId, state_cd, ncm_cd, cest_cd, icms_typ_cd, icms_pct, mva_ori_op_intra_st_pct,
      mva_adj_op_inter_st_12_pct, mva_adj_op_inter_st_7_pct, mva_adj_op_inter_st_4_pct,
      intr_red_pct, pmpf_st_base, mva_reductor_pct
  from {pcm_ppp_ref_tax_non_med_icms_rt}
),
cte_no_med_cest as(
  select
      c1.*,
      cest.nec_lvl_cd,
      if(cest.cest_cd is not null,  cest.cest_cd, if(icms.NCM_cd = c1.ncm_cd, coalesce(icms.cest_cd, '-'), '-')) calc_cest
    from cte_trans_1 C1
    left join cte_all_nec_cest cest on c1.pcmdty_id = cest.pcmdty_id
    left join non_med_icms_rt ICMS on ICMS.state_cd =  c1.buyer_st_cd and ICMS.ncm_cd = c1.ncm_cd and rowId = 1
),
tax_rt as(
    select -- get rows from NCM_CD, sort by IPI_PCT
      row_number() over (partition by ncm_cd order by ipi_pct desc) rowId, ncm_cd, ipi_pct, pis_cofins_typ_cd, pis_cofins_pct
    from {pcm_ppp_ref_tax_no_med_federal_tax_rt}
    -- used below to get rows_id = 0
  )
,
cte_no_med_2 as
  (
    select
--     ICMS.state_cd , C1.buyer_st_cd , ICMS.ncm_cd, C1.ncm_cd , coalesce(ICMS.cest_cd, '-') ,C1.calc_cest,
      C1.*,
      if(cst_cd in (0, 4, 5, 6, 7), 1, 0) pck_is_natl_ind, -- D17	1 = Nacional | 0 Importado
      if(seller_is_manuf_ind = 0, 
      0,
      coalesce(tax.ipi_pct, 0)
    ) ipi_cd, -- D23
      TAX.pis_cofins_typ_cd, -- D24
      if(seller_is_manuf_ind = 0, 
      0, 
      tax.pis_cofins_pct
    ) tipi_pis, -- D25
      coalesce(ICMS.icms_pct, 0) icms_pct, -- D27 = D64
      ICMS.icms_typ_cd icms_typ_cd, -- D31 = D54
      if(ICMS.pmpf_st_base is NULL, 
      '-', 
      ICMS.pmpf_st_base
    )	pmpf_st_base, -- D32 = D55
      ICMS.mva_ori_op_intra_st_pct, -- 
      ICMS.mva_adj_op_inter_st_4_pct, -- 
      ICMS.mva_adj_op_inter_st_7_pct, -- 
      ICMS.mva_adj_op_inter_st_12_pct, -- 												    
      coalesce(ICMS.mva_reductor_pct, 0) mva_reductor_pct, -- D35 = D60
      intr_red_pct,
      coalesce(tax.ncm_cd , 'Carga Média') ncm_tax,
      if(ex_na_tipi <> 'NAO', c1.ncm_cd + ' - ' + ex_na_tipi, c1.ncm_cd ) test
    from cte_no_med_cest C1
      left join non_med_icms_rt ICMS on	ICMS.state_cd = C1.buyer_st_cd and ICMS.ncm_cd = C1.ncm_cd and coalesce(ICMS.cest_cd, '-') =  C1.calc_cest and rowId = 1
      left join (
            select
              ncm_cd, ipi_pct, pis_cofins_typ_cd, pis_cofins_pct
            from tax_rt
            where 
            rowId = 1
      ) tax on 
      tax.ncm_cd = if(coalesce(ex_na_tipi, 'NAO') <> 'NAO', concat(concat(c1.ncm_cd, ' - '), ex_na_tipi), c1.ncm_cd)
  ),
cte_no_med_icms_ops  as(
select
     c2.*,
     if(c2.trans_is_intra_st_ind = 1,									
     coalesce(c2.icms_pct, 0), 
     if(c2.pck_is_natl_ind = 0,														
       0.04,
       if(c2.seller_st_cd in ('SP', 'SC', 'RS', 'RJ', 'PR', 'MG') and c2.buyer_st_cd not in ('SP', 'SC', 'RS', 'RJ', 'PR', 'MG'), 
         0.07, 
         0.12 
       )
     )
   ) icms_ops
   -- D29 = D49
   from
     cte_no_med_2 c2
  ),
  cte_no_med_3
  as
  (
    select
      c2.*,
      coalesce(if(c2.ICMS_OPs = 0.12,										-- D34 = D59
      mva_adj_op_inter_st_12_pct,														
      if(c2.ICMS_OPs = 0.07, 
        mva_adj_op_inter_st_7_pct,	
        if(c2.ICMS_OPs = 0.04, 
          mva_adj_op_inter_st_4_pct,
          mva_ori_op_intra_st_pct
        )
      )
    ), 0) mva_adj,
      (FINAL_purchase_price_amt * ipi_cd) ipi_val, -- D44
      if(seller_is_manuf_ind = 1 and trans_is_intra_st_ind = 0 and pis_cofins_typ_cd = 'Monofásico',  --if_1
      if(tipi_pis = 0.125,											-- test if_1 if_2
        if(icms_ops = 0.12,										-- test if_2 if_3
          0.1049,													-- test if_3 if_4
          if(icms_ops = 0.07,									-- no if_3
            0.099,
            if(icms_ops = 0.04,
              0.0959,
              0
            )
          )
        ),
        if(tipi_pis = 0.12,											-- no if_2
          if(icms_ops = 0.12,
            0.099,
            if(icms_ops = 0.07,
            0.0934,
              if(icms_ops = 0.04,
                0.0904,
                0
              )
            )
          ),
          0
        )
      ), 
      -- no if_1
      if(trans_is_intra_st_ind = 1, 
        coalesce(intr_red_pct, 0),
        0
      )
    ) reductor_icms, -- D30 = D50
      if(pis_cofins_typ_cd = 'D/C', 
      tipi_pis, 
      0
    )
    pis_cofins_pharm
    -- D26 = D46
    from
      cte_no_med_icms_ops c2
  ),
  cte_no_med_4
  as
  (
    select
      c3.*,
      icms_ops * (1-reductor_icms) icms_redu, -- D51
      0 PMPF, -- D56 2021-09-06: Boffil specified we should leave the value = for later review
      if(icms_typ_cd = 'ST',
      (FINAL_purchase_price_amt + ipi_val) * (1 + mva_adj) * (1 - mva_reductor_pct),
      0
    ) calc_mva, -- D61
      (FINAL_purchase_price_amt * pis_cofins_pharm) credit_pis_co
    -- D47
    from
      cte_no_med_3 c3
  ),
  cte_nom_med_5
  as
  (
    select
      c4.*,
      if(flg_icms = 2 and trans_is_intra_st_ind = 1 and icms_typ_cd = 'ST', 
      0,
      FINAL_purchase_price_amt * icms_redu
    ) icms_prop, -- D52
      if(pmpf_st_base = 'PMPF' and PMPF > 0,
      PMPF,
      calc_mva
    ) calc_st
    -- D63
    from
      cte_no_med_4 c4
  ),
  cte_no_med_6
  as
  (
    select
      c5.*,
      if(icms_typ_cd = 'ST'and flg_icms = 2 and trans_is_intra_st_ind = 1,
      0,
      calc_st * icms_pct
    ) st_debi
    -- D65
    from
      cte_nom_med_5 c5
  ),
  cte_no_med_7
  as
  (
    select
      c6.*,
      if(icms_typ_cd = 'ST',
      if(flg_icms = 2 and trans_is_intra_st_ind = 1, 
        0, 
        if(icms_prop > st_debi, 
          0, 
          st_debi - icms_prop
        )
      ),
      0
    ) ST
    -- D66
    from
      cte_no_med_6 c6
  ),
  cte_no_med_8
  as
  (
    select
      period_cd,
      prc_amt,
      pcmdty_id
    from {pcm_ppp_ref_dim_ext_prc_d}
    where 
      period_cd between left(cast(date_format(dateadd(month, -1, to_date(concat('{period_cd_from}', '01'), 'yyyyMMdd')),'yyyyMMdd') as string), 6) and left(cast(date_format(dateadd(month, +1, to_date(concat('{period_cd_to}', '01'), 'yyyyMMdd')),'yyyyMMdd') as string), 6)
  ),
  cte_no_med_9
  as

  (
  select
        c7.*,
        if(icms_typ_cd = 'ST', 
        FINAL_purchase_price_amt + ipi_val + ST - credit_pis_co,
        if(coalesce((c8.prc_amt*(if(pis_cofins_typ_cd = 'D/C', pis_cofins_pharm, 0) + if(icms_typ_cd = 'D/C', icms_pct, 0))), 0) 
        <= (credit_pis_co + icms_prop), FINAL_purchase_price_amt + ipi_val, FINAL_purchase_price_amt + ipi_val - credit_pis_co - icms_prop + 
        coalesce((c8.prc_amt*(if(pis_cofins_typ_cd = 'D/C', pis_cofins_pharm, 0) + if(icms_typ_cd = 'D/C', icms_pct, 0))), 0))
        ) as ppp_prc_amt
    -- D67
    from
      cte_no_med_7 c7
    left join cte_no_med_8 c8 on 
        c7.pcmdty_id = c8.pcmdty_id and c7.period_cd = left(cast(date_format(dateadd(month, +1, to_date(concat(c8.period_cd, '01'), 'yyyyMMdd')),'yyyyMMdd') as string), 6)
  )
insert into {pcm_ppp_tax_calc}
  (
  buys_trans_line_id,
  dsupp_proc_id,
  trans_dt,
  seller_otlt_cd,
  buyer_otlt_cd,
  buyer_is_otlt_chain_ind,
  pcmdty_id,
  seller_is_manuf_ind,
  buyer_is_pharmy_ind,
  dsupp_is_distr_ind,
  ppp_prc_amt,
  period_cd,
  buyer_st_cd,
  pck_is_medcn_ind
  )
select
  upper(line_id),
  dsupp_proc_id,
  trans_dt,
  seller_otlt_cd,
  buyer_otlt_cd,
  is_buyer_chain_ind,
  pcmdty_id,
  seller_is_manuf_ind,
  buyer_is_pharmy_ind,
  dsupp_is_distr_ind,
  ppp_prc_amt,
  period_cd,
  buyer_st_cd,
  pck_is_medcn_ind
from
  cte_no_med_9
"""
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------


