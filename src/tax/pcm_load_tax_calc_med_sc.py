# Databricks notebook source
# MAGIC %md
# MAGIC #### Input

# COMMAND ----------

# MAGIC %run ../config/pcm_common

# COMMAND ----------

state_cd = ('SC')

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETL

# COMMAND ----------

## args

## steps

# 10
spark.sql(f"""
WITH
  cte_med_st_1
  AS
  (
    select
      trans.line_id,
      trans.dsupp_proc_id,
      trans.trans_dt,
      trans.seller_otlt_cd,
      trans.buyer_otlt_cd,
      trans.icms_typ_cd,
      trans.pcmdty_id,
      trans.pur_prc_amt,
      trans.icms_tax_pct,
      trans.prc_disc_pct,
      trans.seller_st_cd,
      trans.buyer_st_cd,
      trans.is_buyer_chain_ind, -- R (RED) = 1 | Independiente = 0
      trans.seller_is_manuf_ind, -- 1 = Industria | 0 = Distribuidor	
      trans.buyer_is_pharmy_ind,
      --@period_cd period_cd,
      TRANS.period_cd,
      med.gmrs_cd,
      if(dprod.pack_pnn_flag_cd='T',0.0925,0)							pis_cofins_rt,	--D18
      if(dprod.pack_pnn_flag_cd in ('P', 'N', 'T') 
      and med.gmrs_cd='G',0.12,0.17)									intr_icms_rt, --D19
      trans_is_intra_st_ind												trans_is_intra_st_ind, --D20 1 = interna | 0 = interestadual
      trans.pur_prc_amt - (trans.pur_prc_amt * trans.prc_disc_pct /100)	fnal_purchase_price_amt, --D28
      0																	pmpf_prc_amt,
      0																	cst_cd,
      trans.dsupp_is_distr_ind											dsupp_is_distr_ind,
      med.pnn_cd															pnn_cd,
      -- 2021-11-02: disabled
      --dprod.pmc_18_prc_amt												pcm_prc_amt,
      med.trava_pct / 100												trava_rt, --D42
      med.mva_orig_pct / 100											mva_origl_rt, --D46
      med.mva_12_pct,
      med.mva_7_pct,
      med.mva_4_pct,
      med.mva_orig_pct,
      dprod.pck_is_medcn_ind,
      coalesce(CPP.prc_amt, 0)												CPP_prc

    from {pcm_ppp_ref_dim_prod} dprod
      join {pcm_ppp_buys_trans_fltr_1} trans on dprod.pcmdty_id = trans.pcmdty_id
      join {pcm_ppp_ref_tax_med_icms_all_states} med on dprod.pack_pnn_flag_cd = med.pnn_cd and med.gmrs_cd = dprod.pck_gmrs_class_cd and med.st_cd = upper('{state_cd}')
      left join {pcm_ppp_ref_dim_ext_prc_d} CPP on trans.pcmdty_id= CPP.pcmdty_id 
        and left(cast(date_format(dateadd(month, -1, to_date(concat(trans.period_cd, '01'), 'yyyyMMdd')),'yyyyMMdd') as string), 6) = CPP.period_cd
    where
    dprod.pck_is_medcn_ind = 1
      and trans.buyer_st_cd = upper('{state_cd}')
      and TRANS.period_cd between {period_cd_from} and {period_cd_to}
  ),
  cte_med_st_2
  AS
  (

    SELECT med_tran.*,
      --if(med_tran.trans_is_intra_st_ind=1,
      --	med_tran.intr_icms_rt,
      --	if(cst_cd in (0, 4, 5, 6, 7),
      --		0.12,
      --		0.04
      --	)
      --)																									 			trans_icms_rt,		--D21
      if(med_tran.trans_is_intra_st_ind=1,
        0.12, --difference
        if(cst_cd in (0, 4, 5, 6, 7),
          0.12,
          0.04
        )
      )																												trans_icms_rt, --D21
      (fnal_purchase_price_amt * pis_cofins_rt)																		pis_cofins_cr_amt
    --D31
    --if(pcm_prc_amt = 0,
    --	pmpf_prc_amt,
    --	if(pmpf_prc_amt > pcm_prc_amt,
    --		pcm_prc_amt,
    --		pmpf_prc_amt
    --	)
    --)																												prod_ref_price_amt	--D40
    FROM cte_med_st_1 med_tran
  ),
  cte_med_st_3
  AS

  (
    SELECT cte_med_st_2.*,
      if(seller_is_manuf_ind = 1 and trans_is_intra_st_ind = 0 and pnn_cd ='N',
        case trans_icms_rt
          when 0.12 then 0.099
          when 0.07 then 0.0934
          when 0.04 then 0.0904
          else 0
        end, 0
      )																												icms_reductor_rt
    --D22
    --Trava_rt  * ( 1 - intr_icms_rt ) / ( 1 -  trans_icms_rt)														trava_adjusted_rt	--D43
    FROM cte_med_st_2
  ),
  cte_med_st_4
  AS

  (
    select cte_med_st_3.*,
      --trans_icms_rt * (1 - icms_reductor_rt)																			reduced_icms_rt,	--D34->D35
      --if( icms_typ_cd = 2 and trans_is_intra_st_ind = 1,
      --	0,
      --	(trans_icms_rt * (1 - icms_reductor_rt)) * fnal_purchase_price_amt
      --)																												icms_own_amt,		--D36
      (trans_icms_rt * (1 - icms_reductor_rt)) * fnal_purchase_price_amt												icms_own_amt
    --D36
    --(pmpf_prc_amt * trava_adjusted_rt)																				trava_amt,			--D44
    --(if(trans_icms_rt=0.12,
    --	mva_12_pct,
    --	if(trans_icms_rt=0.07,
    --		mva_7_pct,
    --		if(trans_icms_rt=0.04,
    --			mva_4_pct,
    --			mva_orig_pct
    --		)
    --	)
    --)) /100																											mva_adj				--D47

    from cte_med_st_3
  ),
  --cte_med_st_5
  --AS(
  --	select	cte_med_st_4.*,
  --	fnal_purchase_price_amt * (1 + mva_adj)																					mva_calc			--D48
  --	from cte_med_st_4
  --),
  --cte_med_st_6
  --AS(
  --	select	cte_med_st_5.*,
  --	if(pcm_prc_amt = 0,
  --		mva_calc,
  --		if(mva_calc > pcm_prc_amt,
  --			pcm_prc_amt,
  --			mva_calc
  --		)
  --	)																														lim_pmc_mva			--D49
  --	from cte_med_st_5
  --),
  --cte_med_st_7
  --AS(
  --	select	cte_med_st_6.*
  --	if(pmpf_prc_amt = 0 and pcm_prc_amt = 0,
  --		mva_calc,
  --		if(fnal_purchase_price_amt >= trava_amt,
  --			lim_pmc_mva,
  --			prod_ref_price_amt
  --		)
  --	)																														calc_st				--D51
  --	from cte_med_st_6
  --),
  --cte_med_st_8
  --AS(
  --	select	cte_med_st_7.*,
  --	if(icms_typ_cd = 2 and trans_is_intra_st_ind = 1,
  --		0,
  --		calc_st * intr_icms_rt
  --	)																														st_db				--D53
  --	from cte_med_st_7
  --),
  --cte_med_st_9
  --AS(
  --	select	cte_med_st_8.*,
  --	if(icms_typ_cd = 2 and trans_is_intra_st_ind = 1,
  --		0,
  --		if(icms_own_amt > st_db,
  --			0,
  --			st_db - icms_own_amt
  --		)
  --	)																														st_prc				--D54
  --	from cte_med_st_8
  --),
  cte_med_st_5
  --cte_med_st_10 -> cte_med_st_5
  AS

  (
    select cte_med_st_4.*, --cte_med_st_9 -> cte_med_st_6
      fnal_purchase_price_amt - icms_own_amt - pis_cofins_cr_amt + (0.17*CPP_prc)											ppp_prc_amt
    --D55->D28
    from cte_med_st_4
    --cte_med_st_9 -> cte_med_st_4
  )

insert into {pcm_ppp_tax_calc}
    (
    buys_trans_line_id
    ,dsupp_proc_id
    ,trans_dt
    ,seller_otlt_cd
    ,buyer_otlt_cd
    ,buyer_is_otlt_chain_ind
    ,pcmdty_id
    ,seller_is_manuf_ind
    ,buyer_is_pharmy_ind
    ,dsupp_is_distr_ind
    ,ppp_prc_amt
    ,period_cd
    ,buyer_st_cd
    ,pck_is_medcn_ind
    )
SELECT
      upper(line_id)
    ,dsupp_proc_id
    ,trans_dt
    ,seller_otlt_cd
    ,buyer_otlt_cd
    ,is_buyer_chain_ind
    ,pcmdty_id
    ,seller_is_manuf_ind
    ,buyer_is_pharmy_ind
    ,dsupp_is_distr_ind
    ,ppp_prc_amt
    ,period_cd
    ,buyer_st_cd
    ,pck_is_medcn_ind
FROM
  cte_med_st_5
"""
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------

 
