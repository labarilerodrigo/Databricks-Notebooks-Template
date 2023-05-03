# Databricks notebook source
# MAGIC %md
# MAGIC #### Input

# COMMAND ----------

# MAGIC %run ../config/pcm_common

# COMMAND ----------

state_cd = ('MA')

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
          trans.icms_typ_cd, --D15
          trans.pcmdty_id,
          trans.pur_prc_amt,
          trans.prc_disc_pct,
          trans.seller_st_cd,
          trans.buyer_st_cd,
          trans.is_buyer_chain_ind, -- R (RED) = 1 | Independiente = 0
          trans.seller_is_manuf_ind, --1 = Industria | 0 = Distribuidor  -D14
          trans.buyer_is_pharmy_ind,
          TRANS.period_cd,
          med.gmrs_cd,
          if(dprod.pack_pnn_flag_cd='T',0.0925,0)							pis_cofins_rt, --D18 = D30
          med.icms_int_pct / 100												intr_icms_rt, --D19 = D47 -- revisar con juli
          trans_is_intra_st_ind												trans_is_intra_st_ind, --D20 1 = interna | 0 = interestadual
          trans.pur_prc_amt - (trans.pur_prc_amt * trans.prc_disc_pct /100)	fnal_purchase_price_amt, --D25-D27 = D28
          0																	pmpf_prc_amt,
          0																	cst_cd,
          trans.dsupp_is_distr_ind											dsupp_is_distr_ind,
          med.pnn_cd															pnn_cd,
          PR_M.prc_amt pcm_prc_amt, -- D38
          med.mva_12_pct,
          med.mva_7_pct,
          med.mva_4_pct,
          med.mva_orig_pct, --D42
          dprod.pck_is_medcn_ind,
          coalesce(med.pcm_red_pct,0) /100										pcm_red_rt
        --D39
        from {pcm_ppp_ref_dim_prod} dprod
          join {pcm_ppp_buys_trans_fltr_1} trans on dprod.pcmdty_id = trans.pcmdty_id
          join {pcm_ppp_ref_tax_med_icms_all_states} med on dprod.pack_pnn_flag_cd = med.pnn_cd and med.gmrs_cd = dprod.pck_gmrs_class_cd and med.st_cd = upper('{state_cd}')
          left join {pcm_ppp_ref_dim_ext_prc_m} PR_M on PR_M.pcmdty_id = DPROD.pcmdty_id and TRANS.period_cd = PR_M.period_cd
        Where 
          dprod.pck_is_medcn_ind = 1
          AND trans.buyer_st_cd = upper('{state_cd}')
          AND TRANS.period_cd between {period_cd_from} and {period_cd_to}
          and PR_M.period_cd between {period_cd_from} and {period_cd_to}
      ),
      cte_med_st_2
      AS
      (

        SELECT med_tran.*,
          if(med_tran.trans_is_intra_st_ind = 1, 
            med_tran.intr_icms_rt, 
            if(cst_cd in (0, 4, 5, 6, 7), 
              if(seller_st_cd in ('SP', 'SC', 'RS', 'RJ', 'PR', 'MG'),
                0.07,
                0.12
              ), 
              0.04
            )
          )																												trans_icms_rt, --D21 = D33 - revisar con juli -> Ok hasta acá
          (fnal_purchase_price_amt * pis_cofins_rt)																		pis_cofins_cr_amt, --D31
          if(pcm_prc_amt = 0, 
            pmpf_prc_amt, 
            if(pmpf_prc_amt > pcm_prc_amt, 
              pcm_prc_amt, 
              pmpf_prc_amt
            )
          )																												prod_ref_price_amt
        FROM cte_med_st_1 med_tran
      ),
      cte_med_st_3
      AS

      (
        SELECT cte_med_st_2.*,
          if(seller_is_manuf_ind=1 and trans_is_intra_st_ind = 0 and pnn_cd ='N',
            case trans_icms_rt when 0.12 
                then 0.099 when 0.07 
                then 0.0934 when 0.04 
                then 0.0904 
                else 0 
            end,
            0
          )																												icms_reductor_rt
        --D22 = D34
        FROM cte_med_st_2
      ),
      cte_med_st_4
      AS

      (
        select cte_med_st_3.*,
          trans_icms_rt * (1 - icms_reductor_rt)																			reduced_icms_rt, --D35 
          if( icms_typ_cd = 2 and trans_is_intra_st_ind = 1,
            0, 
            fnal_purchase_price_amt * (trans_icms_rt * (1 - icms_reductor_rt))
          )																												icms_own_amt, --D36 

          pcm_prc_amt - (pcm_prc_amt * pcm_red_rt)																		reduced_pmc, --D40 

          (if(intr_icms_rt = trans_icms_rt,
            mva_orig_pct,
            if(trans_icms_rt=0.12,
              mva_12_pct,
              if(trans_icms_rt=0.07,
                mva_7_pct, 
                if(trans_icms_rt=0.04, 
                  mva_4_pct, 
                  0
                )
              )
            )
          )) / 100																										mva_adj
        --D43	
        from cte_med_st_3
      ),
      cte_med_st_5
      AS

      (
        select cte_med_st_4.*,
          fnal_purchase_price_amt * (1 + mva_adj)																					mva_calc
        --D44 
        from cte_med_st_4
      ),
      cte_med_st_6
      AS

      (
        select cte_med_st_5.*,
          if(pcm_prc_amt = 0, 
        mva_calc, 
        reduced_pmc
      )																														calc_st
        --D46
        from cte_med_st_5
      ),
      cte_med_st_7
      AS

      (
        select cte_med_st_6.*,
          if(icms_typ_cd = 2 and trans_is_intra_st_ind = 1,
        0, 
        --calc_st * intr_icms_rt/100 21-09-2021
        calc_st * intr_icms_rt
      )																														st_db
        --D48
        from cte_med_st_6
      ),
      cte_med_st_8
      AS

      (
        select cte_med_st_7.*,
          if(icms_typ_cd = 2 and trans_is_intra_st_ind = 1, 
        0, 
        if(icms_own_amt > st_db, 
          0, 
          st_db - icms_own_amt
        )
      )																														st_prc
        --D49
        from cte_med_st_7
      ),
      cte_med_st_9
      AS

      (
        select cte_med_st_8.*,
          fnal_purchase_price_amt + st_prc - pis_cofins_cr_amt																	ppp_prc_amt
        --D50
        from cte_med_st_8
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
      cte_med_st_9
"""
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------

 
