# Databricks notebook source
# MAGIC %md
# MAGIC #### Input

# COMMAND ----------

# MAGIC %run ../config/pcm_common

# COMMAND ----------

state_cd = ('AL')

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETL

# COMMAND ----------

## args

## steps

# 10
spark.sql(f"""
 with
      cte_med_st_1
      as
      (
        select
          TRANS.line_id,
          TRANS.dsupp_proc_id,
          TRANS.trans_dt,
          TRANS.seller_otlt_cd,
          TRANS.buyer_otlt_cd,
          TRANS.icms_typ_cd, --D15
          TRANS.pcmdty_id,
          TRANS.pur_prc_amt,
          TRANS.seller_st_cd,
          TRANS.buyer_st_cd,
          TRANS.is_buyer_chain_ind, -- R (RED) = 1 | Independiente = 0
          TRANS.seller_is_manuf_ind, --1 = Industria | 0 = Distribuidor  -D14
          TRANS.buyer_is_pharmy_ind,
          TRANS.period_cd,
          if(DPROD.pack_pnn_flag_cd = 'T', 0.0925, 0) pis_cofins_rt, -- D18 = D30
          MED.icms_int_pct / 100 intr_icms_rt, -- D19
          trans_is_intra_st_ind trans_is_intra_st_ind, -- D20 1 = interna | 0 = interestadual
          TRANS.pur_prc_amt - 
          (TRANS.pur_prc_amt * TRANS.prc_disc_pct / 100) fnal_purchase_price_amt, -- D25 - D27 = D28
          0 cst_cd,
          TRANS.dsupp_is_distr_ind dsupp_is_distr_ind,
          MED.pnn_cd pnn_cd,
          PR_M.prc_amt pcm_prc_amt, -- D38
          MED.mva_12_pct,
          MED.mva_7_pct,
          MED.mva_4_pct,
          MED.mva_orig_pct, -- D42
          DPROD.pck_is_medcn_ind,
          MED.red_mva_pct/100 red_mva_rt, -- D44
          coalesce(MED.pcm_red_pct,0) /100 pcm_red_rt			-- D39
        from {pcm_ppp_ref_dim_prod} dprod
          join {pcm_ppp_buys_trans_fltr_1} trans on DPROD.pcmdty_id = TRANS.pcmdty_id
          join {pcm_ppp_ref_tax_med_icms_all_states} med on DPROD.pack_pnn_flag_cd = MED.pnn_cd and MED.gmrs_cd = DPROD.pck_gmrs_class_cd and MED.st_cd = upper('{state_cd}')
          left join {pcm_ppp_ref_dim_ext_prc_m} PR_M on PR_M.pcmdty_id = DPROD.pcmdty_id and TRANS.period_cd = PR_M.period_cd
        where 
          dprod.pck_is_medcn_ind = 1
          and TRANS.buyer_st_cd = upper('{state_cd}')
          and TRANS.period_cd between {period_cd_from} and {period_cd_to}
          and PR_M.period_cd between {period_cd_from} and {period_cd_to}
      ),
      cte_med_st_2
      as
      (
        select
          cte_med_st_1.*,
          if(trans_is_intra_st_ind = 1, 
          intr_icms_rt,
          if(cst_cd in (0, 4, 5, 6, 7), 
            if(seller_st_cd in ('SP', 'SC', 'RS', 'RJ', 'PR', 'MG'), 0.07, 0.12), 
            0.04
          )
        ) trans_icms_rt, -- D21 = D33
          (fnal_purchase_price_amt * pis_cofins_rt) pis_cofins_cr_amt	-- D31
        from cte_med_st_1
      ),
      cte_med_st_3
      as
      (
        select
          cte_med_st_2.*,
          if(seller_is_manuf_ind = 1 and trans_is_intra_st_ind = 0 and pnn_cd ='N',
          case trans_icms_rt
            when 0.12 then 0.099
            when 0.07 then 0.0934
            when 0.04 then 0.0904 
            else 0 
          end,
          0
        ) icms_reductor_rt	-- D22 = D34
        from cte_med_st_2
      ),
      cte_med_st_4
      as
      (
        select
          cte_med_st_3.*,
          if( icms_typ_cd = 2 and trans_is_intra_st_ind = 1,
          0, 
          fnal_purchase_price_amt * (trans_icms_rt * (1 - icms_reductor_rt))
        ) icms_own_amt, -- D36 

          pcm_prc_amt - (pcm_prc_amt * pcm_red_rt) reduced_pmc, -- D40 

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
        )) / 100		
        mva_adj	-- D43					
        from cte_med_st_3
      ),
      cte_med_st_5
      as
      (
        select
          cte_med_st_4.*,
          fnal_purchase_price_amt * (1 + mva_adj)	* (1 - red_mva_rt) mva_calc		-- D45
        from cte_med_st_4
      ),
      cte_med_st_6
      as
      (
        select
          cte_med_st_5.*,
          if(pcm_prc_amt = 0, 
          mva_calc, 
          reduced_pmc
        ) calc_st			-- D47
        from cte_med_st_5
      ),
      cte_med_st_7
      as
      (
        select
          cte_med_st_6.*,
          if(icms_typ_cd = 2 and trans_is_intra_st_ind = 1,
          0, 
          calc_st * intr_icms_rt
        ) st_db			-- D49
        from cte_med_st_6
      ),
      cte_med_st_8
      as
      (
        select
          cte_med_st_7.*,
          if(icms_typ_cd = 2 and trans_is_intra_st_ind = 1, 
          0, 
          if(icms_own_amt > st_db, 
            0, 
            st_db - icms_own_amt
          )
        ) st_prc			-- D50
        from cte_med_st_7
      ),
      cte_med_st_9
      as
      (
        select
          cte_med_st_8.*,
          fnal_purchase_price_amt + st_prc - pis_cofins_cr_amt ppp_prc_amt			-- D51
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
    select
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
    from
      cte_med_st_9
"""
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------

 
