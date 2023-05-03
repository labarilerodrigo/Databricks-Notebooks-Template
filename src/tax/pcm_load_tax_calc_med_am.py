# Databricks notebook source
# MAGIC %md
# MAGIC #### Input

# COMMAND ----------

# MAGIC %run ../config/pcm_common

# COMMAND ----------

state_cd = ('AM')

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
          TRANS.line_id,
          TRANS.dsupp_proc_id,
          TRANS.trans_dt,
          TRANS.seller_otlt_cd,
          TRANS.buyer_otlt_cd,
          TRANS.icms_typ_cd, --D15
          TRANS.pcmdty_id,
          TRANS.pur_prc_amt,
          TRANS.icms_tax_pct,
          TRANS.prc_disc_pct,
          TRANS.seller_st_cd,
          TRANS.buyer_st_cd,
          TRANS.is_buyer_chain_ind, -- R (RED) = 1 | Independiente = 0
          TRANS.seller_is_manuf_ind, -- 1 = Industria | 0 = Distribuidor	
          TRANS.buyer_is_pharmy_ind,
          TRANS.period_cd,
          med.gmrs_cd,
          if(dprod.pack_pnn_flag_cd='T',0.0925,0)							pis_cofins_rt, --D18 = D30 PIS/COFINS | Ok AM
          med.icms_int_pct / 100												intr_icms_rt, --D19 = D46 ICMS Interno | Ok AM
          trans_is_intra_st_ind												trans_is_intra_st_ind, --D20 1 = interna | 0 = interestadual Operação
          TRANS.pur_prc_amt - (TRANS.pur_prc_amt * TRANS.prc_disc_pct /100)	fnal_purchase_price_amt, --D28	Preço Final de Compra | Ok
          0																	pmpf_prc_amt, --D38 PMPF
          0																	cst_cd,
          TRANS.dsupp_is_distr_ind											dsupp_is_distr_ind,
          med.pnn_cd															pnn_cd,
          PR_M.prc_amt pcm_prc_amt, -- D38
          med.trava_pct / 100												trava_rt, --D42	Ok		
          med.mva_12_pct	/ 100												tax_am_12_pct,
          med.mva_7_pct	/ 100												tax_am_7_pct,
          med.mva_4_pct	/ 100												tax_am_4_pct,
          med.mva_orig_pct / 100												tax_am_orig_pct,
          med.red_mva_pct, --D42
          dprod.pck_is_medcn_ind
        from
          {pcm_ppp_ref_dim_prod} dprod
          join {pcm_ppp_buys_trans_fltr_1} trans on dprod.pcmdty_id = TRANS.pcmdty_id
          join {pcm_ppp_ref_tax_med_icms_all_states} med on dprod.pack_pnn_flag_cd = med.pnn_cd and med.gmrs_cd = dprod.pck_gmrs_class_cd and med.st_cd = upper('{state_cd}')
          left join {pcm_ppp_ref_dim_ext_prc_m} PR_M on PR_M.pcmdty_id = DPROD.pcmdty_id and TRANS.period_cd = PR_M.period_cd
        where 
          dprod.pck_is_medcn_ind = 1
          and TRANS.buyer_st_cd = upper('{state_cd}')
          and TRANS.period_cd between {period_cd_from} and {period_cd_to}
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
          )																												trans_icms_rt, --D21 ICMS da Operação | OK
          (fnal_purchase_price_amt * pis_cofins_rt)																		pis_cofins_cr_amt	--D31 Crédito PIS/COFINS	|	Ok
        FROM cte_med_st_1 med_tran
      ),
      cte_med_st_3
      AS

      (
        SELECT cte_med_st_2.*,
          case trans_icms_rt
            when 0.12 then tax_am_12_pct
            when 0.07 then tax_am_7_pct
            when 0.04 then tax_am_4_pct
            else tax_am_orig_pct
            end																											tax_am			--D33 Taxa AM |
        FROM cte_med_st_2
      ),
      cte_med_st_4
      AS

      (
        select cte_med_st_3.*,
          if(icms_typ_cd = 2 and trans_is_intra_st_ind = 1,
        0, 
        fnal_purchase_price_amt * tax_am
      )																														st_db			--D34 ST Débito	Ok ES
        from cte_med_st_3
      ),
      cte_med_st_5
      AS

      (
        select cte_med_st_4.*,
          fnal_purchase_price_amt + st_db - pis_cofins_cr_amt																	ppp_prc_amt			--D49 Custo da Farmácia	OK ES
        from cte_med_st_4
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


