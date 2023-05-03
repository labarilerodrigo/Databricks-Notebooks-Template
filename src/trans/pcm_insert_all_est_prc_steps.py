# Databricks notebook source
# MAGIC %md
# MAGIC #### Input

# COMMAND ----------

# MAGIC %run ../config/pcm_common

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETL

# COMMAND ----------

from typing import List, Tuple

# args
steps = [102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116,
        202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219]

def get_step_cols(step: int) -> Tuple[int, List[str]]:
    """
    Returns a tuple of is_medcn, cols
    """
    # steps for is_medcn == 0
    if step == 102:
        return 0, ['manuf_cd', 'prod_cd', 'nec4_cd', 'app1_cd']
    elif step == 103:
        return 0, ['manuf_cd', 'prod_cd', 'nec4_cd']
    elif step == 104:
        return 0, ['manuf_cd', 'prod_cd', 'nec3_cd', 'app1_cd']
    elif step == 105:
        return 0, ['manuf_cd', 'prod_cd', 'nec3_cd']
    elif step == 106:
        return 0, ['manuf_cd', 'prod_cd', 'nec2_cd', 'app1_cd']
    elif step == 107:
        return 0, ['manuf_cd', 'prod_cd', 'nec2_cd']
    elif step == 108:
        return 0, ['nec4_cd', 'app1_cd']
    elif step == 109:
        return 0, ['nec4_cd']
    elif step == 110:
        return 0, ['nec3_cd', 'app1_cd']
    elif step == 111:
        return 0, ['nec3_cd']
    elif step == 112:
        return 0, ['nec2_cd', 'app1_cd']
    elif step == 113:
        return 0, ['nec2_cd']
    elif step == 114:
        return 0, ['nec1_cd', 'app1_cd']
    elif step == 115:
        return 0, ['nec1_cd']
    elif step == 116:
        return 0, ['mseg3_cd']
    # steps for is_medcn == 1
    elif step == 202:
        return 1, ['manuf_cd', 'prod_cd', 'atc4_cd', 'app1_cd', 'mseg3_cd']
    elif step == 203:
        return 1, ['manuf_cd', 'prod_cd', 'atc4_cd', 'mseg3_cd']
    elif step == 204:
        return 1, ['manuf_cd', 'prod_cd', 'atc4_cd']
    elif step == 205:
        return 1, ['manuf_cd', 'atc3_cd', 'app1_cd', 'mseg3_cd']
    elif step == 206:
        return 1, ['manuf_cd', 'atc3_cd', 'mseg3_cd']
    elif step == 207:
        return 1, ['atc4_cd', 'app1_cd', 'mseg3_cd']
    elif step == 208:
        return 1, ['atc4_cd', 'mseg3_cd']
    elif step == 209:
        return 1, ['atc4_cd', 'app1_cd']
    elif step == 210:
        return 1, ['atc3_cd', 'app1_cd', 'mseg3_cd']
    elif step == 211:
        return 1, ['atc3_cd', 'mseg3_cd']
    elif step == 212:
        return 1, ['atc3_cd', 'app1_cd']
    elif step == 213:
        return 1, ['atc4_cd']
    elif step == 214:
        return 1, ['app1_cd', 'mseg3_cd']
    elif step == 215:
        return 1, ['mseg3_cd']
    elif step == 216:
        return 1, ['atc2_cd', 'app1_cd']
    elif step == 217:
        return 1, ['atc3_cd']
    elif step == 218:
        return 1, ['atc2_cd']
    elif step == 219:
        return 1, ['mseg3_cd']
    else:
        raise ValueError('Invalid step code')

# loop through steps
for step in steps:
    print(f'processing step: {step}')
    is_medcn, cols = get_step_cols(step)
    join_filter = ' and '.join([f'e.{col} = a.{col}' for col in cols])
    group_cond = ', '.join([f'a.{col}' for col in cols])
    cluster_cols = ', '.join([f'a.{col}' for col in cols])
    
    #10
    spark.sql("drop view if exists step_avg_prc")
    
    # 11
    spark.sql(f"delete from {pcm_ppp_prc} where step_cd = {step} and period_cd between {period_cd_from} and {period_cd_to}")
    
    # 12
    spark.sql(f"""
    cache table step_avg_prc
    select
    prc.period_cd as period_cd,
    avg(prc.ppp_prc_amt) as ppp_prc_amt, 
    cast(avg(if(prc.ppp_prc_amt > cpp_prc.prc_amt,null , if(prc.ppp_prc_amt=0, null, 1-((cpp_prc.prc_amt-prc.ppp_prc_amt)/cpp_prc.prc_amt)))) as numeric(18,4)) as c_disc_rt,
    avg(1-((coalesce(tr_lst_prc.prc_amt, 0.01)) - prc.ppp_prc_amt) / (coalesce(tr_lst_prc.prc_amt, 0.01))) as L_disc_rt,
    {group_cond}
    from {pcm_ppp_prc} prc
    inner join {pcm_ppp_ref_dim_prod} a on a.pcmdty_id = prc.pcmdty_id
    left join {pcm_ppp_ref_dim_ext_prc_l} tr_lst_prc on tr_lst_prc.pcmdty_id = a.pcmdty_id and tr_lst_prc.period_cd = prc.period_cd
    left join {pcm_ppp_ref_dim_ext_prc_d} cpp_prc on cpp_prc.pcmdty_id = prc.pcmdty_id 
    and cpp_prc.period_cd = left(cast(date_format(dateadd(month, -1, to_date(concat(prc.period_cd, '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6)
    where prc.period_cd between {period_cd_from} and {period_cd_to} and prc.step_cd < 100 and a.pck_is_medcn_ind = {is_medcn}
    and cpp_prc.prc_amt > 0.05
    group by prc.period_cd, {group_cond}
    """)
    
    # 13
    spark.sql(f"""
    with
    prc_periods as (select distinct period_cd from {pcm_ppp_prc} prc where prc.period_cd between {period_cd_from} and {period_cd_to}),
    not_calc_fcc as --Give me the FCCs that are not in Step-1 (Acquired) or in any other already inserted Step
        (select p.pcmdty_id, pd.period_cd from {pcm_ppp_ref_dim_prod} p 
                                          cross join prc_periods pd 
                                          left join {pcm_ppp_prc} prc on prc.pcmdty_id = p.pcmdty_id and prc.period_cd = pd.period_cd 
                                          where prc.pcmdty_id is null and p.pck_is_medcn_ind = {is_medcn}),
    steps_prc_calc as 
        (
            select
            e.pcmdty_id,
            a.period_cd as period_cd, 
            {step} as step_cd,
            if(cprc.prc_amt is null, a.ppp_prc_amt, a.C_disc_rt * cprc.prc_amt) as nomed_prc_amt,
            if(lprc.prc_amt is null, a.ppp_prc_amt, a.L_disc_rt * lprc.prc_amt) as med_prc_amt,
            lprc.prc_amt as est_lst_prc
            from step_avg_prc a
            inner join {pcm_ppp_ref_dim_prod} e on {join_filter}
            inner join not_calc_fcc f on f.pcmdty_id = e.pcmdty_id and f.period_cd = a.period_cd
            left join {pcm_ppp_ref_dim_ext_prc_l} lprc on lprc.pcmdty_id = e.pcmdty_id and lprc.period_cd=a.period_cd and e.pck_is_medcn_ind=1
            and lprc.prc_amt > 0.05
            left join {pcm_ppp_ref_dim_ext_prc_d} cprc on cprc.pcmdty_id = e.pcmdty_id and cprc.prc_amt > 0.05
            and cprc.period_cd = left(cast(date_format(dateadd(month, -1, to_date(concat(a.period_cd, '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6)
            where e.pck_is_medcn_ind = {is_medcn} and a.period_cd between {period_cd_from} and {period_cd_to}
        )

    insert into {pcm_ppp_prc} (pcmdty_id, ppp_prc_amt, period_cd, step_cd)
    select 
    c.pcmdty_id,
    case
        when {is_medcn} = 1 then if(c.med_prc_amt > c.est_lst_prc, c.est_lst_prc , c.med_prc_amt) --for MED, MAX-Limit price is CurrentMth list Price.
        else ifnull(c.nomed_prc_amt, if(c.med_prc_amt > c.est_lst_prc, c.est_lst_prc , c.med_prc_amt)) --is_medcn=0
    end ppp_prc_amt,
    c.period_cd,
    c.step_cd
    --,if({is_medcn} = 1 and (c.med_prc_amt <=0 or c.med_prc_amt > c.est_lst_prc) , c.est_lst_prc,  c.med_prc_amt) as  med_prc_amt
    from steps_prc_calc c
    """).show()



# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------


