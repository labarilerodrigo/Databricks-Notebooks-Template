# Databricks notebook source
# MAGIC %md
# MAGIC #### Input

# COMMAND ----------

# MAGIC %run ../config/pcm_common

# COMMAND ----------

dbutils.widgets.text('hist_months', '-3')
hist_months = int(dbutils.widgets.get('hist_months'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETL

# COMMAND ----------

import uuid
from datetime import datetime


# args
percent_dev = 0.95
approx_percentile_accuracy = 1000000000
batch_id = uuid.uuid4()
timestamp = datetime.now()
period_cd_hist_from = spark.sql(f"select left(cast(date_format(dateadd(month, -3, to_date(concat({period_cd_from}, '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6) as period_cd").first().period_cd
period_cd_hist_to = spark.sql(f"select left(cast(date_format(dateadd(month, -1, to_date(concat({period_cd_from}, '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6) as period_cd").first().period_cd


#steps

# 10
spark.sql(f"use {config['pcmppp_db']}")

try:
    # 11
    spark.sql(f"""
    create or replace table tmp_sub_national_staging3 as
        with
            cte_hist_avg_prc
            as
            (
                select pcmdty_id, geo_lvl_cd, '{period_cd_from}' as period_cd, avg(ppp_prc_amt) as prc_avg
                from {pcm_ppp_wgt_calc}
                where period_cd between '{period_cd_hist_from}' and '{period_cd_hist_to}'
                group by pcmdty_id, geo_lvl_cd
            ),
            cte_calc_dev
            as
            (
                select a.*,
                    b.pck_is_medcn_ind,
                    b.otc_seg_cd,
                    c.prc_avg,
                    if(c.prc_avg = 0, 0, abs(a.ppp_prc_amt- c.prc_avg)/c.prc_avg) as deviation
                from {pcm_ppp_wgt_calc} a
                inner join {pcm_ppp_ref_dim_prod} b on a.pcmdty_id = b.pcmdty_id
                inner join cte_hist_avg_prc c on a.pcmdty_id = c.pcmdty_id and a.geo_lvl_cd = c.geo_lvl_cd and a.period_cd = c.period_cd
            ),
            cte_calc_perc
            as
            (
                select 
                    period_cd,
                    pck_is_medcn_ind,
                    otc_seg_cd,
                    approx_percentile(deviation, {percent_dev}, {approx_percentile_accuracy}) perc_rank
                from cte_calc_dev
                group by period_cd, pck_is_medcn_ind, otc_seg_cd
            ),
            cte_calc_all
            as
            (
                select 
                    d.period_cd,
                    d.pck_is_medcn_ind,
                    d.otc_seg_cd,
                    d.pcmdty_id,
                    d.geo_lvl_cd,
                    d.ppp_prc_amt,
                    d.prc_avg,
                    d.deviation,
                    p.perc_rank
                from cte_calc_dev d
                join cte_calc_perc p on d.period_cd = p.period_cd and d.pck_is_medcn_ind = p.pck_is_medcn_ind and d.otc_seg_cd = p.otc_seg_cd
            )
        select pcmdty_id, geo_lvl_cd, period_cd, ppp_prc_amt, prc_avg as ppp_prc_amt_new
        from cte_calc_all
        where deviation > perc_rank
    """)

    # 12
    spark.sql(f"""
    insert into {pcm_ppp_wgt_calc_hist}
        select a.pcmdty_id,
            a.geo_lvl_cd,
            a.ppp_prc_amt,
            b.step_cd,
            '{batch_id}' as batch_id,
            '{timestamp}' as isrt_ts,
            'Prices-Smooth-Step-2' as replmnt_rsn_cd,
            a.period_cd
        from tmp_sub_national_staging3 a
        join {pcm_ppp_wgt_calc} b on a.pcmdty_id = b.pcmdty_id and  a.geo_lvl_cd= b.geo_lvl_cd and a.period_cd = b.period_cd
    """).show()

    # 13
    spark.sql(f"""
    --Update sub national prices with the new calculated prices (pcm_ppp_wgt_calc_hist)
    merge into {pcm_ppp_wgt_calc} a 
    using tmp_sub_national_staging3 b
    on (a.period_cd = b.period_cd and a.pcmdty_id = b.pcmdty_id and a.geo_lvl_cd = b.geo_lvl_cd)
    when matched then
    update set a.ppp_prc_amt = b.ppp_prc_amt_new, a.step_cd = 51
    """).show()

    # 14
    spark.sql(f"""
        --Recalculate National Prices
        create or replace table tmp_national_staging as
        with
            cte_states_valid_units
            as
            (
                select period_cd, pcmdty_id, substring(geo_lvl_cd, 1, 2) as uf, if(sum(wgt_units_qty)>0, 1, 0) as units_ind
                from {pcm_ppp_wgt_calc}
                where period_cd = '{period_cd_from}'
                group by period_cd, pcmdty_id, substring(geo_lvl_cd, 1, 2)
            ),
            cte_national_prc
            as
            (
                select a.pcmdty_id, a.period_cd,
                    if(sum(wgt_units_qty)= 0,
                        avg(ppp_prc_amt),
                        if(sum(sales_units_qty)=0,
                            0,
                            sum(ppp_prc_amt*sales_units_qty*units_ind)/sum(sales_units_qty*units_ind))) as ppp_prc_amt
                from {pcm_ppp_wgt_calc} a
                join cte_states_valid_units b on a.period_cd = b.period_cd and a.pcmdty_id = b.pcmdty_id and substring(a.geo_lvl_cd, 1, 2) = b.uf
                group by a.pcmdty_id, a.period_cd
            ),
            cte_flag_updates
            as
            (
                select a.pcmdty_id, a.ppp_prc_amt as ppp_prc_amt_new, a.period_cd, b.ppp_prc_amt
                from cte_national_prc a
                inner join {pcm_ppp_prc} b on a.period_cd = b.period_cd and a.pcmdty_id = b.pcmdty_id
                where a.ppp_prc_amt <> b.ppp_prc_amt
            )
        select *
        from cte_flag_updates
    """)

    # 15
    spark.sql(f"""
    insert into {pcm_ppp_prc_hist}
        select a.pcmdty_id,
            a.ppp_prc_amt,
            b.step_cd,
            '{batch_id}' as batch_id,
            '{timestamp}' as isrt_ts,
            'Prices-Smooth' as replmnt_rsn_cd,
            a.period_cd
        from tmp_national_staging a
        join {pcm_ppp_prc} b on a.pcmdty_id = b.pcmdty_id and a.period_cd = b.period_cd
    """).show()

    # 16
    spark.sql(f"""
    --Update sub national prices with the new calculated prices (pcm_ppp_wgt_calc_hist)
    merge into {pcm_ppp_prc} a 
    using tmp_national_staging b
    on (a.period_cd= b.period_cd and a.pcmdty_id= b.pcmdty_id)
    when matched then
    update set a.ppp_prc_amt= b.ppp_prc_amt_new, a.step_cd = 51
    """).show()
except Exception as e:
    raise e
finally:
    spark.sql("drop table if exists tmp_sub_national_staging3")
    spark.sql("drop table if exists tmp_national_staging")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------


