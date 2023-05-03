# Databricks notebook source
# MAGIC %md
# MAGIC #### Input

# COMMAND ----------

# MAGIC %run ../config/pcm_common

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETL

# COMMAND ----------

import uuid
from datetime import datetime


# args
batch_id = uuid.uuid4()
timestamp = datetime.now()

#steps
# 10
spark.sql(f"use {config['pcmppp_db']}")

try:
    # 11
    spark.sql(f"""
    --Identify all the fcc/uf/channel prices with big variations
    create or replace table tmp_sub_national_staging as
    with
        cte_universe
        as
        (
            select a.pcmdty_id,
                a.geo_lvl_cd,
                a.ppp_prc_amt,
                a.period_cd,
                b.ppp_prc_amt as prc_national,
                a.step_cd
            from {pcm_ppp_wgt_calc} a
            inner join {pcm_ppp_prc} b on a.pcmdty_id = b.pcmdty_id and a.period_cd = b.period_cd
            where (a.period_cd between '{period_cd_from}' and '{period_cd_to}')
        ),
        cte_group_FCC
        as
        (
            select pcmdty_id, period_cd, avg(ppp_prc_amt) avg_prc, cast(stddev(ppp_prc_amt) as float) std_prc
            from cte_universe
            where ppp_prc_amt <> prc_national
            group by pcmdty_id, period_cd
        ),
        cte_smooth_flag
        as
        (
            --Flag as outliers all combinations of FCC/UF/Channel
            select a.pcmdty_id,
                a.geo_lvl_cd,
                a.ppp_prc_amt,
                a.period_cd,
                a.step_cd,
                substring(a.geo_lvl_cd, 1, 2) as uf,
                substring(a.geo_lvl_cd, 3, 1) as channel,
                if((a.ppp_prc_amt > (avg_prc+ (1.5 * std_prc))) or (a.ppp_prc_amt < (avg_prc- (1.5 * std_prc))),
                    1,
                    0) flag_smooth
            from cte_universe a
                inner join cte_group_FCC b on a.pcmdty_id = b.pcmdty_id and a.period_cd = b.period_cd
        ),
        --Step 1(a): For same fcc/uf, only one of the channel type price has huge variation
        cte_replace_prc_1
        as
        (
            select a.*,
                if(b.ppp_prc_amt is not null, b.ppp_prc_amt, a.ppp_prc_amt) as ppp_prc_amt_new,
                if(b.ppp_prc_amt is not null, 1, 0) as flag_updated
            from cte_smooth_flag a
            left join cte_smooth_flag b on a.pcmdty_id = b.pcmdty_id and a.uf = b.uf and a.period_cd = b.period_cd and b.flag_smooth = 0
        )
    select distinct period_cd, pcmdty_id, geo_lvl_cd, ppp_prc_amt, ppp_prc_amt_new, flag_smooth, flag_updated, step_cd
    from cte_replace_prc_1
    where flag_smooth = 1
    """)

    # 12
    spark.sql(f"""
    --Update historical table(pcm_ppp_wgt_calc_hist) with fcc identified in the step 1 prices smooth process
    insert into {pcm_ppp_wgt_calc_hist}
    select pcmdty_id,
           geo_lvl_cd,
           ppp_prc_amt,
           step_cd,
           '{batch_id}' as batch_id,
           '{timestamp}' as isrt_ts,
           'Prices-Smooth-Step-1.1' as replmnt_rsn_cd,
           period_cd
    from tmp_sub_national_staging
    where flag_updated = 1
    """).show()

    # 13
    spark.sql(f""" 
    --Update sub national prices with the new calculated prices (pcm_ppp_wgt_calc_hist)
    merge into {pcm_ppp_wgt_calc} a 
    using tmp_sub_national_staging b
    on (a.period_cd = b.period_cd and a.pcmdty_id = b.pcmdty_id and a.geo_lvl_cd = b.geo_lvl_cd and b.flag_updated = 1)
    when matched then
    update set a.ppp_prc_amt = b.ppp_prc_amt_new, a.step_cd = 51
    """).show()

    # 14
    spark.sql(f"""
    --Step 1(b), For FCC records where both the channel's for a state(UF) have big variations
    create or replace table tmp_sub_national_staging2 as
    with
        cte_universe
        as
        (
            select a.*, substring(a.geo_lvl_cd, 1, 2) as uf,
                if(b.flag_smooth= 1 and b.flag_updated = 0, 1, 0) as flag_smooth_2
            from {pcm_ppp_wgt_calc} a
            left join tmp_sub_national_staging b on a.period_cd = b.period_cd and a.pcmdty_id = b.pcmdty_id and a.geo_lvl_cd = b.geo_lvl_cd
            where a.period_cd between '{period_cd_from}' and '{period_cd_to}'
        ),
        cte_state_valid_units
        as
        (
            select period_cd, pcmdty_id, uf, sum(wgt_units_qty) as units
            from cte_universe
            group by period_cd, pcmdty_id, uf
        ),
        cte_valid_units_price
        as
        (
            select a.*, if(b.units > 0, 1, 0) as units_ind
            from cte_universe a
            join cte_state_valid_units b on a.period_cd = b.period_cd and a.pcmdty_id = b.pcmdty_id and a.uf = b.uf
        ),
        cte_national_prc
        as
        (
            select pcmdty_id, period_cd,
                   if(sum(sales_units_qty * units_ind) = 0, 0, sum(sales_units_qty * units_ind * ppp_prc_amt)/sum(sales_units_qty * units_ind)) as prc_national
            from cte_valid_units_price
            where flag_smooth_2 = 0
            group by pcmdty_id, period_cd
        ),
        cte_update_prc
        as
        (
            select a.*,
                   b.prc_national,
                   if(flag_smooth_2 = 1, b.prc_national, a.ppp_prc_amt) as ppp_prc_amt_new
            from cte_universe a
            inner join cte_national_prc b on a.pcmdty_id = b.pcmdty_id and a.period_cd = b.period_cd
        )
    select pcmdty_id, geo_lvl_cd, ppp_prc_amt, ppp_prc_amt_new, wgt_units_qty, period_cd, step_cd
    from cte_update_prc
    where flag_smooth_2 = 1
    """)

    # 15
    spark.sql(f"""
    --Update historical table of sub national prices(pcm_ppp_wgt_calc_hist)
    insert into {pcm_ppp_wgt_calc_hist}
    select pcmdty_id,
           geo_lvl_cd,
           ppp_prc_amt,
           step_cd,
           '{batch_id}' as batch_id,
           '{timestamp}' as isrt_ts,
           'Prices-Smooth-Step-1.2' as replmnt_rsn_cd,
           period_cd
    from tmp_sub_national_staging2
    """).show()

    # 16
    spark.sql(f""" 
    --Update sub national prices
    merge into {pcm_ppp_wgt_calc} a 
    using tmp_sub_national_staging2 b
    on (a.period_cd= b.period_cd and a.pcmdty_id = b.pcmdty_id and a.geo_lvl_cd = b.geo_lvl_cd)
    when matched then
    update set a.ppp_prc_amt = b.ppp_prc_amt_new, a.step_cd = 51
    """).show()
except Exception as e:
    raise e
finally:
    # 17
    spark.sql("drop table if exists tmp_sub_national_staging")
    spark.sql("drop table if exists tmp_sub_national_staging2")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------


