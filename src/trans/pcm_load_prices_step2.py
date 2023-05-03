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
start_dt = spark.sql(f"select to_date(concat('{period_cd_from}', '01'), 'yyyyMMdd') as period").first().period
end_dt = spark.sql(f"select to_date(concat('{period_cd_to}', '01'), 'yyyyMMdd') as period").first().period

## steps

# 10
spark.sql(f"delete from {pcm_ppp_wgt_calc} where step_cd = 2 and period_cd between '{period_cd_from}' and '{period_cd_to}'")
spark.sql(f"delete from {pcm_ppp_prc} where step_cd = 2 and period_cd between '{period_cd_from}' and '{period_cd_to}'")

# 10
spark.sql("drop view if exists tmp_chain_ind")
spark.sql("""
cache table tmp_chain_ind
select 1 as chain_ind
union
select 0 as chain_ind
""")

# 11
spark.sql("drop view if exists tmp_all_states")
spark.sql(f"""
--cross join to get all the combination of states, chain_ind and fcc
cache table tmp_all_states
select a.state_abbrev as state_cd, b.chain_ind
from {pcm_ppp_ref_states} a
cross join tmp_chain_ind b
""")

# 12
while start_dt <= end_dt:
    spark.sql("drop view if exists tmp_prc_step2")
    spark.sql(f"""
    cache table tmp_prc_step2
        with
            cte_fcc_history
            as
            (
                select period_cd, pcmdty_id, ppp_prc_amt
                from {pcm_ppp_prc}
                where period_cd = left(date_format(to_date(dateadd(month, -1, '{start_dt}')), 'yyyyMMdd'), 6)
                    and step_cd in (1, 2, 50, 51)
            ),
            cte_fcc_update_current
            as
            (
                select distinct left(date_format('{start_dt}', 'yyyyMMdd'), 6) as period_cd, a.pcmdty_id, a.ppp_prc_amt
                from cte_fcc_history a
                left join {pcm_ppp_prc} b on a.pcmdty_id = b.pcmdty_id and b.period_cd = left(date_format('{start_dt}', 'yyyyMMdd'), 6)
                where b.ppp_prc_amt is null
            ),
            cte_all
            as
            (
                (
                    select a.period_cd, a.pcmdty_id, a.ppp_prc_amt, b.state_cd, if(b.chain_ind = 0, 'I', 'R') channel, 0 as national_ind
                    from cte_fcc_update_current a
                    cross join (select distinct state_cd, chain_ind from tmp_all_states) b
                )
                    --Cross join to get all combinations of fcc, state, channel; This is to load to sub-national table
                union
                    (select period_cd, pcmdty_id, ppp_prc_amt, NULL as state_cd, NULL as channel, 1 as national_ind --national_ind = 1 is for sub-national prices
                    from cte_fcc_update_current)
            )
        select *
        from cte_all
        order by pcmdty_id, state_cd, channel
    """)
    
    spark.sql(f"""
        insert into {pcm_ppp_wgt_calc}
        select pcmdty_id,
            concat(state_cd, channel) as geo_lvl_cd,
            ppp_prc_amt,
            0 as wgt_units_qty,
            2 as step_cd,
            0 as sales_units_qty,
            period_cd
        from tmp_prc_step2
        where national_ind = 0
    """).show()
    
    spark.sql(f"""
        insert into {pcm_ppp_prc}
        select pcmdty_id,
            ppp_prc_amt,
            2 as step_cd,
            period_cd
        from tmp_prc_step2
        where national_ind = 1
    """).show()
    
    start_dt = spark.sql(f"select to_date(dateadd(month, 1, '{start_dt}')) as period").first().period

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------


