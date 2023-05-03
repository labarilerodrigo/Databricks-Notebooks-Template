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
cnt = 0
max_periods = 60
## steps

# 10
spark.sql(f"""drop table if exists {temp_periods}""")
spark.sql("drop view if exists prc_to_copy")
spark.sql(f"""create table {temp_periods} (period_cd string)""")
while cnt < max_periods:
    spark.sql(f"""
    insert into {temp_periods} (period_cd)
    select left(cast(date_format(dateadd(month, -{cnt}, to_date(concat('{period_cd_to}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6)
    """)
    cnt += 1
spark.sql(f"""
cache table prc_to_copy
    with all_pcmdty as (
            select distinct 
                pcmdty_id, 
                ppp_prc_amt
            from {pcm_ppp_prc}
            where step_cd >= 100
            and period_cd = '{period_cd_to}'
        ),
        cte_with_no_prc as (
            select 
                pcmdty_id, ppp_prc_amt
            from all_pcmdty
            where pcmdty_id not in 
                (
                    select distinct 
                        pcmdty_id
                    from {pcm_ppp_prc}
                    where period_cd < '{period_cd_to}'
                )	
        ), cmplt_hist as (
            select pcmdty_id, ppp_prc_amt, period_cd
            from cte_with_no_prc
            cross join {temp_periods}
        )
        
        select * 
        from cmplt_hist
        where period_cd < '{period_cd_to}';
""")

spark.sql(f"""
    with pcmdty_id_valid as(
		select distinct pcmdty_id
		from prc_to_copy
		), geo_lv as(
		select distinct  geo_lvl_cd
		from {pcm_ppp_wgt_calc}
		where pcmdty_id in (select max(pcmdty_id) from prc_to_copy)
	)
    -- Sub-national insert
	insert into {pcm_ppp_wgt_calc}
	select pcmdty_id, geo_lvl_cd, ppp_prc_amt, 0 wgt_units_qty, 54 step_cd, 0 sales_units_qty, period_cd
    from prc_to_copy
	cross join geo_lv;
""").show()

spark.sql(f"""
-- National insert
	insert into {pcm_ppp_prc}
	select pcmdty_id, ppp_prc_amt, 54 step_cd, period_cd
	from prc_to_copy
""").show()

spark.sql(f"""drop table if exists {temp_periods}""")
spark.sql("drop view if exists prc_to_copy")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation
