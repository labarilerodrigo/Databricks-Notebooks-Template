# Databricks notebook source
# MAGIC %md
# MAGIC #### Input

# COMMAND ----------

# MAGIC %run ../config/pcm_common

# COMMAND ----------

# MAGIC %md
# MAGIC #### Helpers

# COMMAND ----------

from datetime import datetime

# set period
period_cd = period_cd_from

def evaluate_rule(rule_name: str,
                  rule_desc: str,
                  rule_number: int,
                  rule_query: str,
                  timestamp: datetime = datetime.now(),
                  period_cd: str = period_cd):
    """
    Assert tests against a spark query, result that returns 0 rows or 'PASSED' passes, result with != 0 rows or 'FAILED' fails.
    """
    result_query = spark.sql(rule_query).head()[0]
    try:
        assert 0 == result_query or 'PASSED' == result_query
        result_cd = 'PASSED'
        assertion_failed = False
    except Exception:
        result_cd = 'FAILED'
        assertion_failed = True
    df = spark.sql(f"""
    select '{rule_name}'     as rule_name, 
           '{rule_desc}'     as rule_desc, 
           '{rule_number}'   as rule_number, 
           '{timestamp}'     as exec_timestamp, 
           '{period_cd}'     as period_cd, 
           '{result_cd}'     as result_cd
           """)
    display(df)
    if assertion_failed:
        raise AssertionError(f"Rule: '{rule_name}' FAILED with result query: [{result_query}]")

    

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------

# template
evaluate_rule(rule_name='Rule',
              rule_desc='Desc',
              rule_number=1,
              rule_query=f"""
              select 0 as cnt
              """)

# COMMAND ----------

evaluate_rule(rule_name='Checking the price of CPP',
              rule_desc='The CPP price of the current period must exist',
              rule_number=3,
              rule_query=f"""
              select case when exists (
                 select
                 count(*) as cnt
                 from {lgy_prices}
                 where left(price_date, 6) = {period_cd}
                 and price_type in ('C')
                 group by price_type
              )
              then 'PASSED' else 'FAILED' end
              """)

# COMMAND ----------

evaluate_rule(rule_name='Checking the price of PMC',
              rule_desc='The PMC price of the current period must exist',
              rule_number=4,
              rule_query=f"""
              select case when exists (
                 select
                 count(*) as cnt  
                 from {lgy_prices}
                 where left(price_date, 6) = {period_cd}
                 and price_type in ('M')
                 group by price_type
              )
              then 'PASSED' else 'FAILED' end
              """)

# COMMAND ----------

evaluate_rule(rule_name='Bridged Review',
              rule_desc='Checking that the bridges are correct',
              rule_number=103,
              rule_query=f"""
              select case when exists (
                  select count(*) as cnt
                  from {pcm_ppp_buys_trans}
                  where pcmdty_id < 0 and pcmdty_id > 999999
                  having count(*) > 1
              )
              then 'FAILED' else 'PASSED' end
              """)

# COMMAND ----------

evaluate_rule(rule_name='CPP threshold',
              rule_desc='CPP threshold',
              rule_number=110,
              rule_query=f"""
                with cte_uni as (
                    select pcm.pcmdty_id, pcm.ppp_prc_amt price_ppp, cpp.prc_amt price_cpp, pcm.period_cd
                    from {pcm_ppp_prc} pcm
                    join {pcm_ppp_ref_dim_ext_prc_d} cpp on pcm.pcmdty_id = cpp.pcmdty_id and pcm.period_cd = pcm.period_cd
                    where cpp.prc_amt > 0.05 and pcm.period_cd = {period_cd} 
                )
                select case when exists (
                    select pcmdty_id,price_ppp, price_cpp, period_cd, (price_cpp * ref.cpp_min_pct / 100) mini, (price_cpp * ref.cpp_max_pct/100) maxi from cte_uni
                    cross join {pcm_ppp_cpp_min_max_pct} ref
                    where price_ppp < (price_cpp * ref.cpp_min_pct / 100) and price_ppp > (price_ppp * ref.cpp_max_pct / 100)
                )
                then 'FAILED' else 'PASSED' end
              """)

# COMMAND ----------

evaluate_rule(rule_name='Duplicate FCC Prices National',
              rule_desc='No FCC should be duplicated on pcm_ppp_prc table',
              rule_number=116,
              rule_query=f"""
               select case when exists (
                 select
                 pcmdty_id, period_cd, count(1) as cnt
                 from {pcm_ppp_prc}
                 where period_cd between '{period_cd_from}' and '{period_cd_to}'
                 group by pcmdty_id, period_cd
                 having count(1) > 1
               )
               then 'FAILED' else 'PASSED' end
              """)

# COMMAND ----------

evaluate_rule(rule_name='Duplicated FCC',
              rule_desc='No FCC should be duplicated on Dim Products',
              rule_number=101,
              rule_query=f"""
              select case when exists (
                select
                pcmdty_id, count(1) as cnt
                from {pcm_ppp_ref_dim_prod}
                group by pcmdty_id
                having count(1) > 1
              )
              then 'FAILED' else 'PASSED' end
              """)

# COMMAND ----------

evaluate_rule(rule_name='Exist FCC',
              rule_desc='All FCCs from Dim_Products should exist on pcm_ppp_prc table',
              rule_number=115,
              rule_query=f"""
              select case when exists (
                select p.pcmdty_id
                from {pcm_ppp_ref_dim_prod} p
                left join {pcm_ppp_prc} pr on pr.pcmdty_id = p.pcmdty_id and pr.period_cd = {period_cd}
                where pr.pcmdty_id is null
              )
              then 'FAILED' else 'PASSED' end
              """)

# COMMAND ----------

evaluate_rule(rule_name='Maximum Med Price',
              rule_desc='No Medicine FCC should have PPP Price greater than List Price "L"',
              rule_number=111,
              rule_query=f"""
              select case when exists (  
                 select distinct prc.period_cd,prc.step_cd, prc.pcmdty_id, pr.prc_amt L_Price, prc.ppp_prc_amt  
                 from {pcm_ppp_prc} prc  
                 inner join {pcm_ppp_ref_dim_prod} a on a.pcmdty_id = prc.pcmdty_id  
                 inner join {pcm_ppp_ref_dim_ext_prc_l} pr on  
                 pr.pcmdty_id = a.pcmdty_id and pr.period_cd = prc.period_cd  
                 and a.pck_is_medcn_ind=1  
                 and prc.period_cd between '{period_cd_from}' and '{period_cd_to}'
                 where 1=1
                 and pr.prc_amt < prc.ppp_prc_amt and pr.prc_amt > 0.05
                 and step_cd <> 2
              )  
              then 'FAILED' else 'PASSED' end
              """)

# COMMAND ----------

evaluate_rule(rule_name='Negative units exists',
              rule_desc='Negative units must not exist in the wgt table',
              rule_number=112,
              rule_query=f"""
              select case when exists (  
                 select period_cd, count (*) as cnt  
                 from {pcm_ppp_wgt_calc}
                 where period_cd between '{period_cd_from}' and '{period_cd_to}'
                 and sales_units_qty < 0
                 group by period_cd
                 having count(*) > 1
              )  
              then 'FAILED' else 'PASSED' end
              """)

# COMMAND ----------

evaluate_rule(rule_name='Price > 0.05',
              rule_desc='The Sub-National price must be greater than 0.05 for all fcc med and no-med',
              rule_number=104,
              rule_query=f"""
              select case when exists (
                 select count(*) as cnt
                 from {pcm_ppp_wgt_calc}
                 where ppp_prc_amt <= 0.05 and step_cd = 1
                 and period_cd between '{period_cd_from}' and '{period_cd_to}'
                 having count(1) > 1
              )
              then 'FAILED' else 'PASSED' end
              """)

evaluate_rule(rule_name='Price > 0.05 National',
              rule_desc='The National price must be greater than 0.05 for all fcc med and no-med',
              rule_number=105,
              rule_query=f"""
              select case when exists (
                 select count(*) as cnt
                 from {pcm_ppp_prc}
                 where ppp_prc_amt <= 0.05 and step_cd = 1
                 and period_cd between '{period_cd_from}' and '{period_cd_to}'
                 having count(1) > 1
              )
              then 'FAILED' else 'PASSED' end
              """)

# COMMAND ----------

evaluate_rule(rule_name='Price is null', 
              rule_desc='The sub-national price must not contain nulls',
              rule_number=117,
              rule_query=f"""
              select case when exists (
                 select count(*) as cnt
                 from {pcm_ppp_wgt_calc}
                 where ppp_prc_amt is null
                 and period_cd between '{period_cd_from}' and '{period_cd_to}'
                 having count(1) > 1
              )
              then 'FAILED' else 'PASSED' end
              """)

evaluate_rule(rule_name='Price is null - National', 
              rule_desc='The national price must not contain nulls',
              rule_number=118,
              rule_query=f"""
              select case when exists (
                 select count(*) as cnt
                 from {pcm_ppp_prc}
                 where ppp_prc_amt is null
                 and period_cd between '{period_cd_from}' and '{period_cd_to}'
                 having count(1) > 1
              )  
              then 'FAILED' else 'PASSED' end
              """)

# COMMAND ----------

evaluate_rule(rule_name='Price > 0', 
              rule_desc='The Sub-National price must be greater than 0 for all fcc med and no-med',
              rule_number=113,
              rule_query=f"""
              select case when exists (
                 select count(*) as cnt
                 from {pcm_ppp_wgt_calc}
                 where ppp_prc_amt <= 0
                 and period_cd between '{period_cd_from}' and '{period_cd_to}'
                 having count(1) > 1
              )
              then 'FAILED' else 'PASSED' end
              """)

evaluate_rule(rule_name='Price > 0 National', 
              rule_desc='The National price must be greater than 0 for all fcc med and no-med',
              rule_number=114,
              rule_query=f"""
              select case when exists (
                 select count(*) as cnt
                 from {pcm_ppp_prc}
                 where ppp_prc_amt <= 0
                 and period_cd between '{period_cd_from}' and '{period_cd_to}'
                 having count(1) > 1
              )
              then 'FAILED' else 'PASSED' end
              """)

# COMMAND ----------

evaluate_rule(rule_name='Maximum-med-price step2', 
              rule_desc='No Medicine FCC should have PPP Price greater than List Price "L"',
              rule_number=108,
              rule_query=f"""
              select case when exists (
                 select distinct prc.period_cd, prc.step_cd, prc.pcmdty_id, pr.prc_amt l_Price, prc.ppp_prc_amt
                 from {pcm_ppp_prc} prc
                 inner join {pcm_ppp_ref_dim_prod} a on a.pcmdty_id = prc.pcmdty_id
                 inner join {pcm_ppp_ref_dim_ext_prc_l} pr on
                 pr.pcmdty_id = a.pcmdty_id and pr.period_cd = prc.period_cd
                 and a.pck_is_medcn_ind = 1
                 and prc.period_cd between '{period_cd_from}' and '{period_cd_to}'
                 where 1 = 1
                 and pr.prc_amt < prc.ppp_prc_amt and pr.prc_amt > 0.05
                 and step_cd = 2
               )
               then 'FAILED' else 'PASSED' end
              """)

# COMMAND ----------

evaluate_rule(rule_name='Step 2 Price > 0', 
              rule_desc='The Sub-National price must be greater than 0 for all fcc med and no-med for Step 2',
              rule_number=106,
              rule_query=f"""
              select case when exists (
                 --subnational table
                 select count(*) as cnt
                 from {pcm_ppp_wgt_calc}
                 where ppp_prc_amt <= 0
                 and step_cd = 2
                 and period_cd between '{period_cd_from}' and '{period_cd_to}'
                 having count(1) > 1
              )
              then 'FAILED' else 'PASSED' end
              """)

evaluate_rule(rule_name='Step 2 Price > 0 national', 
              rule_desc='The National price must be greater than 0 for all fcc med and no-med for Step 2',
              rule_number=107,
              rule_query=f"""
              select case when exists ( 
                 --national table
                 select count (*) as cnt
                 from {pcm_ppp_prc}
                 where ppp_prc_amt <= 0
                 and step_cd = 2
                 and period_cd between '{period_cd_from}' and '{period_cd_to}'
                 having count(1) > 1
              )
              then 'FAILED' else 'PASSED' end
              """)

# COMMAND ----------

evaluate_rule(rule_name='Step Duplicate', 
              rule_desc='Step Duplicate',
              rule_number=109,
              rule_query=f"""
              select case when exists (
                select pcmdty_id, geo_lvl_cd, period_cd, step_cd, count(step_cd) 
                from {pcm_ppp_wgt_calc}
                group by pcmdty_id, geo_lvl_cd, period_cd, step_cd
                having count(step_cd) > 1
              )
              then 'FAILED' else 'PASSED' end
              """)

# COMMAND ----------

evaluate_rule(rule_name='Units loaded successfully', 
              rule_desc='Units are compared with a threshold of 40%',
              rule_number=1,
              rule_query=f"""
              select case when exists (
                select sum(sum_units) units
                from {sales_ddd_un}
                where period_cd = {period_cd}
                having sum(sum_units)*1.4 > (select sum(sum_units) from {sales_ddd_un} where period_cd =  left(cast(date_format(dateadd(month, -1, to_date(concat('{period_cd_to}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6))
              )
              then 'PASSED' else 'FAILED' end
              """)

# COMMAND ----------

evaluate_rule(rule_name='Units exists',
              rule_desc='Units must exist in the sales table',
              rule_number=102,
              rule_query=f"""
              select case when exists (
                select period_cd, count(*) as cnt
                from {sales_ddd_un}
                where period_cd between '{period_cd_from}' and '{period_cd_to}'
                group by period_cd
                having count(*) > 1
              )
              then 'PASSED' else 'FAILED' end
              """)

# COMMAND ----------

evaluate_rule(rule_name='Same Prices',
              rule_desc='Prices have to be the same between PCM and NPS',
              rule_number=2,
              rule_query=f"""
              select case when exists (
                select pcmdty_id,
                       cast(round(ppp_prc_amt,2)*100 as int) ppp_prc_amt,
                       b.price,
                       period_cd
                from {pcm_ppp_prc} a
                join {lgy_prices} b on b.fcc = a.pcmdty_id and left(b.price_date, 6) = a.period_cd
                where a.period_cd  between {period_cd_from} and left(cast(date_format(dateadd(month, -1, to_date(concat('{period_cd_to}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6)
                and b.price_type in ('J','C')
                and cast(round(ppp_prc_amt,2)*100 as int) - b.price <> 0
              )
              then 'FAILED' else 'PASSED' end
              """)

# COMMAND ----------


