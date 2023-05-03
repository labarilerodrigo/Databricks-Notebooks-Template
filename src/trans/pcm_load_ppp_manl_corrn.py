# Databricks notebook source
# MAGIC %md
# MAGIC #### Input

# COMMAND ----------

# MAGIC %run ../config/pcm_common

# COMMAND ----------

dbutils.widgets.text('filename', f'manual_corrections')
filename = dbutils.widgets.get('filename') + '.csv'

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETL

# COMMAND ----------

import uuid
from datetime import datetime


# args
filepath = f"{config['pcmppp_input']}/{filename}"
batch_id = uuid.uuid4()
timestamp = datetime.now()
period_60 = spark.sql(f"select left(cast(date_format(dateadd(month, -60, to_date(concat({period_cd_from}, '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6) as period_cd").first().period_cd

#steps

# 10
print(f"Loading dataframe from file: '{filepath}'")
df = spark.read \
          .format("csv") \
          .option("header", "true") \
          .option("inferSchema", "true") \
          .option("mode", "FAILFAST") \
          .load(filepath)
df.createOrReplaceTempView("pcm_ppp_ref_prc_manl_corrn")
print(f"Loaded dataframe from file: '{filepath}' with {df.count()} records")

# check invalid periods on input
assert spark.sql(f"""select count(*) as cnt 
                 from pcm_ppp_ref_prc_manl_corrn 
                 where not rlike(period_cd, '^20[0-9][0-9]((0[1-9])|(1[0-2]))$')""").first().cnt == 0, f"Invalid period(s) found on {filepath}"

# 11
spark.sql(f"""
--Insert data from current manual correction table into historical table
insert into {pcm_ppp_ref_prc_manl_corrn_hist}
select period_cd, pcmdty_id, prc_amt, '{timestamp}' as isrt_ts, '{batch_id}' as batch_id
from {pcm_ppp_ref_prc_manl_corrn}
""")

# 12
print("Truncating source tables")
spark.sql(f"truncate table {pcm_ppp_ref_prc_manl_corrn}")

# 13
spark.sql(f"""
--load new manual correction csv file
insert into {pcm_ppp_ref_prc_manl_corrn}
select period_cd, pcmdty_id, prc_amt from pcm_ppp_ref_prc_manl_corrn
""").show()

try:
    # 14
    spark.sql("""
    create or replace table pcm_ppp_ref_prc_manl_corrn_tmp
    (period_cd string, pcmdty_id int, prc_amt numeric(18,4), error_desc string)
    """)
    
    print("Inserting invalid data into pcm_ppp_ref_prc_manl_corrn_tmp")
    spark.sql(f"""
    --Null FCC
    insert into pcm_ppp_ref_prc_manl_corrn_tmp
    select period_cd, pcmdty_id, prc_amt, 'FCC is NULL' as error_desc
    from {pcm_ppp_ref_prc_manl_corrn}
    where pcmdty_id is null
    """).show()

    spark.sql(f"""
    --Invalid FCC
    insert into pcm_ppp_ref_prc_manl_corrn_tmp
    select period_cd, pcmdty_id, prc_amt, 'FCC is Invalid' as error_desc
    from {pcm_ppp_ref_prc_manl_corrn}
    where (pcmdty_id is NOT NULL) and pcmdty_id not in (select distinct pcmdty_id from {pcm_ppp_ref_dim_prod})
    """).show()

    spark.sql(f"""
    --Duplicated FCC for one period
    with cte_FCC_dup as
        (
        select period_cd, pcmdty_id, count(*) count_pcmdty
        from {pcm_ppp_ref_prc_manl_corrn}
        group by period_cd, pcmdty_id
        )
    insert into pcm_ppp_ref_prc_manl_corrn_tmp
    select a.period_cd, a.pcmdty_id, b.prc_amt, 'Duplicated FCC' as error_desc
    from cte_FCC_dup a
    join {pcm_ppp_ref_prc_manl_corrn} b on a.period_cd = b.period_cd and a.pcmdty_id = b.pcmdty_id
    where count_pcmdty > 1
    """).show()

    spark.sql(f"""
    --Negative prices
    insert into pcm_ppp_ref_prc_manl_corrn_tmp
    select period_cd, pcmdty_id, prc_amt, 'Price is Negative' as error_desc
    from {pcm_ppp_ref_prc_manl_corrn}
    where prc_amt < 0
    """).show()

    spark.sql(f"""
    --Null prices
    insert into pcm_ppp_ref_prc_manl_corrn_tmp
    select period_cd, pcmdty_id, prc_amt, 'Price is Null' as error_desc
    from {pcm_ppp_ref_prc_manl_corrn}
    where prc_amt is NULL
    """).show()

    spark.sql(f"""
    --Invalid Period (Current month to -60 months)
    insert into pcm_ppp_ref_prc_manl_corrn_tmp
    select period_cd, pcmdty_id, prc_amt, 'Period is Invalid' as error_desc
    from {pcm_ppp_ref_prc_manl_corrn}
    where (period_cd is NULL) or
            (period_cd > {period_cd_from}) or
            (period_cd < {period_60})
    """).show()
    
    print("Deleting invalid data from pcm_ppp_ref_prc_manl_corrn")
    spark.sql(f"""
    --delete NULL records from manl_corrn table
    delete from {pcm_ppp_ref_prc_manl_corrn}
    where pcmdty_id is null or prc_amt is null or period_cd is null
    """).show()

    spark.sql(f"""
    --delete invalid records from manl_corrn table
    merge into {pcm_ppp_ref_prc_manl_corrn} p
    using pcm_ppp_ref_prc_manl_corrn_tmp m
    on (p.pcmdty_id = m.pcmdty_id and p.period_cd = m.period_cd and p.prc_amt = m.prc_amt)
    when matched then
    delete
    """).show()
    
    print("Inserting invalid records in pcm_ppp_ref_prc_manl_corrn_error_log")
    spark.sql(f"""
    --Insert the invalid records into error log table
    Insert into {pcm_ppp_ref_prc_manl_corrn_error_log}
    select period_cd, pcmdty_id, prc_amt, error_desc, '{timestamp}' as isrt_ts, '{batch_id}' as batch_id
    from pcm_ppp_ref_prc_manl_corrn_tmp
    """).show()

    # 15
    print("Inserting into pcm_ppp_prc_hist")
    spark.sql(f"""
    --Insert and delete national prices
    insert into {pcm_ppp_prc_hist}
    select p.pcmdty_id,
           p.ppp_prc_amt,
           p.step_cd,
           '{batch_id}' as batch_id,
           '{timestamp}' as isrt_ts,
           null as replmnt_rsn_cd,
           p.period_cd
    from {pcm_ppp_prc} p
    join {pcm_ppp_ref_prc_manl_corrn} m on m.period_cd = p.period_cd and m.pcmdty_id = p.pcmdty_id
    """).show()

    # 16
    print("Merging into pcm_ppp_prc")
    spark.sql(f"""
    merge into {pcm_ppp_prc} p
    using {pcm_ppp_ref_prc_manl_corrn} m
    on (m.period_cd = p.period_cd and m.pcmdty_id = p.pcmdty_id)
    when matched then
    delete
    """).show()

    # 17
    print("Inserting into pcm_ppp_prc")
    spark.sql(f"""
    insert into {pcm_ppp_prc}
    select c.pcmdty_id,
           c.prc_amt,
           50 as step_cd,
           c.period_cd
    from {pcm_ppp_ref_prc_manl_corrn} c
    """).show()

    # 18
    print("Inserting into pcm_ppp_wgt_hist")
    spark.sql(f"""
    insert into {pcm_ppp_wgt_hist}
    select p.pcmdty_id,
           p.geo_lvl_cd,
           p.ppp_prc_amt,
           p.wgt_units_qty,
           p.period_cd,
           p.step_cd,
           p.sales_units_qty,
           '{timestamp}' as isrt_ts,
           '{batch_id}' as batch_id
    from {pcm_ppp_wgt_calc} p
    join {pcm_ppp_ref_prc_manl_corrn} m on m.period_cd = p.period_cd and m.pcmdty_id = p.pcmdty_id
    """).show()

    # 19
    print("Merging into pcm_ppp_wgt_calc")
    spark.sql(f"""
    merge into {pcm_ppp_wgt_calc} p
    using {pcm_ppp_ref_prc_manl_corrn} m
    on (m.period_cd = p.period_cd and m.pcmdty_id = p.pcmdty_id)
    when matched then
    delete
    """).show()

    # 20
    print("Inserting into pcm_ppp_wgt_calc")
    spark.sql(f"""
    with
        cte_states
        as
        (
            select state_abbrev state_cd
            from {pcm_ppp_ref_states}
        ),
        cte_chain
        as
        (
                select 'R' ri
                union all
                select 'I' ri
        ),
        cte_states_ri
        as
        (
            select state_cd, ri, concat(state_cd, ri) geo_lvl_cd
            from cte_states cross join cte_chain
        )
        insert into {pcm_ppp_wgt_calc}
        select c.pcmdty_id,
               ri.geo_lvl_cd,
               c.prc_amt,
               0 as wgt_units_qty,
               50 as step_cd,
               0 as sales_units_qty,
               c.period_cd
        from {pcm_ppp_ref_prc_manl_corrn} c cross join cte_states_ri ri
        left join {pcm_ppp_wgt_calc} w on c.pcmdty_id = w.pcmdty_id and c.period_cd = w.period_cd
        where w.pcmdty_id is null
    """).show()

    # 21
    print("Merging into pcm_ppp_wgt_calc")
    spark.sql(f"""
    merge into {pcm_ppp_wgt_calc} w
    using {pcm_ppp_wgt_hist} h
    on (w.pcmdty_id = h.pcmdty_id and w.period_cd = h.period_cd and w.geo_lvl_cd = h.geo_lvl_cd and h.batch_id = '{batch_id}')
    when matched then
    update set w.wgt_units_qty = h.wgt_units_qty, w.sales_units_qty = h.sales_units_qty
    """).show()
except Exception as e:
    raise e
finally:
    # 22
    spark.sql(f"drop table if exists pcm_ppp_ref_prc_manl_corrn_tmp")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------


