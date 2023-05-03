# Databricks notebook source
# MAGIC %md
# MAGIC #### Input

# COMMAND ----------

# MAGIC %run ../config/pcm_common

# COMMAND ----------

dbutils.widgets.text('filename', f'pcm_ppp_ref_packs_z1l')
filename = dbutils.widgets.get('filename') + '.csv'

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame

cnt = 0
df_list = list()
while cnt < 60:
    df_list.append(spark.sql(f"""select left(cast(date_format(dateadd(month, - {cnt}, to_date(concat('{period_cd_from}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6) as period_cd"""))
    cnt += 1
df_reduced = reduce(DataFrame.unionAll, df_list)
df_reduced.createOrReplaceTempView("tmp_periods_pcards")

## args
    
## steps

## 10
print(f"Loading dataframe from file: '{config['pcmppp_input']}/{filename}'")
df = spark.read \
          .format("csv") \
          .option("header", "true") \
          .option("inferSchema", "true") \
          .option("mode", "FAILFAST") \
          .load(f"{config['pcmppp_input']}/{filename}")
print(f"Loaded dataframe from file: '{config['pcmppp_input']}/{filename}' with {df.count()} records")

df.write.mode("overwrite").saveAsTable(pcm_ppp_ref_pack_z1l)

# 11
df = spark.sql(f"""
        with pcards_source as (
        select distinct
            pdp.pcmdty_id fcc_id,
            pdp.pack_cd pack_cd,
            cast(round(ppp.ppp_prc_amt, 2) * 100 as int) calcd_price_amt,
            ppp.period_cd pd_dt,
            substring(ppp.period_cd, 3, 2) pd_yr,
            right(ppp.period_cd, 2) pd_mth
        from {pcm_ppp_ref_dim_prod} pdp
        join {pcm_ppp_prc} ppp on ppp.pcmdty_id = pdp.pcmdty_id
        join {pcm_ppp_ref_pack_z1l} z1l on z1l.pack_cd = pdp.pack_cd
		join tmp_periods_pcards p on p.period_cd = ppp.period_cd
        where
			ppp.ppp_prc_amt != 0
            and pdp.atc4_cd  != 'Z98A2'
            and pdp.manuf_cd not in ('4581','4580')
        ),
        cte_file as (
	        select
		        'PIMS0000' ||
		        repeat('0', 7 - length(pack_cd)) || cast(pack_cd as string) ||
		        repeat(' ', 1) ||
		        repeat('0', 2 - length(pd_mth)) || pd_mth ||
		        repeat('0', 2 - length(pd_mth)) || pd_yr ||
		        repeat('0', 2 - length(pd_mth)) || pd_mth ||
		        repeat('0', 2 - length(pd_mth)) || pd_yr ||

		        repeat(' ', 2) ||
                repeat('0', 11 - length(calcd_price_amt)) || replace(calcd_price_amt, '.', '' ) ||
		        repeat(' ', 39) ||
		        'BR' ||
		        repeat(' ', 2) pcards
		        from pcards_source
	    )
	    select *
        from cte_file
        where 
            pcards is not null
""")

## 12
## export file
# write df to azure blob storage
file_output = f"{config['pcmppp_output']}/LPB_PCARDS_M{period_cd_from[-2:]}{period_cd_from[2:4]}.txt"
print(f"Recording txt file to '{file_output}' with {df.count()} records")
df.toPandas().to_csv(file_output, header=None, index=False)
print("OK")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------


