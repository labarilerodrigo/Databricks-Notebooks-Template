# Databricks notebook source
# MAGIC %md
# MAGIC #### Input

# COMMAND ----------

# MAGIC %run ../config/pcm_common

# COMMAND ----------

dbutils.widgets.text('filename', f'pcm_ppp_ref_packs_z1l')
filename = dbutils.widgets.get('filename') + '.csv'

# COMMAND ----------

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
  with cte_z1l_full as (
    select
        pdp.pcmdty_id fcc_id,
        pdp.pack_cd pack_cd,
        cast(round(ppp.ppp_prc_amt, 2) * 100 as int) calcd_price_amt,
        ppp.period_cd pd_dt,
        substring(ppp.period_cd, 3, 2) pd_yr,
        right(ppp.period_cd, 2) pd_mth
    from {pcm_ppp_ref_dim_prod} pdp
    join {pcm_ppp_prc} ppp on ppp.pcmdty_id = pdp.pcmdty_id
	join {pcm_ppp_ref_pack_z1l} z1l on z1l.pack_cd = pdp.pack_cd
    where
		ppp.period_cd = {period_cd_from}
        and ppp.ppp_prc_amt != 0
        and lower(pdp.atc4_cd)  != 'z98a2'
		and pdp.manuf_cd not in ('4581','4580')
	), z1l_cte_file as (
		select distinct
			repeat('0', 7 - length(pack_cd)) || cast(pack_cd as string) ||
			repeat(' ', 1) ||
			'Z1L' ||
			repeat(' ', 51) ||
            repeat('0', 8 - length(calcd_price_amt)) || replace(calcd_price_amt, '.', '' ) ||
			repeat(' ', 6) ||
			'BR' ||
			repeat(' ', 2) z1l
		from cte_z1l_full
	)
select z1l
from z1l_cte_file 
where 
    z1l is not null
""")

## 12
## export full file
# write df to azure blob storage
file_output = f"{config['pcmppp_output']}/Z1L_LPB_{period_cd_from}.txt"
print(f"Recording txt file to '{file_output}' with {df.count()} records")
df.toPandas().to_csv(file_output, header=None, index=False)
print("OK")

## export chunked files
# write df to azure blob storage
def split_dataframe(df, chunk_size = 29500):
    """
    Converts a spark dataframe to pandas and splits it in multiple chunked dataframes.
    """
    df = df.toPandas()
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks

df_chunks = split_dataframe(df)
cnt = 1
for df in df_chunks:
    file_output = f"{config['pcmppp_output']}/Z1L_LPB_{period_cd_from}_{cnt}.txt"
    print(f"Recording txt file to '{file_output}' with {len(df)} records")
    df.to_csv(file_output, header=None, index=False)
    print("OK")
    cnt += 1

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------


