# Databricks notebook source
# MAGIC %md
# MAGIC #### Input

# COMMAND ----------

# MAGIC %run ../config/pcm_common

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETL

# COMMAND ----------

## args 202001
initial_period = '202112'
current_period = period_cd_from

## steps
# 10
nst1m_query = f"(select period, whs, fcc, outlet, units from dbo.vNST1_DBR with (nolock) where period = '{current_period}') q"
df_nst1 = spark.read.format('jdbc')\
               .option('url', 'jdbc:sqlserver://azwlaappsql01P.production.imsglobal.com;database=IDCDataMart;encrypt=false')\
               .option('driver','com.microsoft.sqlserver.jdbc.SQLServerDriver')\
               .option('user', 'pcmpusr')\
               .option('password', 'Pcmuser1')\
               .option('dbtable', nst1m_query)\
               .load()

df_nst1.createOrReplaceTempView('apps_nst1m')

# 11
spark.sql(f"delete from {sales_ddd_un} where period_cd = '{current_period}'")

# 12
spark.sql(f"""
insert into {sales_ddd_un}
(period_cd, fcc, wholesaler_code, outlet_code, sum_units)
select un.period as period_cd, m.pcmdty_id_new, un.whs as wholesaler_code, un.outlet as outlet_code, cast(sum(un.units) as int) as sum_units
from apps_nst1m un   
join {vw_rds_oac_dim_outlet} o on cast(o.outlet_code - 1000000 as int)  = un.outlet  
join {vw_rds_pcmdty_id_merge} m on m.pcmdty_id_old = un.fcc
where 1 = 1
    and right(o.outlet_type,1) = '1' 
    and un.period = '{current_period}'   
group by un.period, m.pcmdty_id_new, un.whs, un.outlet
having sum(un.units) > 0
""").show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation
