# Databricks notebook source
from datetime import datetime
from dateutil.relativedelta import relativedelta

ppp_prc_period_from = dbutils.widgets.get('ppp_prc_period_from')
ppp_prc_period_to = dbutils.widgets.get('ppp_prc_period_to')
step_ppp_prc_period_from = dbutils.widgets.get('step_ppp_prc_period_from')
step_ppp_prc_period_to = dbutils.widgets.get('step_ppp_prc_period_to')
    
def date_fn(period_cd_from, period_cd_to):
    date_from = datetime.date(datetime.strptime(str(period_cd_from)+'01', '%Y%m%d'))
    date_to = datetime.date(datetime.strptime(str(period_cd_to)+'01', '%Y%m%d'))
    max_period = spark.sql(f"select cast(months_between('{date_to}', '{date_from}') as int) as period_cd").first().period_cd
    cnt = 0
    date_list = ''
    while cnt <= max_period:
        count_date = date_from + relativedelta(months=cnt)
        separator = '' if date_list == '' else ','
        date_list = date_list + separator + count_date.strftime('%Y%m')
        cnt += 1
    return date_list

def export_file(name, df):
    output_folder = "/dbfs/mnt/bdf/development/br9/data/pcmppp/output"
    file_output = f"{output_folder}/{ppp_prc_period_to}_{name}-total_rows_{df.count()}.csv"
    print(f"Recording txt file to '{file_output}' with {df.count()} records")
    df.toPandas().to_csv(file_output, header=True, index=False)
    print("OK")

ppp_prc_all_periods = date_fn(ppp_prc_period_from, ppp_prc_period_to)
print(ppp_prc_all_periods)

step_ppp_prc_all_periods = date_fn(step_ppp_prc_period_from, step_ppp_prc_period_to)
print(step_ppp_prc_all_periods)

# 10

#.option('url', 'jdbc:sqlserver://azwlargqcsql02p.production.imsglobal.com;database=FF_TEMP')\
query = f"""(select
                FCC
             from FF_TEMP.dbo.FV_REPORT_PACK_UN_VAL_PPP
             ) q"""
df = spark.read.format('jdbc')\
               .option('url', 'jdbc:sqlserver://cdtssql986p.production.imsglobal.com;database=FF_TEMP')\
               .option('driver','com.microsoft.sqlserver.jdbc.SQLServerDriver')\
               .option('user', 'pcmqcusr')\
               .option('password', 'Pcmqcuser1')\
               .option('dbtable', query)\
               .load()

df.createOrReplaceTempView('src_fcc_flexview_tmp')

# # PPP PRC Pivot 
df1 = spark.sql(f"""
select * from ( -- pack PPP
	select a.pcmdty_id as FCC, a.ppp_prc_amt, a.period_cd
	from devl_br9_pcm_ppp.pcm_ppp_prc a
    inner join src_fcc_flexview_tmp b on a.pcmdty_id = b.fcc
    where a.period_cd between '{ppp_prc_period_from}' and '{ppp_prc_period_to}'
)
PIVOT
(
	avg(ppp_prc_amt) as ppp_prc_amt
	for period_cd in (
        {ppp_prc_all_periods}
	)
)
order by FCC
""")

# PCM PPP by Step Pivot
df2 = spark.sql(f"""
select * from( -- step
    select a.pcmdty_id as FCC, a.period_cd, a.step_cd
    from devl_br9_pcm_ppp.pcm_ppp_prc a
    inner join src_fcc_flexview_tmp b on a.pcmdty_id = b.fcc
    left join devl_br9_pcm_ppp.pcm_ppp_ref_dim_ext_prc_l c on b.fcc = c.pcmdty_id
    and a.period_cd = c.period_cd
    where a.period_cd between '{step_ppp_prc_period_from}' and '{step_ppp_prc_period_to}'
)
pivot(
    avg(step_cd)
    for period_cd in ({step_ppp_prc_all_periods})
    )
order by FCC
""")

# All pcm ppp periods
df3 = spark.sql(f"""
select distinct period_cd 
from devl_br9_pcm_ppp.pcm_ppp_prc
order by period_cd
""")

# pcm ppp dim product
df4 = spark.sql(f"""
select a.* 
from devl_br9_pcm_ppp.pcm_ppp_ref_dim_prod a
inner join src_fcc_flexview_tmp b on a.pcmdty_id = b.fcc
""")

export_file('pivot_pcm_ppp_prc', df1)
export_file('pivot_pcm_ppp_fcc_by_step', df2)
export_file('pcm_ppp_all_periods', df3)
export_file('pcm_ppp_dim_product', df4)

