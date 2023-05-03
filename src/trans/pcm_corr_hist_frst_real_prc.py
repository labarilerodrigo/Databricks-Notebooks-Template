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
from datetime import datetime

min_period_cd = '202101'
timestamp = datetime.now()

## steps
# 10
spark.sql("drop view if exists pcmdty_step_1_cur_per")
spark.sql(f"""
--pcmdty_id step=1 in current period
cache table pcmdty_step_1_cur_per
select 
	distinct p.pcmdty_id, 
	if(l.prc_amt is not null, (p.ppp_prc_amt/ l.prc_amt - 1) * 100, 0) med_pct,
	p.ppp_prc_amt,
	l.prc_amt l_prc
from {pcm_ppp_prc} p
inner join {pcm_ppp_wgt_calc} wgt on p.pcmdty_id = wgt.pcmdty_id and p.period_cd = wgt.period_cd
left join {pcm_ppp_ref_dim_ext_prc_l} l on p.pcmdty_id = l.pcmdty_id and p.period_cd = l.period_cd
where p.step_cd = 1
and p.period_cd = {period_cd_from}
and wgt.wgt_units_qty > 0
""")

# 11.1
spark.sql("drop view if exists step_52_fltr_1")
spark.sql(f"""
--get min period with transactions in weighting
cache table step_52_fltr_1
select
    wgt.pcmdty_id,
    max(wgt.period_cd) period_cd,
    sum(wgt.wgt_units_qty) wgt_units_qty 
from {pcm_ppp_wgt_calc} wgt
join pcmdty_step_1_cur_per c on wgt.pcmdty_id = c.pcmdty_id and wgt.period_cd between {min_period_cd} and left(cast(date_format(dateadd(month, -1, to_date(concat({period_cd_from}, '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6)
where 
 --wgt.period_cd between {min_period_cd} and left(cast(date_format(dateadd(month, -1, to_date(concat({period_cd_from}, '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6)
    -- old definition
        --and step_cd in (1, 2, 50, 51)
        --and wgt.wgt_units_qty > 0
    -- CR10.1: Do not apply step 52 id we have manual correctios defined. New defined rule:
      (step_cd in (1, 2, 51) and wgt.wgt_units_qty > 0) or step_cd = 50
group by wgt.pcmdty_id
""")

# 11.2
spark.sql("drop view if exists pcmdty_to_work")
spark.sql(f"""
--get min period with transactions in weighting
cache table pcmdty_to_work
select
        pr.period_cd,
        pr.pcmdty_id,
        p.wgt_units_qty 
    from {pcm_ppp_prc} pr
    join step_52_fltr_1 p on pr.pcmdty_id = p.pcmdty_id and pr.period_cd between left(cast(date_format(dateadd(month, +1, to_date(concat(p.period_cd, '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6)and left(cast(date_format(dateadd(month, -1, to_date(concat({period_cd_from}, '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6)
""")

# 11.3.log => step_id=1 => step_cd= 50 and wgt_units_qty <= 0
spark.sql(f"""delete from {pcm_ppp_hist_step_52} where step_typ_id = 1 and prod_period_cd = '{period_cd_to}'""")
spark.sql(f"""
insert into {pcm_ppp_hist_step_52}
select '1' step_typ_id, pr.pcmdty_id, pr.step_cd, null prev_price, pr.ppp_prc_amt, c.wgt_units_qty, '{timestamp}', '{period_cd_to}', pr.period_cd
from {pcm_ppp_prc} pr
join step_52_fltr_1 c on pr.pcmdty_id = c.pcmdty_id and pr.period_cd = c.period_cd
where 
    --wgt.period_cd between {min_period_cd} and left(cast(date_format(dateadd(month, -1, to_date(concat({period_cd_from}, '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6)
    pr.step_cd = 50 and c.wgt_units_qty <= 0
""").show()

# 11.4
spark.sql("drop view if exists pcmdty_to_work_cr10_2")
spark.sql(f"""
cache table pcmdty_to_work_cr10_2
select 
    pr.pcmdty_id, 
    pr.step_cd,
    pr.period_cd,
    pr.ppp_prc_amt,
    lag(pr.ppp_prc_amt,1) over(partition by pr.pcmdty_id order by pr.period_cd) prev_price,
    abs(pr.ppp_prc_amt/ lag(pr.ppp_prc_amt,1) over(partition by pr.pcmdty_id order by pr.period_cd)-1) `abs_price_+/-15`,
    if(abs(pr.ppp_prc_amt/ lag(pr.ppp_prc_amt,1) over(partition by pr.pcmdty_id order by pr.period_cd)-1) > 0.15, 1, 0) `abs_price_+/-15_ind`,
    wgt_units_qty
from {pcm_ppp_prc} pr
join pcmdty_to_work p on pr.pcmdty_id = p.pcmdty_id and pr.period_cd = p.period_cd
    --and pr.period_cd between left(cast(date_format(dateadd(month, -1, to_date(concat(p.period_cd, '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6) and p.period_cd
""")

# 11.5 - log => step_typ_id=2 => abs_price_+/-15
spark.sql(f"""delete from {pcm_ppp_hist_step_52} where step_typ_id = 2 and prod_period_cd ='{period_cd_to}'""")
spark.sql(f"""
insert into {pcm_ppp_hist_step_52}
select '2' step_typ_id, pcmdty_id, step_cd, prev_price, ppp_prc_amt, wgt_units_qty, '{timestamp}', '{period_cd_to}', period_cd
from pcmdty_to_work_cr10_2
where `abs_price_+/-15_ind` = 1
""")

# 12
spark.sql("drop view if exists all_pcmdty")
spark.sql(f"""
--get all pcmdty_ids valid
cache table all_pcmdty
with all_pcmdty_cte as (
    select * 
    from {pcm_ppp_prc} prc
    where period_cd between {min_period_cd} and left(cast(date_format(dateadd(month, -1, to_date(concat('{period_cd_from}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6)
    and pcmdty_id in (select pcmdty_id from pcmdty_step_1_cur_per)
)
    select a.pcmdty_id, a.period_cd 
    from all_pcmdty_cte a
    join pcmdty_to_work_cr10_2 w on a.pcmdty_id = w.pcmdty_id and a.period_cd = w.period_cd
    where w.`abs_price_+/-15_ind` = 0 -- take only price variations below 15%
""")

# 13
spark.sql("drop view if exists all_prc")
spark.sql(f"""
cache table all_prc
with work_cte as (
	select 
		p.period_cd,
		cur.med_pct cur_prc_pct,
		cur.ppp_prc_amt cur_ppp_prc,
		cur.l_prc cur_l_prc,
		l.prc_amt l_prc,
		p.pcmdty_id,
		p.ppp_prc_amt cur_ppp_prc_amt,
		if(dp.pck_is_medcn_ind = 1, cast((l.prc_amt + (l.prc_amt * cur.med_pct)/100) as decimal(18,4)), 0)  med_prc,
		cur.ppp_prc_amt no_med_prc,
		dp.pck_is_medcn_ind
	from {pcm_ppp_prc} p
	join all_pcmdty c on p.pcmdty_id = c.pcmdty_id and p.period_cd = c.period_cd
	join pcmdty_step_1_cur_per cur on c.pcmdty_id = cur.pcmdty_id
	join {pcm_ppp_ref_dim_prod} dp on cur.pcmdty_id = dp.pcmdty_id
	left join {pcm_ppp_ref_dim_ext_prc_l} l on p.pcmdty_id = l.pcmdty_id and p.period_cd = l.period_cd
), med as (
	select 
		period_cd,
		pcmdty_id,
		med_prc ppp_prc,
		pck_is_medcn_ind
	from work_cte
	where pck_is_medcn_ind = 1
), non_med as (
	select
		period_cd,
		pcmdty_id,
		no_med_prc ppp_prc,
		pck_is_medcn_ind
	from work_cte
	where pck_is_medcn_ind = 0
), full_pck as (
	select period_cd, pcmdty_id, ppp_prc, pck_is_medcn_ind
	from (
		select 
			period_cd,
			pcmdty_id,
			ppp_prc,
			pck_is_medcn_ind
		from med
		union all
		select
			period_cd,
			pcmdty_id,
			ppp_prc,
			pck_is_medcn_ind
		from non_med
	) prc
)
	select period_cd, pcmdty_id, ppp_prc,pck_is_medcn_ind
	from full_pck
""")

# 14
spark.sql(f"""
merge into {pcm_ppp_prc} pb
using all_prc c
on (c.pcmdty_id = pb.pcmdty_id and pb.period_cd = c.period_cd)
when matched then
update set pb.ppp_prc_amt = c.ppp_prc, pb.step_cd = 52
""").show()

# 15
spark.sql(f"""
-- sub-national update
merge into {pcm_ppp_wgt_calc} w
using all_prc c
on (c.pcmdty_id = w.pcmdty_id and w.period_cd = c.period_cd)
when matched then
update set w.ppp_prc_amt = c.ppp_prc, step_cd = 52
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation
