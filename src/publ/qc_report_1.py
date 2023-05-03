# Databricks notebook source
# MAGIC %md
# MAGIC #### Input

# COMMAND ----------

# MAGIC %run ../config/pcm_common

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETL

# COMMAND ----------

###############
## ref_price ##
###############


## steps

# 10
spark.sql("drop view if exists tmp_cte_buys_trans")
spark.sql(f"""
cache table tmp_cte_buys_trans
-- ntrnas
select * 
from (
	select f.* ,fa.bridged, ff.mapped --, fff.ntrans, ffff.ntrans
	from (
		select a.dsupp_proc_id, b.ppp_dsupp_typ_cd, b.wholesaler_description, c.pck_is_medcn_ind, count(*) layout
		from {pcm_ppp_buys_trans} a
		left join {pcm_ppp_ref_dim_whs} b on (a.dsupp_proc_id = b.wholesaler_code)
		left join {pcm_ppp_ref_dim_prod} c on (a.pcmdty_id = c.pcmdty_id)
		where a.period_cd = '{period_cd_from}'
		group by a.dsupp_proc_id, b.ppp_dsupp_typ_cd, b.wholesaler_description, c.pck_is_medcn_ind
	) f
	left join (
	select a.dsupp_proc_id, b.ppp_dsupp_typ_cd, c.pck_is_medcn_ind, count(*) bridged
	from {pcm_ppp_buys_trans} a
	left join {pcm_ppp_ref_dim_whs} b on (a.dsupp_proc_id = b.wholesaler_code)
	left join {pcm_ppp_ref_dim_prod} c on (a.pcmdty_id = c.pcmdty_id)
	where a.period_cd = '{period_cd_from}'
		and prod_brdg_stat_cd = 'Bridged-ok'
	group by a.dsupp_proc_id, b.ppp_dsupp_typ_cd, b.wholesaler_description, c.pck_is_medcn_ind
	) fa on (f.dsupp_proc_id = fa.dsupp_proc_id and f.pck_is_medcn_ind = fa.pck_is_medcn_ind)
	left join (
	select a.dsupp_proc_id, b.ppp_dsupp_typ_cd, b.wholesaler_description,a.pck_is_medcn_ind, count(*) mapped
	from {pcm_ppp_buys_trans_fltr_1} a
	left join {pcm_ppp_ref_dim_whs} b on (a.dsupp_proc_id = b.wholesaler_code)
	where a.period_cd = '{period_cd_from}'
	group by a.dsupp_proc_id, b.ppp_dsupp_typ_cd, b.wholesaler_description, a.pck_is_medcn_ind
	) ff on (f.dsupp_proc_id = ff.dsupp_proc_id and f.pck_is_medcn_ind = ff.pck_is_medcn_ind)
) as cte_buys_trans
where pck_is_medcn_ind is not null
order by 1,4
""")

# 11
spark.sql("drop view if exists tmp_tax_00")
spark.sql(f"""
cache table tmp_tax_00
-- tax
select a.dsupp_proc_id, b.ppp_dsupp_typ_cd, b.wholesaler_description, a.pck_is_medcn_ind, count(*) tax
from {pcm_ppp_tax_calc} a
left join {pcm_ppp_ref_dim_whs} b on (a.dsupp_proc_id = b.wholesaler_code)
where a.period_cd = '{period_cd_from}'
and  a.pck_is_medcn_ind = 0
group by a.dsupp_proc_id, b.ppp_dsupp_typ_cd, b.wholesaler_description, a.pck_is_medcn_ind
order by 1,4
""")

# 12
spark.sql("drop view if exists tmp_tax_01")
spark.sql(f"""
cache table tmp_tax_01
select a.dsupp_proc_id, b.ppp_dsupp_typ_cd, b.wholesaler_description, a.pck_is_medcn_ind, count(*) tax
from {pcm_ppp_tax_calc} a
left join {pcm_ppp_ref_dim_whs} b on (a.dsupp_proc_id = b.wholesaler_code)
where a.period_cd = '{period_cd_from}'
and  a.pck_is_medcn_ind =1
group by a.dsupp_proc_id, b.ppp_dsupp_typ_cd, b.wholesaler_description, a.pck_is_medcn_ind
order by 1,4
""")

# 13
spark.sql("drop view if exists tmp_out_00")
spark.sql(f"""
cache table tmp_out_00
-- out
select a.dsupp_proc_id, b.ppp_dsupp_typ_cd, b.wholesaler_description, a.pck_is_medcn_ind, count(*) out
from {pcm_ppp_tax_calc_fltr_1} a
left join {pcm_ppp_ref_dim_whs} b on (a.dsupp_proc_id = b.wholesaler_code)
where a.period_cd = '{period_cd_from}'
and  a.pck_is_medcn_ind = 0
group by a.dsupp_proc_id, b.ppp_dsupp_typ_cd, b.wholesaler_description, a.pck_is_medcn_ind
order by 1,4
""")

# 14
spark.sql("drop view if exists tmp_out_01")
spark.sql(f"""
cache table tmp_out_01
select a.dsupp_proc_id, b.ppp_dsupp_typ_cd, b.wholesaler_description, a.pck_is_medcn_ind, count(*) out
from {pcm_ppp_tax_calc_fltr_1} a
left join {pcm_ppp_ref_dim_whs} b on (a.dsupp_proc_id = b.wholesaler_code)
where a.period_cd = '{period_cd_from}'
and  a.pck_is_medcn_ind = 1
group by a.dsupp_proc_id, b.ppp_dsupp_typ_cd, b.wholesaler_description, a.pck_is_medcn_ind
order by 1,4
""")

# 15
spark.sql("drop view if exists tmp_cte_provider_rank")
spark.sql(f"""
cache table tmp_cte_provider_rank
-- remove transactions from distributor providers what is already sent by pharmacies providers
select buys_trans_line_id, dsupp_proc_id, trans_dt, buyer_is_pharmy_ind, seller_otlt_cd, buyer_otlt_cd, buyer_is_otlt_chain_ind, pcmdty_id, ppp_prc_amt, period_cd,
	buyer_st_cd, dsupp_is_distr_ind, pck_is_medcn_ind, dense_rank() over (partition by buyer_otlt_cd, pcmdty_id, trans_dt order by dsupp_is_distr_ind) row_num
from {pcm_ppp_tax_calc}
where period_cd = '{period_cd_from}'
""")

# 16
spark.sql("drop view if exists tmp_cte_tax_calc")
spark.sql(f"""
cache table tmp_cte_tax_calc
select buys_trans_line_id, dsupp_proc_id, trans_dt, seller_otlt_cd, buyer_otlt_cd, buyer_is_otlt_chain_ind, pcmdty_id, ppp_prc_amt, period_cd,
buyer_st_cd, buyer_is_pharmy_ind, pck_is_medcn_ind
from tmp_cte_provider_rank
where row_num = 1
""")

# 17
spark.sql("drop view if exists tmp_dupp_00")
spark.sql(f"""
cache table tmp_dupp_00
select a.dsupp_proc_id, b.ppp_dsupp_typ_cd, b.wholesaler_description, a.pck_is_medcn_ind, count(*) dupp
from tmp_cte_tax_calc a
left join {pcm_ppp_ref_dim_whs} b on (a.dsupp_proc_id = b.wholesaler_code)
where period_cd = '{period_cd_from}'
and  a.pck_is_medcn_ind = 0
group by a.dsupp_proc_id, b.ppp_dsupp_typ_cd, b.wholesaler_description, a.pck_is_medcn_ind
order by 1,4
""")

# 18
spark.sql("drop view if exists tmp_dupp_01")
spark.sql(f"""
cache table tmp_dupp_01
select a.dsupp_proc_id, b.ppp_dsupp_typ_cd, b.wholesaler_description, a.pck_is_medcn_ind, count(*) dupp
from tmp_cte_tax_calc a
left join {pcm_ppp_ref_dim_whs} b on (a.dsupp_proc_id = b.wholesaler_code)
where period_cd = '{period_cd_from}'
and  a.pck_is_medcn_ind = 1
group by a.dsupp_proc_id, b.ppp_dsupp_typ_cd, b.wholesaler_description, a.pck_is_medcn_ind
order by 1,4
""")

# 19
spark.sql("drop view if exists tmp_cte_cpp_price")
spark.sql(f"""
cache table tmp_cte_cpp_price
-- cpp		
select a.*, ifnull(b.prc_amt, 0.00) as cpp_price
from tmp_cte_tax_calc a
left join {pcm_ppp_ref_dim_ext_prc_d} b on a.pcmdty_id = b.pcmdty_id and left(cast(date_format(dateadd(month, -1, to_date(concat(a.period_cd, '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6) = b.period_cd
""")

# 20
spark.sql("drop view if exists tmp_cte_cpp_outlier")
spark.sql(f"""
cache table tmp_cte_cpp_outlier
select a.*,
  if(a.cpp_price > 0.05,
    if(a.ppp_prc_amt> (a.cpp_price*b.cpp_min_pct/100) and a.ppp_prc_amt< (a.cpp_price*b.cpp_max_pct/100),
       0,
       1),
    0)as outlier_ind
from tmp_cte_cpp_price a
cross join {pcm_ppp_cpp_min_max_pct} b
""")

# 21
spark.sql("drop view if exists tmp_cpp_00")
spark.sql(f"""
cache table tmp_cpp_00
select a.dsupp_proc_id, b.ppp_dsupp_typ_cd, b.wholesaler_description, a.pck_is_medcn_ind, count(*) cpp
from tmp_cte_cpp_outlier a
left join {pcm_ppp_ref_dim_whs} b on (a.dsupp_proc_id = b.wholesaler_code)
where period_cd = '{period_cd_from}'
	and outlier_ind = 0
	and a.pck_is_medcn_ind = 0
group by a.dsupp_proc_id, b.ppp_dsupp_typ_cd, b.wholesaler_description, a.pck_is_medcn_ind
order by 1,4
""")

# 22
spark.sql("drop view if exists tmp_cpp_01")
spark.sql(f"""
cache table tmp_cpp_01
select a.dsupp_proc_id, b.ppp_dsupp_typ_cd, b.wholesaler_description, a.pck_is_medcn_ind, count(*) cpp
from tmp_cte_cpp_outlier a
left join {pcm_ppp_ref_dim_whs} b on (a.dsupp_proc_id = b.wholesaler_code)
where period_cd = '{period_cd_from}'
	and outlier_ind= 0
	and a.pck_is_medcn_ind =1
group by a.dsupp_proc_id, b.ppp_dsupp_typ_cd, b.wholesaler_description, a.pck_is_medcn_ind
order by 1,4
""")

# 23
spark.sql("drop view if exists tmp_ref_providers")
spark.sql(f"""
cache table tmp_ref_providers
select
	dsupp_proc_id,
	ppp_dsupp_typ_cd,
	wholesaler_description,
	pck_is_medcn_ind,
	layout,
	bridged,
	mapped,tax,
	dupp,
	cpp,
	`out`,
	(bridged/layout)-1 as diff1_bridge,
	(mapped/bridged)-1 as diff2_list_price
from (
	select 
        buy.dsupp_proc_id,
        buy.ppp_dsupp_typ_cd,
        buy.wholesaler_description,
        buy.pck_is_medcn_ind,
        buy.layout,
        buy.bridged,
        buy.mapped,
        tax.tax,
        dupp.dupp,
        cpp.cpp,
        out0.`out`
	from tmp_cte_buys_trans buy
	left join tmp_tax_00 tax
	on tax.dsupp_proc_id = buy.dsupp_proc_id
	left join tmp_out_00 out0
	on out0.dsupp_proc_id = buy.dsupp_proc_id
	left join tmp_dupp_00 dupp
	on dupp.dsupp_proc_id = buy.dsupp_proc_id
	left join tmp_cpp_00 cpp
	on cpp.dsupp_proc_id = buy.dsupp_proc_id 
	where buy.pck_is_medcn_ind = 0
	union
	select 
        buy.dsupp_proc_id,
        buy.ppp_dsupp_typ_cd,
        buy.wholesaler_description,
        buy.pck_is_medcn_ind,
        buy.layout,
        buy.bridged,
        buy.mapped,
        tax.tax,
        dupp.dupp,
        cpp.cpp,
        out0.`out`
	from tmp_cte_buys_trans buy
	left join tmp_tax_00 tax
	on tax.dsupp_proc_id = buy.dsupp_proc_id
	left join tmp_out_00 out0
	on out0.dsupp_proc_id = buy.dsupp_proc_id
	left join tmp_dupp_00 dupp
	on dupp.dsupp_proc_id = buy.dsupp_proc_id
	left join tmp_cpp_00 cpp
	on cpp.dsupp_proc_id = buy.dsupp_proc_id 
	where buy.pck_is_medcn_ind = 1
) as diff
order by pck_is_medcn_ind,dsupp_proc_id
""")

# 24
spark.sql("drop view if exists tmp_ref_price")
spark.sql(f"""
cache table tmp_ref_price
-- med
with 
cte_median_prices_med as (
	select 
		tax.dsupp_proc_id, 
		tax.pcmdty_id, 
        tax.period_cd, 
		percentile_cont(0.5) within group (order by tax.ppp_prc_amt) over (partition by tax.dsupp_proc_id, tax.pcmdty_id, tax.period_cd) as mediancont
	from {pcm_ppp_tax_calc_fltr_1} tax
    left join tmp_ref_providers prov on tax.dsupp_proc_id = prov.dsupp_proc_id
	where tax.pck_is_medcn_ind = 1 
	order by tax.dsupp_proc_id,tax.pcmdty_id,tax.period_cd desc
),
-- nomed
cte_median_prices_nomed as (
	select 
		tax.dsupp_proc_id, 
		tax.pcmdty_id, 
		tax.period_cd, 
		percentile_cont(0.5) within group (order by tax.ppp_prc_amt) over (partition by tax.dsupp_proc_id, tax.pcmdty_id, tax.period_cd) as mediancont
	from {pcm_ppp_tax_calc_fltr_1} tax
    left join tmp_ref_providers prov on tax.dsupp_proc_id = prov.dsupp_proc_id
	where tax.pck_is_medcn_ind = 0
	order by tax.dsupp_proc_id,tax.pcmdty_id,tax.period_cd desc
)
select
	a.pcmdty_id, 
	a.period_cd, 
	cast(avg(a.mediancont) as decimal(38,2)) ref_price
from (
	select *
	from cte_median_prices_med
	where period_cd = '{period_cd_from}'
	group by dsupp_proc_id, pcmdty_id,period_cd, mediancont
) a
group by a.pcmdty_id, a.period_cd
union
select 
	a.pcmdty_id, 
	a.period_cd, 
	cast(avg(a.mediancont) as decimal(38,2)) ref_price
from (
	select *
	from cte_median_prices_nomed
	where period_cd = '{period_cd_from}'
	group by dsupp_proc_id, pcmdty_id,period_cd, mediancont
) a
group by a.pcmdty_id, a.period_cd
""")

# COMMAND ----------

## PRICECH-3448

## args
from datetime import datetime
from dateutil.relativedelta import relativedelta

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

def date_fn_2(period_cd_from, period_cd_to, alias):
    date_from = datetime.date(datetime.strptime(str(period_cd_from)+'01', '%Y%m%d'))
    date_to = datetime.date(datetime.strptime(str(period_cd_to)+'01', '%Y%m%d'))
    max_period = spark.sql(f"select cast(months_between('{date_to}', '{date_from}') as int) as period_cd").first().period_cd
    cnt = 0
    date_list = ''
    while cnt <= max_period:
        count_date = date_from + relativedelta(months=cnt)
        separator = '' if date_list == '' else ','
        date_list = date_list + separator + 'coalesce('+alias+'.'+'`'+count_date.strftime('%Y%m')+'`, 0) as '+count_date.strftime('%Y%m')+'_'+alias
        cnt += 1
    return date_list

period_cd_from = spark.sql(f"select left(cast(date_format(dateadd(month, -30, to_date(concat('{period_cd_to}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6) as period_cd").first().period_cd
last_2_mth = spark.sql(f"select left(cast(date_format(dateadd(month, -1, to_date(concat('{period_cd_to}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6) as period_cd").first().period_cd
last_3_mth = spark.sql(f"select left(cast(date_format(dateadd(month, -2, to_date(concat('{period_cd_to}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6) as period_cd").first().period_cd
last_4_mth = spark.sql(f"select left(cast(date_format(dateadd(month, -3, to_date(concat('{period_cd_to}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6) as period_cd").first().period_cd
last_5_mth = spark.sql(f"select left(cast(date_format(dateadd(month, -4, to_date(concat('{period_cd_to}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6) as period_cd").first().period_cd

mth = date_fn(period_cd_from, period_cd_to)
l_3_mth = date_fn(last_3_mth, period_cd_to)

# print
s_ppp = date_fn_2(period_cd_from, period_cd_to, 'PPP')
s_step = date_fn_2(last_3_mth, period_cd_to, 'STEP')
s_l = date_fn_2(last_3_mth, period_cd_to, 'PF')
s_disc = date_fn_2(last_3_mth, period_cd_to, 'DESC')


## step
#10
df = spark.sql(f"""
    with prev_mth_units as (
        select
            wgt.pcmdty_id,
            sum(wgt.sales_units_qty) total_prev_sales_units
        from {pcm_ppp_wgt_calc} wgt
        where wgt.period_cd = {last_2_mth}
        group by wgt.pcmdty_id
    ),    
    cur_units_mth as (
        select
            prd.pcmdty_id,
            prd.pri_org_short_nm,
            prd.pri_org_nm,
            prd.stdiz_pcmdty_nm,
            prd.gmrs_cd,
            prd.atc1_cd,
            prd.atc1_nm,
            ea.pck_mkt_seg_class_3 seg_mkt_3,
            prc.ppp_prc_amt,
            pu.total_prev_sales_units,
            pu.total_prev_sales_units * prc.ppp_prc_amt sales_fat
        from {pcm_ppp_prc} prc
        join {pcm_ppp_wgt_calc} wgt on prc.pcmdty_id = wgt.pcmdty_id and prc.period_cd = wgt.period_cd
        join prev_mth_units pu on wgt.pcmdty_id = pu.pcmdty_id
        join {vw_rds_product_dim} prd on wgt.pcmdty_id = prd.pcmdty_id
        join {vw_rds_product_ext_attr} ea on prd.pcmdty_id = ea.pcmdty_id
        where prc.period_cd = '{period_cd_to}'
        group by 
                prd.pcmdty_id,
                prd.pri_org_short_nm,
                prd.pri_org_nm,
                prd.stdiz_pcmdty_nm,
                prd.gmrs_cd,
                prd.atc4_cd,
                prc.pcmdty_id,
                prd.atc1_cd,
                prd.atc1_nm,
                ea.pck_mkt_seg_class_3,
                pu.total_prev_sales_units,
                prc.ppp_prc_amt
    ),
    ppp_his_prc as (
            select
                prc.pcmdty_id,
                prc.period_cd,
                prc.ppp_prc_amt
            from {pcm_ppp_prc} prc
            where prc.period_cd between '{period_cd_from}' and '{period_cd_to}'
            --and pcmdty_id = 2
    ),
    pivot_ppp_his_prc as (
        select *        
        from (
            select
                prc.pcmdty_id,
                prc.period_cd,
                prc.ppp_prc_amt
            from ppp_his_prc prc
        ) pivot(
            round(avg(ppp_prc_amt), 4) ppp_prc_amt
            for period_cd in ({mth})
        )
    ),
    l_prc_last_3_mth as (
        select * from (
            select
                l.period_cd,
                l.pcmdty_id,
                l.prc_amt
            from {pcm_ppp_ref_dim_ext_prc_l} l
            where l.period_cd between '{last_3_mth}' and '{period_cd_to}'
        ) pivot(
            round(avg(prc_amt), 4)
            for period_cd in ({l_3_mth})
        )
    ),
    l_disc as (
        select * from (
            select
                l.period_cd,
                l.pcmdty_id,
                (prc.ppp_prc_amt / l.prc_amt -1) * 100 disc
            from {pcm_ppp_ref_dim_ext_prc_l} l
            join ppp_his_prc prc on l.pcmdty_id = prc.pcmdty_id and l.period_cd = prc.period_cd
            where l.period_cd between '{last_3_mth}' and '{period_cd_to}'
        ) pivot(
            round(avg(disc), 0)
            for period_cd in ({l_3_mth})
        )
    ),
    last_3_steps as(
        select * from (
            select
                prc.pcmdty_id,
                prc.period_cd,
                prc.step_cd
            from {pcm_ppp_prc} prc
            where prc.period_cd between '{last_5_mth}' and '{period_cd_to}'
        )pivot(
            cast(avg(step_cd) as int)
            for period_cd in ({l_3_mth})
        )
    ),
    diff_1 as (
        select
            prc.pcmdty_id,
            coalesce((prc.`{last_3_mth}` / prc.`{last_4_mth}`) -1, 0) {last_3_mth}_diff,
            coalesce((prc.`{last_2_mth}` / prc.`{last_3_mth}`) -1, 0) {last_2_mth}_diff,
            coalesce((prc.`{period_cd_to}` / prc.`{last_2_mth}`) -1, 0) {period_cd_to}_diff
        from pivot_ppp_his_prc prc
    ),
    diff_2 as(
        select
            prc.pcmdty_id,
            coalesce((prc.`{last_3_mth}` / prc.`{last_5_mth}`) -1, 0) {last_3_mth}_diff_2,
            coalesce((prc.`{last_2_mth}` / prc.`{last_4_mth}`) -1, 0) {last_2_mth}_diff_2,
            coalesce((prc.`{period_cd_to}` / prc.`{last_3_mth}`) -1, 0) {period_cd_to}_diff_2            
        from pivot_ppp_his_prc prc
    ),
    media_ppp as (
        select 
            a.pcmdty_id, 
            a.period_cd, 
            round(avg(a.mean_1), 2) media_ppp
        from (
          select 
            pcmdty_id, 
            period_cd,
            dsupp_proc_id,
            avg(ppp_prc_amt) mean_1
          from {pcm_ppp_tax_calc_fltr_1}
          where period_cd = {period_cd_to}
          group by pcmdty_id, period_cd, dsupp_proc_id
        ) a
        group by pcmdty_id, period_cd
    ),
    cte_final_part_1 as (
        select
            u.pcmdty_id,
            u.pri_org_short_nm corp_cd,
            u.pri_org_nm manufacturer,
            u.stdiz_pcmdty_nm pack,
            coalesce(u.gmrs_cd, '-') gmrs_cd,
            concat(u.atc1_cd, concat(' - ', u.atc1_nm)) ct1,
            u.seg_mkt_3,
            u.total_prev_sales_units unit_{last_2_mth},
            u.sales_fat fat,
            {s_ppp},
            {s_step},
            {s_l},
            {s_disc},
            round(diff1.`{last_3_mth}_diff` * 100, 0) `{last_3_mth}_diff`,
            round(diff1.`{last_2_mth}_diff` * 100, 0) `{last_2_mth}_diff`,
            round(diff1.`{period_cd_to}_diff` * 100, 0) `{period_cd_to}_diff`,
            round(diff2.`{last_3_mth}_diff_2` * 100, 0) `{last_3_mth}_diff_2`,
            round(diff2.`{last_2_mth}_diff_2` * 100, 0) `{last_2_mth}_diff_2`,
            round(diff2.`{period_cd_to}_diff_2` * 100, 0) `{period_cd_to}_diff_2`,
            rp.ref_price `ref_{period_cd_to}`,
            coalesce(round((ppp.`{last_3_mth}` / rp.ref_price - 1) * 100, 0), '-') `diff_ref_{last_3_mth}`,
            coalesce(round((ppp.`{last_2_mth}` / rp.ref_price - 1) * 100, 0), '-') `diff_ref_{last_2_mth}`,
            coalesce(round((ppp.`{period_cd_to}` / rp.ref_price - 1) * 100, 0), '-') `diff_ref_{period_cd_to}`,
            if(u.sales_fat >10000, true, false) priority,
            coalesce(if(abs(diff2.`{last_2_mth}_diff_2`) > 0.2, 1, 0), '-') `prev"+/-20"_{last_2_mth}`,
            coalesce(if(abs(diff1.`{last_2_mth}_diff`) > 0.15, 1, 0), '-') `mes"+/-15"_{last_2_mth}`,
            coalesce(if(abs(round((ppp.`{last_2_mth}` / rp.ref_price - 1) * 100, 0)) > 0.2, 1, 0), '-') `ref_mes"+/-20"_{last_2_mth}`,
            coalesce(if(abs(diff2.`{period_cd_to}_diff_2`) > 0.2, 1, 0), '-') `prev"+/-20"_{period_cd_to}`,
            coalesce(if(abs(round((ppp.`{period_cd_to}` / rp.ref_price - 1) * 100, 0)) > 0.2, 1, 0), '-') `ref_mes"+/-20"_{period_cd_to}`
        from cur_units_mth u
        join pivot_ppp_his_prc ppp on u.pcmdty_id = ppp.pcmdty_id
        join last_3_steps step on ppp.pcmdty_id = step.pcmdty_id
        join l_prc_last_3_mth pf on step.pcmdty_id = pf.pcmdty_id
        join l_disc desc on step.pcmdty_id = desc.pcmdty_id
        join diff_1 diff1 on desc.pcmdty_id = diff1.pcmdty_id
        join diff_2 diff2 on diff1.pcmdty_id = diff2.pcmdty_id
        left join tmp_ref_price rp on diff2.pcmdty_id = rp.pcmdty_id
    ),
    cte_final_part_2 as (
        select
            u.*,
            if(`prev"+/-20"_{last_2_mth}` = 0 and `mes"+/-15"_{last_2_mth}`= 1, (`{last_4_mth}_ppp` +`{last_3_mth}_ppp`+`{period_cd_to}_ppp`) / 3, 0) `Ajustar_{last_2_mth}`
        from cte_final_part_1 u
    ),
    cte_final_part_3 as (
        select
            u.*,
            if(`Ajustar_{last_2_mth}` = 0, `{last_2_mth}_ppp`, `Ajustar_{last_2_mth}`) `new_{last_2_mth}`
        from cte_final_part_2 u
    ),
    cte_final_part_4 as (
        select
            u.*,
            if(abs(`{period_cd_to}_ppp`/ `new_{last_2_mth}`-1) > 0.15, 1, 0) `mês_"+/-15"_{last_2_mth}`
        from cte_final_part_3 u
    ),
    cte_final_part_5 as (
        select
            u.*,
            if(`prev"+/-20"_{period_cd_to}` = 1 and `mês_"+/-15"_{last_2_mth}` = 1 and `ref_mes"+/-20"_{period_cd_to}` = 0,
            (`ref_{period_cd_to}` + `{last_3_mth}_ppp`+`{last_2_mth}_ppp`+`{period_cd_to}_ppp`) / 4,
            0) `Ajustar_{period_cd_to}_1`
        from cte_final_part_4 u
    ),
    cte_final as (
        select
            u.*,
            if(`mês_"+/-15"_{last_2_mth}` = 1 and `Ajustar_{period_cd_to}_1` = 0, (`{last_3_mth}_ppp`+`{period_cd_to}_ppp`) / 2, 0) `Ajustar_{period_cd_to}_2`,
            '' `new_{period_cd_to}`,
            '' avaliar,
            mp.media_ppp
        from cte_final_part_5 u
        left join media_ppp mp on u.pcmdty_id = mp.pcmdty_id
    )
select * from cte_final
order by pcmdty_id asc
""")

# 11
from pyspark.sql.functions import col
df = df.select([col(c).cast("string") for c in df.columns])
file_output = f"{config['pcmppp_output']}/ppp_qc_report_{period_cd_to}-price_validation.csv"
print(f"Recording txt file to '{file_output}' with {df.count()} records")
df.toPandas().to_csv(file_output, header=True, index=False)
print("OK")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------


