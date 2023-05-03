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
approx_percentile_accuracy = 1000000000
std_avg_constant = 0.25
trans_limit1 = 3
trans_limit2 = 10
trans_limit3 = 11
trans_limit4 = 999
trans_limit5 = 1000

## steps

# 10
spark.sql(f"""delete from {pcm_ppp_tax_calc_fltr_1} where period_cd between '{period_cd_from}' and '{period_cd_to}'""")

# 11
spark.sql("drop view if exists cte_provider_rank")
spark.sql(f"""
cache table cte_provider_rank
select buys_trans_line_id, 
       dsupp_proc_id, 
       trans_dt, 
       buyer_is_pharmy_ind, 
       seller_otlt_cd, 
       buyer_otlt_cd, 
       buyer_is_otlt_chain_ind, 
       pcmdty_id, 
       ppp_prc_amt, 
       period_cd,
       buyer_st_cd, 
       dsupp_is_distr_ind, 
       pck_is_medcn_ind, dense_rank() over(partition by buyer_otlt_cd, pcmdty_id, trans_dt order by dsupp_is_distr_ind) row_num
from {pcm_ppp_tax_calc}
where period_cd between '{period_cd_from}' and '{period_cd_to}'
""")

# 12
spark.sql("drop view if exists cte_cpp_outlier")
spark.sql(f"""
cache table cte_cpp_outlier
with cte_tax_calc as
    (
      select buys_trans_line_id, dsupp_proc_id, trans_dt, seller_otlt_cd, buyer_otlt_cd, buyer_is_otlt_chain_ind, 
             pcmdty_id, ppp_prc_amt, period_cd, buyer_st_cd, buyer_is_pharmy_ind, pck_is_medcn_ind
      from cte_provider_rank
      where row_num = 1
    ),
    cte_cpp_price as (
        select a.*, coalesce(b.prc_amt, 0.00) as cpp_price, b.cpp_min_pct as cpp_min_pct, b.cpp_max_pct as cpp_max_pct
        from cte_tax_calc a, {pcm_ppp_cpp_min_max_pct} b
        left join {pcm_ppp_ref_dim_ext_prc_d} b on a.pcmdty_id = b.pcmdty_id and left(cast(date_format(dateadd(month, -1, to_date(concat(a.period_cd, '01'), 'yyyyMMdd')),'yyyyMMdd') as string), 6) = b.period_cd
      ),
      cte_cpp_outlier as (
        select *,
          if(cpp_price > 0.05,
            if(ppp_prc_amt> (cpp_price*cpp_min_pct/100) and ppp_prc_amt< (cpp_price*cpp_max_pct/100),
              0,
              1),
            0)as outlier_ind --1 is outlier
        from cte_cpp_price
      )
      select *
      from cte_cpp_outlier
      where outlier_ind = 0
""")

# 13
spark.sql("drop view if exists valid_trans")
spark.sql("""
cache table valid_trans
with valid_trans as (
	select
		period_cd,
		pcmdty_id,
		buyer_is_otlt_chain_ind
	from cte_cpp_outlier
	group by period_cd,
			pcmdty_id,
			buyer_is_otlt_chain_ind
	having count(*) >= 5
)
select * 
from valid_trans
""")

# 14
spark.sql("drop view if exists cte_limits")
spark.sql(f"""
cache table cte_limits
with get_max_prc as (
    select 
		period_cd,
		pcmdty_id,
		max(ppp_prc_amt) max_prc
	from cte_cpp_outlier
	group by period_cd,
			 pcmdty_id
),
prc_stdzn as(
	select 
		t.*,
		t.ppp_prc_amt / p.max_prc stdzn_prc
	from cte_cpp_outlier t
	join get_max_prc p on t.period_cd = p.period_cd and t.pcmdty_id = p.pcmdty_id
),
flag_trans as(
	select 
		t.*,
		if(v.pcmdty_id is null, 0, 1) valid_flag
	from cte_cpp_outlier t
	left join valid_trans v on t.pcmdty_id = v.pcmdty_id and t.period_cd = v.period_cd and t.buyer_is_otlt_chain_ind = v.buyer_is_otlt_chain_ind
),
percentil as (
	select 
		t.buys_trans_line_id,
		t.period_cd,
		t.pcmdty_id,
		t.pck_is_medcn_ind,
		pr.ppp_prc_amt,
		pr.stdzn_prc,
        percentile_approx(pr.stdzn_prc, 0.25, 2147483647) over (partition by t.period_cd, t.pcmdty_id, t.buyer_is_otlt_chain_ind) as p025_prc,
        percentile_approx(pr.stdzn_prc, 0.75, 2147483647) over (partition by t.period_cd, t.pcmdty_id, t.buyer_is_otlt_chain_ind) as p075_prc,
		percentile_approx(pr.stdzn_prc, 0.75, 2147483647) over (partition by t.period_cd, t.pcmdty_id, t.buyer_is_otlt_chain_ind) - 
        percentile_approx(pr.stdzn_prc, 0.25, 2147483647) over (partition by t.period_cd, t.pcmdty_id, t.buyer_is_otlt_chain_ind) as rang_inter
	from flag_trans t
	join prc_stdzn pr on t.pcmdty_id = pr.pcmdty_id and t.period_cd = pr.period_cd and t.buys_trans_line_id = pr.buys_trans_line_id
	where t.valid_flag = 1
),
limit_perc as (
	select
		p.buys_trans_line_id,
		p.period_cd,
		p.pcmdty_id,
		p.pck_is_medcn_ind,
		p025_prc,
		p075_prc,
		rang_inter,
		cast(p.p025_prc - (1.5 * p.rang_inter) as decimal(38,12)) li,
		cast(p.p075_prc + (1.5 * p.rang_inter) as decimal(38,12)) ls,
	    cast(stdzn_prc as decimal(38,12)) stdzn_prc,
        ppp_prc_amt
	from percentil p
)
select * from limit_perc
""")

# 15
spark.sql("drop view if exists trans_out_distr")
spark.sql("""
cache table trans_out_distr
with flag_dist_out as (
	select
		l.buys_trans_line_id,
		l.period_cd,
		l.pcmdty_id,
		l.pck_is_medcn_ind,
		ppp_prc_amt,
		p025_prc,
		p075_prc,
		rang_inter,
		li,
		ls,
        if(l.pck_is_medcn_ind = 0, 
            if((stdzn_prc < l.li) or (stdzn_prc > l.ls), 1, 0), 
            if((stdzn_prc < l.li), 1, 0)
            ) flag_out
	from cte_limits l
)
select *
from flag_dist_out
where flag_out = 1
""")

# 16
spark.sql("drop view if exists cte_ppp_outlier_2")
spark.sql(f"""
cache table cte_ppp_outlier_2
select c.* 
from cte_cpp_outlier c
left join trans_out_distr d on c.buys_trans_line_id = d.buys_trans_line_id
where d.buys_trans_line_id is null
""")

# 21
spark.sql(f"""
insert into {pcm_ppp_tax_calc_fltr_1}
select
    dsupp_proc_id,
    trans_dt,
    seller_otlt_cd,
    buyer_otlt_cd,
    buyer_is_otlt_chain_ind,
    pcmdty_id,
    ppp_prc_amt,
    buyer_st_cd,
    buyer_is_pharmy_ind,
    pck_is_medcn_ind,
    period_cd
from cte_ppp_outlier_2
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation
