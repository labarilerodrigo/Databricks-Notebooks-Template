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

## steps

# 10
spark.sql(f"""delete from {pcm_ppp_wgt_calc} where period_cd between '{period_cd_from}' and '{period_cd_to}'""")
spark.sql(f"""delete from {pcm_ppp_prc} where period_cd between '{period_cd_from}' and '{period_cd_to}'""")

# 11
spark.sql("drop view if exists tmp_missing_otlt")
spark.sql(f"""
--get all the outlet_code that have units in sales table but doesn't have transactions
cache table tmp_missing_otlt
with cte_sales as (
      select a.period_cd, a.fcc, a.outlet_code, concat(a.period_cd, a.outlet_code) as otlt_key
      from {sales_ddd_un} a
      where a.period_cd between '{period_cd_from}' and '{period_cd_to}'
  ),
  cte_trans as (
      select a.period_cd,  a.buyer_otlt_cd, concat(a.period_cd, a.buyer_otlt_cd) as otlt_key
      from {pcm_ppp_tax_calc_fltr_1} a
      where a.period_cd between '{period_cd_from}' and '{period_cd_to}'
  ),
  cte_missing_otlt as (
      select distinct a.period_cd, a.fcc, a.outlet_code
      from cte_sales a
      where not exists (select * from cte_trans b where a.otlt_key= b.otlt_key)
  )
  select a.period_cd, a.fcc, a.outlet_code, b.cnpj_cd, b.cnpj_root_cd, b.st_cd, concat(b.cnpj_root_cd, b.st_cd) as root_cnpj_st, cast(is_otlt_chain_ind as boolean) as is_otlt_chain_ind
  from cte_missing_otlt a
  join {pcm_ppp_ref_dim_otlt} b on a.outlet_code= b.outlet_code
""")

# 12
spark.sql("drop view if exists tmp_prc_cnpjroot_st")
spark.sql(f"""
--create table with ppp_prc grouped by cnpj_root_cd/st_cd
cache table tmp_prc_cnpjroot_st
with cte_cnpj_st as (
      select a.period_cd, a.pcmdty_id, b.cnpj_cd, b.cnpj_root_cd, CONCAT(b.cnpj_root_cd, b.st_cd) as cnpj_root_st, a.buyer_is_pharmy_ind, avg(a.ppp_prc_amt) as ppp_prc
      from {pcm_ppp_tax_calc_fltr_1} a
      join {pcm_ppp_ref_dim_otlt} b on a.buyer_otlt_cd = b.outlet_code
      where a.period_cd between '{period_cd_from}' and '{period_cd_to}'
      group by a.period_cd, a.pcmdty_id, b.cnpj_cd, b.cnpj_root_cd, b.st_cd, a.buyer_is_pharmy_ind
  )
  select period_cd, pcmdty_id, cnpj_root_cd, cnpj_root_st, buyer_is_pharmy_ind, avg(ppp_prc) as ppp_prc
  from cte_cnpj_st
  group by period_cd, pcmdty_id, cnpj_root_cd, cnpj_root_st, buyer_is_pharmy_ind
""")

# 13
spark.sql("drop view if exists tmp_prc_cnpjroot")
spark.sql(f"""
--create table with ppp_prc grouped by cnpj_root_cd
cache table tmp_prc_cnpjroot
select period_cd, pcmdty_id, cnpj_root_cd, buyer_is_pharmy_ind, avg(ppp_prc) as ppp_prc
from tmp_prc_cnpjroot_st
group by period_cd, pcmdty_id, cnpj_root_cd, buyer_is_pharmy_ind
""")

# 14
spark.sql("drop view if exists tmp_msng_otlt_prc")
spark.sql(f"""
--get ppp_prc for outlets that doesn't have transactions
cache table tmp_msng_otlt_prc
select a.period_cd, a.fcc, a.outlet_code, a.cnpj_cd, a.cnpj_root_cd, a.st_cd, a.root_cnpj_st, a.is_otlt_chain_ind,
      if(b.buyer_is_pharmy_ind is null, if(c.buyer_is_pharmy_ind is null, null, c.buyer_is_pharmy_ind), b.buyer_is_pharmy_ind) as buyer_is_pharmy_ind,
      if(b.ppp_prc is null, if(c.ppp_prc is null, 0, c.ppp_prc), b.ppp_prc) as ppp_prc_amt
from tmp_missing_otlt a
left join tmp_prc_cnpjroot_st b on a.period_cd= b.period_cd and a.root_cnpj_st= b.cnpj_root_st and a.fcc= b.pcmdty_id
left join tmp_prc_cnpjroot c on a.period_cd= c.period_cd and a.cnpj_root_cd= c.cnpj_root_cd and a.fcc= c.pcmdty_id
""")

# 15
spark.sql("drop view if exists tmp_whs_st")
spark.sql(f"""
--create table with ppp_prc grouped by whs_cd/st_cd
cache table tmp_whs_st
with cte_whs_trans as (
      select t.period_cd, t.pcmdty_id, whs.wholesaler_code, concat(whs.wholesaler_code, otlt.st_cd) whs_key, t.buyer_is_pharmy_ind, t.ppp_prc_amt
      from {pcm_ppp_tax_calc_fltr_1} t
      inner join {pcm_ppp_ref_dim_whs} whs on t.dsupp_proc_id = whs.wholesaler_code
      inner join {pcm_ppp_ref_dim_otlt} otlt on t.buyer_otlt_cd = otlt.outlet_code
      where whs.is_whs_dlvry_ind = 1 and (t.period_cd between '{period_cd_from}' and '{period_cd_to}')
  )
  select period_cd, pcmdty_id, wholesaler_code, whs_key, buyer_is_pharmy_ind, avg(ppp_prc_amt) as ppp_prc
  from cte_whs_trans
  group by period_cd, pcmdty_id, wholesaler_code, whs_key, buyer_is_pharmy_ind
""")

# 16
spark.sql("drop view if exists tmp_whs")
spark.sql(f"""
--create table with ppp_prc grouped by whs_cd
cache table tmp_whs
select period_cd, pcmdty_id, wholesaler_code, buyer_is_pharmy_ind, avg(ppp_prc) as ppp_prc
from tmp_whs_st
group by period_cd, pcmdty_id, wholesaler_code, buyer_is_pharmy_ind
""")

# 17
spark.sql("drop view if exists tmp_cnpj_BN_list")
spark.sql(f"""
--get all the cnpj_code of delivery type (satrts with BN) that have units in sales table
cache table tmp_cnpj_BN_list
select s.period_cd, s.fcc, s.wholesaler_code, s.outlet_code,  otlt.st_cd, concat(s.wholesaler_code, otlt.st_cd) as whs_key, cast(otlt.is_otlt_chain_ind as boolean) as is_otlt_chain_ind
from {sales_ddd_un} s
inner join {pcm_ppp_ref_dim_whs} whs on s.wholesaler_code= whs.wholesaler_code
inner join {pcm_ppp_ref_dim_otlt} otlt on s.outlet_code= otlt.outlet_code
where otlt.cnpj_cd like 'BN%' and (s.period_cd between '{period_cd_from}' and '{period_cd_to}')
""")

# 18
spark.sql("drop view if exists tmp_cnpj_BN_prc")
spark.sql(f"""
--get ppp_prc for 'BN' type outlets
cache table tmp_cnpj_BN_prc
select a.period_cd, a.fcc, a.outlet_code, a.is_otlt_chain_ind, a.st_cd,
    if(b.buyer_is_pharmy_ind is null, if(c.buyer_is_pharmy_ind is null, null, c.buyer_is_pharmy_ind), b.buyer_is_pharmy_ind) as buyer_is_pharmy_ind,
    if(b.ppp_prc is null, if(c.ppp_prc is null, 0, c.ppp_prc), b.ppp_prc) as ppp_prc_amt
from tmp_cnpj_BN_list a
left join tmp_whs_st b on a.period_cd= b.period_cd and a.whs_key= b.whs_key and a.fcc= b.pcmdty_id
left join tmp_whs c on a.period_cd= c.period_cd and a.wholesaler_code= c.wholesaler_code and a.fcc= c.pcmdty_id
""")

# 19
spark.sql("drop view if exists tmp_all_trans")
spark.sql(f"""
--new transaction table with original transactions and the outlets with derived transactions
cache table tmp_all_trans
with cte_all_trans as (
      select period_cd, fcc as pcmdty_id, outlet_code as buyer_otlt_cd, st_cd as buyer_st_cd, is_otlt_chain_ind as buyer_is_otlt_chain_ind, buyer_is_pharmy_ind, ppp_prc_amt
      from tmp_msng_otlt_prc
      where ppp_prc_amt > 0
      union
      select period_cd, fcc as pcmdty_id, outlet_code as buyer_otlt_cd, st_cd as buyer_st_cd, is_otlt_chain_ind as buyer_is_otlt_chain_ind, buyer_is_pharmy_ind, ppp_prc_amt
      from tmp_cnpj_BN_prc
      where ppp_prc_amt > 0
      union
      select period_cd, pcmdty_id, buyer_otlt_cd, buyer_st_cd, buyer_is_otlt_chain_ind, buyer_is_pharmy_ind, ppp_prc_amt
      from {pcm_ppp_tax_calc_fltr_1}
      where period_cd between '{period_cd_from}' and '{period_cd_to}'
  )
select * from cte_all_trans
""")

# 20
spark.sql("drop view if exists tmp_chain_ind")
spark.sql(f"""
cache table tmp_chain_ind
select 1 as chain_ind
union
select 0 as chain_ind
""")

# 21
spark.sql("drop view if exists tmp_all_states")
spark.sql(f"""
--cross join to get all the combination of states, chain_ind and fcc
cache table tmp_all_states
select a.state_abbrev as state_cd, b.chain_ind, c.pcmdty_id, c.period_cd
from {pcm_ppp_ref_states} a
cross join tmp_chain_ind b
cross join (select distinct pcmdty_id, period_cd
          from tmp_all_trans
          where period_cd between '{period_cd_from}' and '{period_cd_to}') c

""")

# 22
spark.sql("drop view if exists tmp_chnl_prc")
spark.sql(f"""
--weighted prices amount for all the fcc by state, channel and outlet
cache table tmp_chnl_prc
with
  cte_outlet_price
  as
  (
      --calculate avg price by outlet
      select pcmdty_id, buyer_otlt_cd as outlet, buyer_is_otlt_chain_ind as otlt_chain_ind, buyer_st_cd as state_cd, period_cd, avg(ppp_prc_amt) as prc_outlet_avg
      from tmp_all_trans
      where period_cd between '{period_cd_from}' and '{period_cd_to}'
      group by pcmdty_id, buyer_otlt_cd, buyer_is_otlt_chain_ind, buyer_st_cd, period_cd
  ),
  --2052700
  cte_cnpj_state
  as
  (
      --calculate avg price by cnpj_root_cd, state
      select f.period_cd, f.pcmdty_id, o.cnpj_root_cd, o.st_cd, concat(o.cnpj_root_cd, o.st_cd) as cnpj_key, avg(f.ppp_prc_amt) as prc_cnpj_st
      from {pcm_ppp_ref_dim_otlt} o
      join tmp_all_trans f on f.buyer_otlt_cd = o.outlet_code
      where f.period_cd between '{period_cd_from}' and '{period_cd_to}'
      group by f.period_cd, f.pcmdty_id, o.cnpj_root_cd, o.st_cd
  ),
  --9158838
  cte_cnpj_national
  as
  (
      --calculate national avg price by cnpj_root_cd
      select f.period_cd, f.pcmdty_id, o.cnpj_root_cd, avg(f.ppp_prc_amt) as prc_cnpj
      from {pcm_ppp_ref_dim_otlt} o
      join tmp_all_trans f on f.buyer_otlt_cd = o.outlet_code
      where f.period_cd between '{period_cd_from}' and '{period_cd_to}'
      group by f.period_cd, f.pcmdty_id, o.cnpj_root_cd
  ),
  --9154453
  cte_units_outlet_cnpj
  as
  (
      --get units by outlet, cnpj
      select s.period_cd, s.fcc as pcmdty_id, s.outlet_code, o.cnpj_root_cd as cnpj, st_cd, concat(o.cnpj_root_cd, st_cd) as cnpj_key,
          cast(is_otlt_chain_ind as boolean) as is_otlt_chain_ind, if(ppp_fisc_actvty_class_cd= 'PHARMACY', 1, 0) as pharmacy_ind, sum_units as units
      from {sales_ddd_un} s
      join {pcm_ppp_ref_dim_otlt} o on s.outlet_code= o.outlet_code
      where s.period_cd between '{period_cd_from}' and '{period_cd_to}' and s.sum_units> 0
  ),
  --471,602,165
  cte_price
  as
  (
      --calculate final price by outlet, cnpj
      select a.*, b.prc_outlet_avg, c.prc_cnpj_st, if(a.pharmacy_ind= 1, d.prc_cnpj, 0) as prc_cnpj,
          if(b.prc_outlet_avg is not null,
              b.prc_outlet_avg,
              if(c.prc_cnpj_st is not null,
                  c.prc_cnpj_st,
                  if(d.prc_cnpj is not null,
                      d.prc_cnpj,
                      0))) as prc_final
      from cte_units_outlet_cnpj a
      left join cte_outlet_price b on a.period_cd= b.period_cd and a.pcmdty_id= b.pcmdty_id and a.outlet_code= b.outlet
      left join cte_cnpj_state c on a.period_cd= c.period_cd and a.pcmdty_id= c.pcmdty_id and a.cnpj_key= c.cnpj_key
      left join cte_cnpj_national d on a.period_cd= d.period_cd and a.pcmdty_id= d.pcmdty_id and a.cnpj= d.cnpj_root_cd
  ),
  --471,602,165
  cte_missing_states
  as
  (
      --include all the missing combinations of pcmdty_id, state, channel
      select period_cd, pcmdty_id, outlet_code, cnpj, st_cd, is_otlt_chain_ind, units, prc_final
      from cte_price
      where prc_final<> 0
      union
      select period_cd, pcmdty_id, '0' as outlet_code, '0' as cnpj, state_cd, cast(chain_ind as boolean), 0 as units, 0 as prc_final
      from tmp_all_states
  ),
  --484,751,975
  cte_price_st_chnl
  as
  (
      --calculate weighted average by pcmdty_id, state, channel
      select period_cd, pcmdty_id, st_cd, is_otlt_chain_ind, sum(units) as units, if(sum(units)= 0, 0, sum(prc_final*units)/sum(units)) prc_st_chnl
      from cte_missing_states
      group by period_cd, pcmdty_id, st_cd, is_otlt_chain_ind
  ),
  --14,510,755
  cte_units_fcc_st_chnl
  as
  (
      --get total units by fcc, state, channel; Total units are used for calculating final National prices
      select period_cd, pcmdty_id, st_cd, is_otlt_chain_ind, sum(units) as units_total
      from cte_units_outlet_cnpj
      group by period_cd, pcmdty_id, st_cd, is_otlt_chain_ind
  ),
  cte_price_tot_units
  as
  (
      --we only use total units to calculate weighted price if sun_units > 0; units_ind is created
      select a.*, ifnull(b.units_total, 0) as units_total, if(units = 0, 0, 1) as units_ind
      from cte_price_st_chnl a
      left join cte_units_fcc_st_chnl b on a.period_cd= b.period_cd and a.pcmdty_id= b.pcmdty_id and a.st_cd= b.st_cd and a.is_otlt_chain_ind= b.is_otlt_chain_ind
  )

select *
from cte_price_tot_units
""")

# 23
spark.sql("drop view if exists tmp_chnl_prc_non_zero")
spark.sql(f"""
--loop through the table to update the price for the states which have 0 price for only one of the channels
cache table tmp_chnl_prc_non_zero
select *
from tmp_chnl_prc
where prc_st_chnl<> 0
""")

# 24
spark.sql("drop view if exists tmp_chnl")
spark.sql(f"""
cache table tmp_chnl
  select distinct a.period_cd, a.pcmdty_id, a.st_cd, a.is_otlt_chain_ind, a.units, a.units_total,
                  if(a.prc_st_chnl= 0 and b.prc_st_chnl is not null, b.prc_st_chnl, a.prc_st_chnl) prc_st_chnl,
                  if(a.units_ind= 0 and b.units_ind is not null, b.units_ind, a.units_ind) units_ind
  from tmp_chnl_prc a
  left join tmp_chnl_prc_non_zero b on a.pcmdty_id=b.pcmdty_id and a.st_cd= b.st_cd and a.period_cd= b.period_cd
""")

#25
spark.sql("drop view if exists tmp_wgt_calc")
spark.sql(f"""
--update the prices for states with 0 price for both the channels
  cache table tmp_wgt_calc
  with
      cte_national_price
      as
      (
          --calculate national price for using for states with 0 prices for both channels
          select period_cd, pcmdty_id, if(sum(units_ind * units_total) = 0, 0, sum(prc_st_chnl*units_ind*units_total)/sum(units_ind*units_total)) prc_national
          from tmp_chnl
          group by period_cd, pcmdty_id
      ),
      cte_update_state
      as
      (
          --update prices of state with 0 prices
          select a.period_cd, a.pcmdty_id, a.st_cd, if(a.is_otlt_chain_ind = 0, 'I', 'R') channel, a.units, a.units_total, a.units_ind,
                if(a.prc_st_chnl = 0, b.prc_national, a.prc_st_chnl) fnl_wgt_prc
          from tmp_chnl a
          inner join cte_national_price b on a.pcmdty_id = b.pcmdty_id and a.period_cd = b.period_cd
      ),
      /*
      --select only the FCC with active 3 months of historical data
      --PRICECH-3496: From 10-Apr onwards this piece of code will be disabled !
          cte_fcc_active_history
          as
          (
              select pcmdty_id, period_cd, sum(active) as active
              from (
                  select distinct pcmdty_id, left(cast(date_format(dateadd(month, 1, to_date(concat(period_cd, '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6) as period_cd, 1 as active
                  from {pcm_ppp_tax_calc_fltr_1}
                  where period_cd between left(cast(date_format(dateadd(month, -1, to_date(concat('{period_cd_from}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6) and
                                  left(cast(date_format(dateadd(month, -1, to_date(concat('{period_cd_to}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6)
                  union all
                  select distinct pcmdty_id, left(cast(date_format(dateadd(month, 2, to_date(concat(period_cd, '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6) as period_cd, 1 as active
                  from {pcm_ppp_tax_calc_fltr_1}
                  where period_cd between left(cast(date_format(dateadd(month, -2, to_date(concat('{period_cd_from}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6) and
                                  left(cast(date_format(dateadd(month, -2, to_date(concat('{period_cd_to}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6)
                  union all
                  select distinct pcmdty_id, left(cast(date_format(dateadd(month, 3, to_date(concat(period_cd, '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6) as period_cd, 1 as active
                  from {pcm_ppp_tax_calc_fltr_1}
                  where period_cd between left(cast(date_format(dateadd(month, -3, to_date(concat('{period_cd_from}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6) and
                                  left(cast(date_format(dateadd(month, -3, to_date(concat('{period_cd_to}', '01'), 'yyyyMMdd')), 'yyyyMMdd') as string), 6)

              ) a
              group by pcmdty_id, period_cd
          ),
      */
      cte_final
      as
      (
          select a.period_cd, a.pcmdty_id, concat(a.st_cd, a.channel) as geo_lvl_cd, a.units, a.units_total, a.units_ind, a.fnl_wgt_prc
              --, ifnull(b.active, 0) as active_history
          from cte_update_state a
          -- PRICECH-3496: From 10-Apr onwards this piece of code will be disabled !
          --left join cte_fcc_active_history b on a.pcmdty_id = b.pcmdty_id and a.period_cd = b.period_cd
      )

  select *
  from cte_final
  where fnl_wgt_prc <> 0 --keep only the prices > 0
  -- PRICECH-3496: From 10-Apr onwards this piece of code will be disabled !
  --and active_history = 3
""")

#26
spark.sql(f"""
-- insert into sub national table
insert into {pcm_ppp_wgt_calc}
select pcmdty_id,
  geo_lvl_cd,
  fnl_wgt_prc,
  units,
  1 as step_cd,
  units_total,
  period_cd
from tmp_wgt_calc
""")

# 27
spark.sql(f"""
--insert into national table
insert into {pcm_ppp_prc}
select pcmdty_id,
  if(sum(units_ind * units_total) = 0, 0, sum(fnl_wgt_prc*units_ind*units_total) / sum(units_ind*units_total)) as ppp_prc_amt,
  1 as step_cd,
  period_cd as period_cd
from tmp_wgt_calc
group by pcmdty_id, period_cd
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------


