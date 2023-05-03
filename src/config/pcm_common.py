# Databricks notebook source
# Config Env Constants
DEVL_ENV="devl"
UACC_ENV="uacc"
PROD_ENV="prod"

dbutils.widgets.removeAll()

# Required Widgets
dbutils.widgets.dropdown('country', 'br', ['br', 'mx', 'co', 'ar'])
dbutils.widgets.dropdown('env', DEVL_ENV, [DEVL_ENV, UACC_ENV, PROD_ENV])

# Optional Widgets
dbutils.widgets.text('period_cd_from', '202303')
dbutils.widgets.text('period_cd_to',  dbutils.widgets.get('period_cd_from'))

# Set Widget Variables
period_cd_from = dbutils.widgets.get('period_cd_from')
period_cd_to = dbutils.widgets.get('period_cd_to')

# Print Vars
print(f"country={dbutils.widgets.get('country')}")
print(f"env={dbutils.widgets.get('env')}")
print(f"period_cd_from={period_cd_from}")
print(f"period_cd_to={period_cd_to}")

# COMMAND ----------

## Config System Variables - Common Standard for all environments
country_code=dbutils.widgets.get('country')   #ar,br,mx,co
tenant_short= country_code   #ar,br,mx,co
tenant= f"{tenant_short}9"   #ar9,br9,mx9,co9
env_short=dbutils.widgets.get('env')
env_letter=env_short[:1]   #d,u,p
environment_long_list={DEVL_ENV: 'development', UACC_ENV: 'acceptance', PROD_ENV: 'production'}
env_long = environment_long_list[env_short]   #development,acceptance,production

## Config dict init
config = { DEVL_ENV: {}, UACC_ENV: {}, PROD_ENV: {}}


################# User Config Samples ############################
### BEFORE environment setting - Set manual env variables here
config[DEVL_ENV]['sqlserver_monthly_host'] = "CDTSSQ2045P.production.imsglobal.com"
config[UACC_ENV]['sqlserver_monthly_host'] = "cdtssql295d.production.imsglobal.com\MSSQL2014"
config[PROD_ENV]['sqlserver_monthly_host'] = "CDTSSQ2045P.production.imsglobal.com"

config[DEVL_ENV]['sqlserver_monthly_db'] = "ims_bra"
config[UACC_ENV]['sqlserver_monthly_db'] = "ims_bra_"
config[PROD_ENV]['sqlserver_monthly_db'] = "ims_bra"

config[DEVL_ENV]['sqlserver_weekly_host'] = "CDTSSQ2037P.production.imsglobal.com"
config[UACC_ENV]['sqlserver_weekly_host'] = "CDTSSQ2007P.production.imsglobal.com"
config[PROD_ENV]['sqlserver_weekly_host'] = "CDTSSQ2037P.production.imsglobal.com"

config[DEVL_ENV]['sqlserver_weekly_db'] = "ims_bra"
config[UACC_ENV]['sqlserver_weekly_db'] = "ims_bra_"
config[PROD_ENV]['sqlserver_weekly_db'] = "ims_bra"

## System Config Setting 
config = config[env_short]           ## do not change this line

### AFTER environment setting - Set reusable variables goes here
config['lgy_db'] = f"{env_short}_{tenant}_lgy"
config['batch_db']=f"{env_short}_{tenant}_batch"
config['gdm_db']=f"{env_short}_{tenant}"
config['gdm_rds']=f"{env_short}_{tenant_short}"
config['gdm_rds_xg']=f"{env_short}_rds_xg"
config['pcmppp_db']=f"{env_short}_{tenant}_pcm_ppp"

# FileSystem
config['pcmppp_input']=f"/mnt/bdf/{env_long}/{tenant}/data/pcmppp/input"
config['pcmppp_output']=f"/dbfs/mnt/bdf/{env_long}/{tenant}/data/pcmppp/output"
config['pcmppp_processed']=f"/mnt/bdf/{env_long}/{tenant}/data/pcmppp/processed"

# Log config
for k, v in config.items():
    print(f"{k}={v}")
    # to use widgets in %sql notebooks
    dbutils.widgets.text(k,v)
print()

# COMMAND ----------

#####################
### Common Config ###
#####################

# tables
lgy_prices = f"{config['lgy_db']}.lgy_prices"
lgy_ddd_un = f"{config['lgy_db']}.lgy_ddd_un"
ref_whs_seller_pcmdty_qty_fct = f"{config['pcmppp_db']}.ref_whs_seller_pcmdty_qty_fct"
sales_ddd_un = f"{config['pcmppp_db']}.sales_ddd_un"
pcm_ppp_ref_ncm_pcmdty_brdg = f"{config['pcmppp_db']}.pcm_ppp_ref_ncm_pcmdty_brdg"
pcm_ppp_ref_pnn_flag = f"{config['pcmppp_db']}.pcm_ppp_ref_pnn_flag"
pcm_ppp_ref_ncm_prod_attr_brdg = f"{config['pcmppp_db']}.pcm_ppp_ref_ncm_prod_attr_brdg"
pcm_ppp_ref_ncm_nomed_exc = f"{config['pcmppp_db']}.pcm_ppp_ref_ncm_nomed_exc"
pcm_ppp_ref_ncm_pcmdty_file_brdg = f"{config['pcmppp_db']}.pcm_ppp_ref_ncm_pcmdty_file_brdg"
pcm_ppp_ref_tax_no_med_federal_tax_rt = f"{config['pcmppp_db']}.pcm_ppp_ref_tax_no_med_federal_tax_rt"
pcm_ppp_ref_dim_prod = f"{config['pcmppp_db']}.pcm_ppp_ref_dim_prod"        
pcm_ppp_ref_dim_otlt = f"{config['pcmppp_db']}.pcm_ppp_ref_dim_otlt"
pcm_ppp_ref_dim_whs = f"{config['pcmppp_db']}.pcm_ppp_ref_dim_whs"
pcm_ppp_ref_dim_ext_prc_d = f"{config['pcmppp_db']}.pcm_ppp_ref_dim_ext_prc_d"
pcm_ppp_ref_dim_ext_prc_m = f"{config['pcmppp_db']}.pcm_ppp_ref_dim_ext_prc_m"
pcm_ppp_ref_dim_ext_prc_l = f"{config['pcmppp_db']}.pcm_ppp_ref_dim_ext_prc_l"
pcm_ppp_ref_states = f"{config['pcmppp_db']}.pcm_ppp_ref_states"
pcm_ppp_cpp_min_max_pct = f"{config['pcmppp_db']}.pcm_ppp_cpp_min_max_pct"
pcm_ppp_gmrs_min_max_pct = f"{config['pcmppp_db']}.pcm_ppp_gmrs_min_max_pct"
pcm_ppp_ref_med_icms_sp = f"{config['pcmppp_db']}.pcm_ppp_ref_med_icms_sp"
pcm_ppp_ref_ncm_prod_attr_brdg = f"{config['pcmppp_db']}.pcm_ppp_ref_ncm_prod_attr_brdg"
pcm_ppp_ref_pack_z1l = f"{config['pcmppp_db']}.pcm_ppp_ref_pack_z1l"
pcm_ppp_ref_whs_qty_mult_fct = f"{config['pcmppp_db']}.pcm_ppp_ref_whs_qty_mult_fct"
pcm_ppp_ref_tax_non_med_icms_rt = f"{config['pcmppp_db']}.pcm_ppp_ref_tax_non_med_icms_rt"
pcm_ppp_ref_whs_smooth = f"{config['pcmppp_db']}.pcm_ppp_ref_whs_smooth"
pcm_ppp_ref_tax_med_icms_all_states = f"{config['pcmppp_db']}.pcm_ppp_ref_tax_med_icms_all_states"
pcm_ppp_ref_ncm_pcmdty_file_brdg_csvinput = f"{config['pcmppp_db']}.pcm_ppp_ref_ncm_pcmdty_file_brdg_csvinput"
pcm_ppp_ref_ncm_prod_attr_brdg_csvinput = f"{config['pcmppp_db']}.pcm_ppp_ref_ncm_prod_attr_brdg_csvinput"
pcm_ppp_ref_ncm_nomed_exc_csvinput = f"{config['pcmppp_db']}.pcm_ppp_ref_ncm_nomed_exc_csvinput"
pcm_ppp_ref_pnn_flag_csvinput = f"{config['pcmppp_db']}.pcm_ppp_ref_pnn_flag_csvinput"
pcm_ppp_ref_tax_no_med_federal_tax_rt_csvinput = f"{config['pcmppp_db']}.pcm_ppp_ref_tax_no_med_federal_tax_rt_csvinput"
pcm_ppp_cpp_min_max_pct_csvinput = f"{config['pcmppp_db']}.pcm_ppp_cpp_min_max_pct_csvinput"
pcm_ppp_gmrs_min_max_pct_csvinput = f"{config['pcmppp_db']}.pcm_ppp_gmrs_min_max_pct_csvinput"
pcm_ppp_ref_med_icms_sp_csvinput = f"{config['pcmppp_db']}.pcm_ppp_ref_med_icms_sp_csvinput"
pcm_ppp_ref_ncm_prod_attr_brdg_csvinput = f"{config['pcmppp_db']}.pcm_ppp_ref_ncm_prod_attr_brdg_csvinput"
pcm_ppp_ref_pack_z1l_csvinput = f"{config['pcmppp_db']}.pcm_ppp_ref_pack_z1l_csvinput"
pcm_ppp_ref_tax_non_med_icms_rt_csvinput = f"{config['pcmppp_db']}.pcm_ppp_ref_tax_non_med_icms_rt_csvinput"
pcm_ppp_ref_whs_qty_mult_fct_csvinput = f"{config['pcmppp_db']}.pcm_ppp_ref_whs_qty_mult_fct_csvinput"
pcm_ppp_ref_whs_smooth_csvinput = f"{config['pcmppp_db']}.pcm_ppp_ref_whs_smooth_csvinput"
pcm_ppp_ref_tax_med_icms_all_states_csvinput  = f"{config['pcmppp_db']}.pcm_ppp_ref_tax_med_icms_all_states_csvinput"
pcm_ppp_ref_tax_no_med_tipi_excl_csvinput  = f"{config['pcmppp_db']}.pcm_ppp_ref_tax_no_med_tipi_excl_csvinput"
pcm_ppp_ref_tax_no_med_cest_excl_csvinput   = f"{config['pcmppp_db']}.pcm_ppp_ref_tax_no_med_cest_excl_csvinput"
pcm_ppp_apps_buys_trans = f"{config['pcmppp_db']}.pcm_ppp_apps_buys_trans"
pcm_ppp_buys_trans = f"{config['pcmppp_db']}.pcm_ppp_buys_trans"
pcm_ppp_buys_trans_fltr_1 = f"{config['pcmppp_db']}.pcm_ppp_buys_trans_fltr_1"
pcm_ppp_prc = f"{config['pcmppp_db']}.pcm_ppp_prc"
pcm_ppp_prc_hist = f"{config['pcmppp_db']}.pcm_ppp_prc_hist"
pcm_ppp_tax_calc_fltr_1 = f"{config['pcmppp_db']}.pcm_ppp_tax_calc_fltr_1"
pcm_ppp_tax_calc = f"{config['pcmppp_db']}.pcm_ppp_tax_calc"
pcm_ppp_wgt_calc = f"{config['pcmppp_db']}.pcm_ppp_wgt_calc"
pcm_ppp_wgt_calc_hist = f"{config['pcmppp_db']}.pcm_ppp_wgt_calc_hist"
pcm_ppp_ref_tax_no_med_tipi_excl = f"{config['pcmppp_db']}.pcm_ppp_ref_tax_no_med_tipi_excl"
pcm_ppp_ref_tax_no_med_cest_excl = f"{config['pcmppp_db']}.pcm_ppp_ref_tax_no_med_cest_excl"
pcm_ppp_wgt_hist = f"{config['pcmppp_db']}.pcm_ppp_wgt_hist"
pcm_ppp_ref_prc_manl_corrn = f"{config['pcmppp_db']}.pcm_ppp_ref_prc_manl_corrn"
pcm_ppp_ref_prc_manl_corrn_hist = f"{config['pcmppp_db']}.pcm_ppp_ref_prc_manl_corrn_hist"
pcm_ppp_ref_prc_manl_corrn_error_log = f"{config['pcmppp_db']}.pcm_ppp_ref_prc_manl_corrn_error_log"
pcm_ppp_publ_prices_wkly = f"{config['pcmppp_db']}.pcm_ppp_publ_prices_wkly"
pcm_ppp_publ_prices_mthly = f"{config['pcmppp_db']}.pcm_ppp_publ_prices_mthly"
pcm_ppp_publ_prices_sub_natl = f"{config['pcmppp_db']}.pcm_ppp_publ_prices_sub_natl"
pcm_cpp_prices_sub_natl = f"{config['pcmppp_db']}.pcm_cpp_prices_sub_natl"
pcm_ppp_leg_prices = f"{config['pcmppp_db']}.pcm_ppp_leg_prices"
pcm_ppp_hist_step_52 = f"{config['pcmppp_db']}.pcm_ppp_hist_step_52"

# views
vw_rds_product_dim = f"{config['gdm_db']}.vw_rds_product_dim"
vw_rds_product_ext_attr = f"{config['gdm_db']}.vw_rds_product_ext_attr"
vw_rds_oac_dim_outlet = f"{config['gdm_db']}.vw_rds_oac_dim_outlet"
vw_rds_oac_dim_whs = f"{config['gdm_db']}.vw_rds_oac_dim_whs"
vw_rds_pcmdty_id_merge = f"{config['gdm_db']}.vw_rds_pcmdty_id_merge"
vw_lgy_apps_mthly_fractn_2nps = f"{config['lgy_db']}.vw_lgy_apps_mthly_fractn_2nps"

# temp
temp_periods = f"{config['pcmppp_db']}.temp_periods"
