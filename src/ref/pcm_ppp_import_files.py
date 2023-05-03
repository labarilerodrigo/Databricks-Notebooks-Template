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
from dataclasses import dataclass
@dataclass
class File():
    file: str
    final_table: str
    temp_table: str
    
        
files = []
files.append(File('pcm_ppp_cpp_min_max_pct', pcm_ppp_cpp_min_max_pct, pcm_ppp_cpp_min_max_pct_csvinput))                                     #pcmrefcppminmaxpct
files.append(File('pcm_ppp_gmrs_min_max_pct', pcm_ppp_gmrs_min_max_pct, pcm_ppp_gmrs_min_max_pct_csvinput))                                  #pcmrefgmrsminmaxpct
files.append(File('pcm_ppp_ref_med_icms_sp', pcm_ppp_ref_med_icms_sp, pcm_ppp_ref_med_icms_sp_csvinput))                                     #pcmrefmedicmssp
files.append(File('pcm_ppp_ref_ncm_prod_attr_brdg', pcm_ppp_ref_ncm_prod_attr_brdg, pcm_ppp_ref_ncm_prod_attr_brdg_csvinput))                #pcmrefncmnecnfc
files.append(File('pcm_ppp_ref_ncm_nomed_exc', pcm_ppp_ref_ncm_nomed_exc, pcm_ppp_ref_ncm_nomed_exc_csvinput))                               #pcmrefncmnomedexc
files.append(File('pcm_ppp_ref_ncm_pcmdty_brdg', pcm_ppp_ref_ncm_pcmdty_file_brdg, pcm_ppp_ref_ncm_pcmdty_file_brdg_csvinput))               #pcmrefncmpcmdtybrg 
files.append(File('pcm_ppp_ref_pnn_flag', pcm_ppp_ref_pnn_flag, pcm_ppp_ref_pnn_flag_csvinput))                                              #pcmrefpnnflag
files.append(File('pcm_ppp_ref_tax_med_icms_all_states', pcm_ppp_ref_tax_med_icms_all_states, pcm_ppp_ref_tax_med_icms_all_states_csvinput)) #pcmreftaxmedicmsallstates
files.append(File('pcm_ppp_ref_federal_tax', pcm_ppp_ref_tax_no_med_federal_tax_rt, pcm_ppp_ref_tax_no_med_federal_tax_rt_csvinput))         #pcmreftaxnomedfederaltax
files.append(File('pcm_ppp_ref_tax_no_med_cest_excl', pcm_ppp_ref_tax_no_med_cest_excl, pcm_ppp_ref_tax_no_med_cest_excl_csvinput))          #pcmreftaxnommedcestexcl
files.append(File('pcm_ppp_ref_tax_non_med_icms_rt', pcm_ppp_ref_tax_non_med_icms_rt, pcm_ppp_ref_tax_non_med_icms_rt_csvinput))             #pcmreftaxnommedicsmsrt
files.append(File('pcm_ppp_ref_tax_no_med_tipi_excl', pcm_ppp_ref_tax_no_med_tipi_excl, pcm_ppp_ref_tax_no_med_tipi_excl_csvinput))          #pcmreftaxnommedtipiexcl
files.append(File('pcm_ppp_ref_whs_qty_mult_fct', pcm_ppp_ref_whs_qty_mult_fct, pcm_ppp_ref_whs_qty_mult_fct_csvinput))                      #pcmrefwhsqtymultfct

for data in files:
    spark.sql(f"drop table if exists {data.temp_table}")
    df = spark.read \
      .format("csv") \
      .option("header", "true") \
      .option("inferSchema", "false") \
      .option("delimiter", "|") \
      .option("mode", "FAILFAST") \
      .load(f"{config['pcmppp_input']}/{data.file}.csv")
    print(f"Loaded dataframe from file: '{config['pcmppp_input']}/{data.file}.csv' with {df.count()} records")
    df.write.mode("overwrite").saveAsTable(data.temp_table)
    
    spark.sql(f"insert overwrite {data.final_table} select * from {data.temp_table}").show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation

# COMMAND ----------


