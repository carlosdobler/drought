
scenario <- "ssp585"
baseline <- seq(1991,2020)
int_windws <- c(3,12)






library(tidyverse)
library(stars)
library(furrr)
box::use(../functions/general_tools[...],
         ../functions/tile[...])

options(future.fork.enable = T)
options(future.rng.onMisuse = "ignore")
plan(multicore)


dir_tmp <- "/mnt/pers_disk/tmp"
dir_res <- "/mnt/pers_disk/res"

fs::dir_create(dir_tmp)
fs::dir_create(dir_res)


# walk(int_windws, \(windw){
#   
#   fs::dir_create(str_glue("{dir_tiles}/w{windw}"))
#   
# })


yrs <- seq(1970,2099)

model_dirs <- 
  rt_gs_list_files("gs://clim_data_reg_useast1/nex/monthly_aggregates/water_balance_pm_quantile/")

