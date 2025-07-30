
# NEEDS TO BE UPDATED TO REFLECT CHANGES IN era_monitor_generator.R



# SCRIPT TO CALCULATE MONTHLY DROUGHT WITH ERA5 
# BASED ON A MODIFIED SPEI METHODOLOGY 
# (USES THORNWHAITES FORMULATION TO CALCULATE PET)





library(tidyverse)
library(stars)
library(furrr)

options(future.fork.enable = T)
options(future.rng.onMisuse = "ignore")

plan(multicore)

# special functions
source("https://raw.github.com/carlosdobler/spatial-routines/master/general_tools.R")
source("misc/functions_drought.R")


# temporary directory
dir_tmp <- "/mnt/pers_disk/tmp"
# if (fs::dir_exists(dir_tmp)) fs::dir_delete(dir_tmp) # clean slate
fs::dir_create(dir_tmp)

# root bucket dir
dir_gs <- "gs://clim_data_reg_useast1/era5"



time_vector_full <- 
  seq(as_date("1991-01-01"),
      as_date("2025-03-01"),
      by = "1 month")


vars <- c("total_precipitation", "2m_temperature")


# DOWNLOAD

# tas and precip data

vars |> 
  map(\(var){
    rt_gs_list_files(str_glue("{dir_gs}/monthly_means/{var}")) |> 
      
      # one year prior for rollsums
      str_subset(str_flatten((year(first(time_vector_full))-1):year(last(time_vector_full)), "|"))
  }) |> 
  walk(rt_gs_download_files, dir_tmp)





k = 12 # INTEGRATION WINDOW


# distribution parameters

ff_dist <- 
  rt_gs_list_files(str_glue("{dir_gs}/climatologies/")) |> 
  str_subset(str_glue("water-balance-th-rollsum{k}")) 
  
ff_dist <- 
  ff_dist |> 
  rt_gs_download_files(dir_tmp)
  

# load parameters
ss_dist_params <- 
  ff_dist |> 
  map(read_ncdf) |> 
  suppressMessages()



# HEAT INDEX CONSTANTS

heat_vars <- heat_index_var_generator()



for(date_to_proc in time_vector_full) {
  
  date_to_proc <- as_date(date_to_proc)
  print(date_to_proc)
  
  tri_month <- 
    seq(as_date(date_to_proc)-months(k-1), as_date(date_to_proc), by = "1 month")
  
  # calculate wb (rollsum)
  
  # block 1
  source(textConnection(readLines("monitor_forecast/era_monitor_generator.R")[184:227]))
  
  
  # calculate anomaly
  
  s_perc <- 
    c(s_wb_rolled, ss_dist_params[[month(date_to_proc)]]) |> 
    merge() |>
    st_apply(c(1,2), \(x){
      
      if(any(is.na(x))) {
        NA
      } else {
        
        lmom::cdfglo(x[1], c(x[2], x[3], x[4]))
        
      }
      
    },
    FUTURE = T,
    .fname = "perc")
  
  
  # save
  
  # block 2
  source(textConnection(readLines("monitor_forecast/era_monitor_generator.R")[276:295]))
  
  
}

fs::dir_delete(dir_tmp)  






# time_vector_full |> sample(1) -> d
# res_file <- str_glue("era5_water-balance-perc-w3_bl-1991-2020_mon_{d}.nc")
# "gcloud storage cp gs://drought-monitor/historical/{res_file} {tempdir()}" |>
#   str_glue() |>
#   system()
# 
# str_glue("{tempdir()}/{res_file}") |>
#   read_ncdf() -> s
# 
# s |>
#   st_warp(st_as_stars(st_bbox(), dx = 0.25)) |>
# 
#   as_tibble() |>
#   ggplot(aes(x, y, fill = perc)) +
#   geom_raster() +
#   colorspace::scale_fill_continuous_divergingx("Spectral",
#                                                mid = 0.5,
#                                                na.value = "transparent",
#                                                limits = c(0,1)) +
#   labs(title = d)






