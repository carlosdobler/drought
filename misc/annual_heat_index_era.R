
# SCRIPT TO CALCULATE ANNUAL HEAT INDEX:
# VARIABLE USED IN THE CALCULATION OF THORNWHAITE'S PET

# Thornwhaite's formulation entails using the mean temperature of the year
# of the month of which PET is to be obtained. Because the drought monitor
# needs to be calculated before the year ends (and thus, before the annual
# mean temperature can be obtained), this script uses the climatological 
# mean temperature from 1991 to 2020.




library(tidyverse)
library(stars)
library(furrr)

options(future.fork.enable = T)
plan(multicore)


# load function to save ncdfs
source("https://raw.github.com/carlosdobler/spatial-routines/master/general_tools.R")

# key directories
dir_gs <- "gs://clim_data_reg_useast1/era5/monthly_means"
dir_data <- "/mnt/pers_disk/tmp"
fs::dir_delete(dir_data)
fs::dir_create(dir_data)


# list all files > subset relevant years
ff <- 
  rt_gs_list_files(str_glue("{dir_gs}/2m_temperature/")) |> 
  str_subset(str_flatten(1991:2020, "|"))


# download files
ff <- 
  rt_gs_download_files(ff, dir_data)



s_monthly_h_ind <- 
  
  # loop through calendar months
  seq(12) |> 
  str_pad(2, "left", "0") |>  
  
  map(\(mon){
    
    # read data
    s <- 
      ff |> 
      str_subset(str_glue("-{mon}-")) |> 
      future_map(read_ncdf, proxy = F) |>  
      suppressMessages() |>  
      map(adrop)
    
    # concatenate
    s <- 
      do.call(c, c(s, along = "time"))
    
    # change units > aggregate (mean)
    s_mon <- 
      s |>
      mutate(t2m = t2m |> units::set_units(degC)) |> 
      st_apply(c(1,2), mean, .fname = "t2m", FUTURE = T)
    
    # heat index
    s_h_ind <- 
      s_mon |> 
      mutate(i = if_else(t2m < 0, 0, (t2m/5)^1.514)) |> 
      select(i)
    
    return(s_h_ind)
    
  })


# aggregate all months into annual
s_ann_h_ind <- 
  do.call(c, c(s_monthly_h_ind, along = "mon")) |> 
  st_apply(c(1,2), sum, .fname = "ann_h_ind", FUTURE = T)


# save result
res_file <- "era5_heat-index_yr_1991-2020.nc"

rt_write_nc(s_ann_h_ind,
            str_glue("{dir_data}/{res_file}"),
            gatt_name = "source code",
            gatt_val = "https://github.com/carlosdobler/drought")

# upload to cloud
str_glue("gcloud storage mv {dir_data}/{res_file} gs://clim_data_reg_useast1/era5/climatologies/") |> 
  system()

# clean up
fs::dir_delete(dir_data)




