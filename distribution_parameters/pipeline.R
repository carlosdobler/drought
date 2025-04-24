
# SET UP ----------------------------------------------------------------------

library(tidyverse)
library(stars)
library(furrr)
library(lmom)

# parallel config
options(future.fork.enable = T)
options(future.rng.onMisuse = "ignore")
plan(multicore)

# load general functions
source("https://raw.github.com/carlosdobler/spatial-routines/master/general_tools.R")



# PROCESS ---------------------------------------------------------------------

# land mask
mask <- 
  st_bbox(c(xmin = -0.125,
            ymin = -90.125,
            xmax = 359.875,
            ymax = 90.125),
          crs = 4326) |> 
  st_as_stars(dx = 0.25) |> 
  st_set_dimensions(c(1,2), names = c("longitude", "latitude")) |> 
  land_mask() |> 
  suppressWarnings()



# ******


## TEMPERATURE ----

# load data
s <- 
  load_data(dir_origin_cloud = "gs://clim_data_reg_useast1/era5/monthly_means/2m_temperature/",
            dir_dest_local = "/mnt/pers_disk/tmp/",
            date_vector = seq(as_date("1991-01-01"), as_date("2020-12-01"), by = "1 month"))

# mask land
s[mask == 0] <- NA

# fit distributions
distr_params(s, 
             dir_tmp_local = "/mnt/pers_disk/tmp",
             dir_output_cloud = "gs://clim_data_reg_useast1/era5/climatologies/",
             distribution = pelgno,
             f_name_root = "era5_2m-temperature_mon_norm-params_1991-2020")



# *****


## PRECIPITATION ----

# load data
s <- 
  load_data(dir_origin_cloud = "gs://clim_data_reg_useast1/era5/monthly_means/total_precipitation/",
            dir_dest_local = "/mnt/pers_disk/tmp/",
            date_vector = seq(as_date("1991-01-01"), as_date("2020-12-01"), by = "1 month"))

# mask land
s[mask == 0] <- NA

# remove zeros to avoid errors when fitting gamma
s[s == 0] <- 1e-10

# fit distributions
distr_params(s, 
             dir_tmp_local = "/mnt/pers_disk/tmp",
             dir_output_cloud = "gs://clim_data_reg_useast1/era5/climatologies/",
             distribution = pelgam,
             f_name_root = "era5_total-precipitation_mon_gamma-params_1991-2020")



