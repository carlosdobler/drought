
# crop <- "coffee"
crop <- "cotton"



library(tidyverse)
library(stars)

s_era <- 
  "/mnt/bucket_mine/era5/annual_aggregates/average_temperature/era5_average-temperature_yr_1971-01-01.nc" |> 
  read_ncdf()


s_crop <- 
  "/mnt/bucket_mine/misc_data/agriculture/spam/spam2020/production/spam2020_v1r0_global_P_{crop |> str_sub(end = 4) |> str_to_upper()}_A.tif" |> 
  str_glue() |> 
  read_stars() |> 
  setNames("p") |> 
  mutate(p = if_else(is.na(p), 0, p)) |> 
  st_warp(st_as_stars(st_bbox(), dx = 0.25, values = NA), # s_era
          use_gdal = T,
          method = "sum") |> 
  setNames("p") |> 
  mutate(p = if_else(p == 0, NA, p)) |> 
  st_warp(s_era)

write_stars(s_crop,
            str_glue("crop_production_regridded_{crop}.tif"))

str_glue("gcloud storage mv crop_production_regridded_{crop}.tif gs://drought-monitor/spam/") |> 
  system()


