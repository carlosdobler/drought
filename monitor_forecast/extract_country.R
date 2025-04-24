country <- c("Ghana", "Ivory Coast")



library(tidyverse)
library(stars)
library(furrr)


options(future.fork.enable = T)
plan(multicore)

source("https://raw.github.com/carlosdobler/spatial-routines/master/general_tools.R")


countries <- "/mnt/bucket_mine/misc_data/admin_units/ne_110m_admin_0/" |> read_sf()

aoi <-
  countries |> 
  filter(ADMIN %in% country) |> 
  mutate(a = 1) |> 
  select(a)


mask <- 
  aoi|> 
  st_rasterize(st_as_stars(st_bbox(aoi), dx = 0.25, values = NA)) |> 
  st_warp(st_as_stars(st_bbox(), dx = 0.25, values = NA))




dir_tmp <- 
  "/mnt/pers_disk/tmp2/"

fs::dir_create(dir_tmp)


# HISTORICAL (DROUGHT MONITOR)

ff <- rt_gs_list_files("gs://drought-monitor/input_data/raster_monthly")
ff <- ff |> tail(-1)  

dd <- ff |> str_sub(-13,-4) |> as_date()


df_hist <- 
  future_map2_dfr(ff, dd, \(f, d){
    
    f_ <- rt_gs_download_files(f, dir_tmp)
    
    s <- 
      read_ncdf(f_) |> 
      suppressMessages()
    
    a <- 
      s |> 
      st_warp(mask) |>
      c(mask) |> 
      mutate(perc = if_else(is.na(a), NA, perc)) |> 
      select(perc) |> 
      pull() |> 
      mean(na.rm = T) |> 
      round(2)
    
    fs::file_delete(f_)
    
    tibble(percentile = a, date = d)
    
    
  })

df_hist |> 
  select(date, percentile) |> 
  write_csv("/mnt/bucket_mine/misc_data/temporary/drought_ghana-icoast_2000-2025.csv")


df_hist |> 
  ggplot(aes(date, percentile)) +
  geom_line()





# FORECAST

source("monitor_forecast/date_to_proc.R")

f <- str_glue("nmme_ensemble_wb-quantile-stats_mon_{date_to_proc}_plus5.nc")

"gcloud storage cp gs://clim_data_reg_useast1/nmme/monthly/ensemble/{f} {dir_tmp}" |> 
  str_glue() |> 
  system()

s <- 
  read_ncdf(str_glue("{dir_tmp}/{f}")) |> 
  suppressMessages()

df_fcst <- 
  seq(6) |> 
  map_dfr(\(l_in){
    
    df <- 
      s |> 
      slice(time, l_in) |> 
      st_warp(mask) |> 
      c(mask) |> 
      as_tibble() |> 
      filter(!is.na(a)) |> 
      select(-x, -y, -a)
    
    df |> 
      summarise(across(everything(), \(x) round(mean(x, na.rm = T),2))) |> 
      select(percentile = mean, agree)
    
  })
  

df_fcst |> 
  mutate(date = st_get_dimension_values(s, "time") |> as_date(), .before = 1) |>
  write_csv("/mnt/bucket_mine/misc_data/temporary/drought_ghana-icoast_forecast_2025-03-01.csv")




fs::dir_delete(dir_tmp)








