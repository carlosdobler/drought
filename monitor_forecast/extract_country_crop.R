level <- 2
admin <- "Texas" #c("Ghana", "Ivory Coast")
crop <- "COTT" # "COCO"



library(tidyverse)
library(stars)
library(furrr)


options(future.fork.enable = T)
plan(multicore)

source("https://raw.github.com/carlosdobler/spatial-routines/master/general_tools.R")

if (level == 1) {
  
  admins <- 
    "/mnt/bucket_mine/misc_data/admin_units/ne_110m_admin_0/" |> 
    read_sf()
  
  aoi <-
    admins |> 
    filter(ADMIN %in% admin)
  
} else if (level == 2) {
  
  admins <- 
    rt_gs_download_files("gs://clim_data_reg_useast1/misc_data/admin_units/gadm_lev_1.gpkg", tempdir())
  
  admins <- 
    admins |> 
    read_sf()
  
  aoi <-
    admins |> 
    filter(NAME_1 %in% admin)
  
}



s_crop <- 
  "/mnt/bucket_mine/misc_data/agriculture/spam/spam2020/production/spam2020_v1r0_global_P_{crop}_A.tif" |> 
  str_glue() |> 
  read_stars() |> 
  setNames("p") |> 
  st_warp(st_as_stars(st_bbox(), dx = 0.25, values = NA))

mask <- 
  s_crop[aoi]

# weights: standardize
mask <- 
  mask |> 
  mutate(p = p/sum(p, na.rm = T))


mask <- 
  mask |> 
  st_warp(st_as_stars(st_bbox(), dx = 0.25, values = NA))




dir_tmp <- 
  "/mnt/pers_disk/tmp2/"

fs::dir_create(dir_tmp)


# HISTORICAL (DROUGHT MONITOR)

ff <- 
  rt_gs_list_files("gs://drought-monitor/historical") |> 
  str_subset(".nc")

dd <- ff |> str_sub(-13,-4) |> as_date()


df_hist <- 
  future_map2_dfr(ff, dd, \(f, d){
    
    # f <- ff[1]
    # d <- dd[1]
    
    f_ <- rt_gs_download_files(f, dir_tmp)
    
    s <- 
      read_ncdf(f_) |> 
      suppressMessages()
    
    a <-
      s |> 
      st_warp(mask) |>
      c(mask) |>
      as_tibble() |> 
      filter(!is.na(p))
    
    a <- 
      a |> 
      mutate(perc = perc * p * nrow(a)) |>
      pull(perc) |> 
      mean(na.rm = T) |> 
      round(2)
    
    fs::file_delete(f_)
    
    tibble(percentile = a, date = d)
    
  })


df_hist |> 
  select(date, percentile) |> 
  write_csv(str_glue("/mnt/bucket_mine/misc_data/temporary/water-bal-weighted_{str_to_lower(admin)}_{str_to_lower(crop)}_2000-2025.csv"))


df_hist |> 
  ggplot(aes(date, percentile)) +
  geom_line() +
  scale_x_date(breaks = "1 year", date_labels = "%Y")





# FORECAST

source("monitor_forecast/date_to_proc.R")

f <- str_glue("nmme_ensemble_wb-quantile-stats_mon_{date_to_proc}_plus5.nc")

"gcloud storage cp gs://clim_data_reg_useast1/nmme/monthly/ensemble/{f} {dir_tmp}" |> 
  str_glue() |> 
  system()

s <- 
  read_ncdf(str_glue("{dir_tmp}/{f}")) |> 
  suppressMessages() |> 
  setNames(c("perc", "agree"))

df_fcst <- 
  seq(6) |> 
  map_dfr(\(l_in){
    
    a <- 
      s |> 
      slice(time, l_in) |> 
      st_warp(mask) |> 
      c(mask) |> 
      as_tibble() |> 
      filter(!is.na(p) & !is.na(perc))
    
    a |>
      mutate(perc = perc * p * nrow(a)) |>
      select(-x, -y, -p) |>
      summarise(across(everything(), \(x) round(mean(x, na.rm = T),2))) |>
      
      # mutate(agree = case_when(agree < 40 ~ "low",
      #                          agree < 70 ~ "moderate",
      #                          TRUE ~ "high")) |> 
      
      select(percentile = perc, agree)
    
  })

df_fcst <- 
  df_fcst |> 
  mutate(date = st_get_dimension_values(s, "time") |> as_date(), .before = 1)

df_fcst |> 
  write_csv(str_glue("/mnt/bucket_mine/misc_data/temporary/water-bal-weighted_{str_to_lower(admin)}_{str_to_lower(crop)}_forecast_{date_to_proc}.csv"))


ggplot(mapping = aes(x = date, y = percentile)) +
  geom_line(data = df_hist |> filter(year(date) >= 2020)) +
  geom_point(data = df_fcst) +
  scale_x_date(breaks = "1 year", date_labels = "%Y")



fs::dir_delete(dir_tmp)








