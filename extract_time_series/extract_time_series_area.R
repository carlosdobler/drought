

library(tidyverse)
library(stars)
library(furrr)
sf_use_s2(F)

options(future.fork.enable = T)
plan(multicore)

box::use(../functions/general_tools[...])

import_areas <- function(country = NA){
  
  if (is.na(country)) {
    
    geom <- 
      "/mnt/bucket_mine/misc_data/admin_units/ne_110m_admin_0/" |> 
      read_sf(quiet = T) |> 
      rename(name = ADMIN) |> 
      select(name)
    
  } else {
    
    temp_file <- 
      rt_gs_download_files("gs://clim_data_reg_useast1/misc_data/admin_units/gadm_lev_1.gpkg", 
                                         tempdir())
    
    geom <- 
      temp_file |> 
      read_sf(quiet = T) |>
      filter(COUNTRY == country) |>
      rename(name = NAME_1) |>
      select(name)
      
    fs::file_delete(temp_file)
    
  }
  
  print(str_flatten(sort(geom$name), " | "))
  return(geom)
  
}



# areas <- import_areas()
areas <- import_areas("United States")

# PARAMETER 1 **************

# area <- "Brazil"
area <- "Texas"

# **************************


area_geom <- 
  areas |> 
  filter(name == area)



# PARAMETER 2 *************

# crop <- NA
# crop <- "coffee"
crop <- "cotton"

# *************************


if (is.na(crop)) {
  
  mask <- 
    area_geom |> 
    mutate(p = 1) |> 
    select(p) |> 
    st_rasterize(st_as_stars(st_bbox(), dx = 0.25, values = NA))

} else {
  
  f <- str_glue("crop_production_regridded_{crop}.tif")
  
  str_glue("gcloud storage cp gs://drought-monitor/spam/{f} .") |> 
    system()
  
  s_crop <- 
    f |>  
    read_stars() |> 
    setNames("p")
  
  mask <- 
    s_crop[st_shift_longitude(area_geom)]
  
  mask <- 
    mask |> 
    mutate(p = p/sum(p, na.rm = T))
  
  fs::file_delete(f)
  
}





# PARAMETER 3 ************

windw <- 3

# ************************




dir_tmp <- 
  "/mnt/pers_disk/tmp2/"

fs::dir_create(dir_tmp)


# HISTORICAL (DROUGHT MONITOR)

ff <- 
  rt_gs_list_files("gs://drought-monitor/historical") |> 
  str_subset(".nc") |> 
  str_subset(str_glue("-w{windw}_"))

dd <- ff |> str_sub(-13,-4) |> as_date()


s_ref <- 
  ff[1] |>
  rt_gs_download_files(dir_tmp, quiet = T) |> 
  read_ncdf()

mask_regrid <- 
  mask |> 
  st_warp(s_ref)




df_hist <- 
  future_map2_dfr(ff, dd, \(f, d){
    
    # f <- ff[1]
    # d <- dd[1]
    
    f_ <- rt_gs_download_files(f, dir_tmp, quiet = T)
    
    s <- 
      read_ncdf(f_) |> 
      suppressMessages()
    
    a <-
      mask_regrid |> 
      c(s) |> 
      as_tibble() |> 
      filter(!is.na(p))
    
    if (!is.na(crop)) {
      
      a <- 
        a |> 
        mutate(perc = perc * p * nrow(a))
      
    }
    
    a <- 
      a |> 
      pull(perc) |> 
      mean(na.rm = T) |> 
      round(2)
    
    fs::file_delete(f_)
    
    tibble(date = d, percentile = a)
    
  })


if (is.na(crop)) {
  f_res <- str_glue("water-bal_w{windw}_{str_to_lower(area)}_{first(df_hist$date)}_{last(df_hist$date)}.csv")
} else {
  f_res <- str_glue("water-bal-weighted_w{windw}_{str_to_lower(area)}_{crop}_{first(df_hist$date)}_{last(df_hist$date)}.csv")
} 

df_hist |> 
  write_csv(str_glue("/mnt/bucket_mine/misc_data/temporary/{f_res}"))


df_hist |> 
  ggplot(aes(date, percentile)) +
  geom_line() +
  scale_x_date(breaks = "1 year", date_labels = "%Y") +
  scale_y_continuous(limits = c(0,1))





# FORECAST

source("monitor_forecast/date_to_proc.R")

f <- str_glue("nmme_ensemble_water-balance-perc-w{windw}_mon_{date_to_proc}_plus5.nc")

"gcloud storage cp gs://drought-monitor/forecast/{f} {dir_tmp}" |> 
  str_glue() |> 
  system()

s <- 
  read_ncdf(str_glue("{dir_tmp}/{f}")) |> 
  suppressMessages()

df_fcst <- 
  seq(6) |> 
  map_dfr(\(l_in){
    
    s_1l <- 
      s |> 
      slice(L, l_in)
    
    a <- 
      mask_regrid |> 
      c(s_1l) |> 
      as_tibble() |> 
      filter(!is.na(p) & !is.na(mean))
    
    
    if(!is.na(crop)) {
      
      a <- 
        a |>
        mutate(across(contains("%"), \(x) x * p * nrow(a)))
      
    }
    
    a <- 
      a |>
      select(-longitude, -latitude, -p) |>
      summarise(across(everything(), \(x) round(mean(x, na.rm = T),2))) |>
      
      # mutate(agree = case_when(agree < 40 ~ "low",
      #                          agree < 70 ~ "moderate",
      #                          TRUE ~ "high")) |> 
      
      select(percentile = `50%`, 
             lower_60 = `20%`,
             upper_60 = `80%`,
             lower_90 = `5%`, 
             upper_90 = `95%`)
    
  })

df_fcst <- 
  df_fcst |> 
  mutate(date = st_get_dimension_values(s, "L") |> as_date(), .before = 1)

if (is.na(crop)) {
  f_res <- str_glue("water-bal_w{windw}_{str_to_lower(area)}_forecast_{first(df_fcst$date)}_{last(df_fcst$date)}.csv")
} else {
  f_res <- str_glue("water-bal-weighted_w{windw}_{str_to_lower(area)}_{crop}_forecast_{first(df_fcst$date)}_{last(df_fcst$date)}.csv")
} 

df_fcst |> 
  write_csv(str_glue("/mnt/bucket_mine/misc_data/temporary/{f_res}"))


df_fcst_2 <- 
  df_fcst |> 
  bind_rows(tibble(date = df_hist |> last() |> pull(date), 
                   percentile = df_hist |> last() |> pull(percentile),
                   lower_60 = df_hist |> last() |> pull(percentile),
                   upper_60 = df_hist |> last() |> pull(percentile),
                   lower_90 = df_hist |> last() |> pull(percentile),
                   upper_90 = df_hist |> last() |> pull(percentile)))

ggplot(mapping = aes(x = date, y = percentile)) +
  geom_line(data = df_hist |> filter(year(date) >= 2023)) +
  geom_point(data = df_hist |> filter(year(date) >= 2023)) +
  
  geom_ribbon(data = df_fcst_2, 
              aes(ymin = lower_90, ymax = upper_90), fill = "red", alpha = 0.2) +
  geom_ribbon(data = df_fcst_2, 
              aes(ymin = lower_60, ymax = upper_60), fill = "red", alpha = 0.2) +
  geom_line(data = df_fcst_2, linetype = "3333") +
  geom_point(data = df_fcst_2) +
  scale_x_date(breaks = "1 year", minor_breaks = "1 month", date_labels = "%Y") +
  scale_y_continuous(limits = c(0,1))



fs::dir_delete(dir_tmp)








