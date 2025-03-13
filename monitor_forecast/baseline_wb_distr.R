
# SCRIPT TO CALCULATE THE BASELINE (1991-2020) WATER 
# BALANCE DISTRIBUTION PARAMETERS



library(tidyverse)
library(stars)
library(furrr)

options(future.fork.enable = T)
options(future.rng.onMisuse = "ignore")
plan(multicore)


# load special functions
source("https://raw.github.com/carlosdobler/spatial-routines/master/general_tools.R")
source("monitor_forecast/functions_monitor.R")


# temporary directory to save files
dir_tmp <- "/mnt/pers_disk/tmp"
if (fs::dir_exists(dir_tmp)) fs::dir_delete(dir_tmp)
fs::dir_create(dir_tmp)


# root cloud path
dir_gs <- "gs://clim_data_reg_useast1/era5"


# date vector
date_vector <- 
  # start in 1990 to accomodate rolling sums
  seq(as_date("1990-01-01"), as_date("2020-12-01"), by = "1 month")




# 1. DOWNLOAD DATA ----

vars <- c("total_precipitation", "2m_temperature")

walk(vars, \(var) {
  
  ff <- 
    rt_gs_list_files(str_glue("{dir_gs}/monthly_means/{var}")) |> 
    str_subset(str_flatten(seq(year(first(date_vector)), year(last(date_vector))), "|"))
  
  ff <- 
    rt_gs_download_files(ff, dir_tmp)
  
})




# 2. HEAT INDEX VARIABLES ----

heat_vars <- heat_index_var_generator()



# 3. CALCULATE WATER BALANCE ----

s_wb <- 
  date_vector |> 
  future_map(wb_calculator, dir = dir_tmp, heat_vars = heat_vars)

s_wb <- 
  do.call(c, c(s_wb, along = "time")) |>  
  st_set_dimensions("time", values = seq(as_date("1990-01-01"), as_date("2020-12-01"), by = "1 month"))
  


# 4. LAND MASK ----

f_land <- "gs://clim_data_reg_useast1/misc_data/physical/ne_50m_land"

str_glue("gcloud storage cp -r {f_land} {tempdir()}") |> system()


land_pol <- 
  str_glue("{tempdir()}/ne_50m_land") |> 
  st_read()

fs::dir_delete(str_glue("{tempdir()}/ne_50m_land"))

# extract centroid coordinates
centr <- 
  land_pol |> 
  st_centroid() |> 
  st_coordinates() |> 
  as_tibble()

# filter our antarctica 
# (centroid < -60 latitude)
# rasterize
land_r <- 
  land_pol %>%
  mutate(Y = centr$Y) |> 
  filter(Y > -60) |> 
  mutate(a = 1) %>% 
  select(a) %>% 
  st_rasterize(st_as_stars(st_bbox(),
                           dx = 0.1,
                           values = 0))

# warp raster
land_r <- 
  land_r %>% 
  st_warp(s_wb |> slice(time, 1),
          method = "max",
          use_gdal = T) %>% 
  setNames("land")

# format dimensions
st_dimensions(land_r) <- st_dimensions(s_wb)[1:2]

# mask out oceans in water balance data
s_wb[land_r == 0] <- NA



# 5. FIT DISTRIBUTION -----

# loop through months
walk(seq(12), \(mon) {
  
  m <- 
    # loop through years
    map(seq(year(first(date_vector))+1, year(last(date_vector))), \(yr){
      
      print(str_glue("month: {mon}  |  year: {yr}"))
      
      d <- as_date(str_glue("{yr}-{mon}-01"))
      
      # subset 1 month and 2 prior > aggregate (rollsum)
      s_wb |> 
        filter(time %in% seq(d-months(2), d, by = "1 month")) |> 
        st_apply(c(1,2), sum, .fname = "wb_rollsum3", FUTURE = T) |> 
        mutate(wb_rollsum3 = wb_rollsum3 |> units::set_units(m))
      
    })
  
  
  # concatenate
  m <- 
    do.call(c, c(m, along = "yr"))
  
  # fit distribution (gamma)
  r <- 
    m |> 
    units::drop_units() |> 
    st_apply(c(1,2), \(x) {
      
      if(any(is.na(x))) {
        
        params <- c(shift = NA, alpha = NA, beta = NA)
        
      } else {
        
        # calculate shift if the cell has negative values
        shift <- if_else(any(x < 0), abs(min(x)) + 1e-10, 0)
        
        # l-moments of data with shift
        lmoms <- lmom::samlmu(x + shift)
        
        # save parameters and shift (to apply to future data)
        params <-
          c(shift = shift, lmom::pelgam(lmoms))
        
      }
      
      return(params)  
      
    },
    FUTURE = T,
    .fname = "params") |> 
    split("params")
  
  
  # save results
  f <- 
    str_glue("{dir_tmp}/era5_water-balance-th-rollsum3_mon_gamma-params_1991-2000_{str_pad(mon, 2, 'left', '0')}.nc")
  
  rt_write_nc(r, f)
  
  # move to cloud
  str_glue("gcloud storage mv {f} gs://clim_data_reg_useast1/era5/climatologies/") |> 
    system()
  
  
})


# clean up
fs::dir_delete(dir_tmp)




