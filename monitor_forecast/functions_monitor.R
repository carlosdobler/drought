
heat_index_var_generator <- function() {
  
  # download climatological annual heat index file
  # (generated with `monitor_forecast/annual_heat_index_era.R` script)
  str_glue("gcloud storage cp gs://clim_data_reg_useast1/era5/climatologies/era5_heat-index_yr_1991-2020.nc {tempdir()}") |>  
    system(ignore.stderr = T, ignore.stdout = T)
  
  # read file
  s_hi <- 
    str_glue("{tempdir()}/era5_heat-index_yr_1991-2020.nc") %>% 
    read_ncdf() %>% 
    suppressMessages() |> 
    mutate(ann_h_ind = round(ann_h_ind, 2))
  
  # delete file
  fs::file_delete(str_glue("{tempdir()}/era5_heat-index_yr_1991-2020.nc"))
  
  
  # calculate alpha
  s_alpha <- 
    s_hi %>% 
    mutate(alpha = (6.75e-7 * ann_h_ind^3) - (7.71e-5 * ann_h_ind^2) + 0.01792 * ann_h_ind + 0.49239) %>% 
    select(alpha)
  
  
  # calculate daylight duration (one for each calendar month)
  # reference for coefficients:
  # https://github.com/sbegueria/SPEI/blob/master/R/thornthwaite.R#L128-L140
  
  K_mon <- 
    map2(seq(15,365, by = 30), # Julian day (mid-point)
         c(31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31), # days/month
         \(J, days_in_mon){
           
           s_hi %>% 
             st_dim_to_attr(2) %>% 
             mutate(tanLat = tan(latitude/57.2957795),
                    Delta = 0.4093 * sin(((2 * pi * J) / 365) - 1.405),
                    tanDelta = tan(Delta),
                    tanLatDelta = tanLat * tanDelta,
                    tanLatDelta = ifelse(tanLatDelta < (-1), -1, tanLatDelta),
                    tanLatDelta = ifelse(tanLatDelta > 1, 1, tanLatDelta),
                    omega = acos(-tanLatDelta),
                    N = 24 / pi * omega,
                    K = N / 12 * days_in_mon / 30) %>% 
             select(K)
           
         })
  
  r <- list(s_hi = s_hi, s_alpha = s_alpha, K_mon = K_mon)
  
  return(r)
  
}




wb_calculator <- function(d, dir, heat_vars) {
  
  # ARGUMENTS:
  # * d = date to process
  # * dir = path where ERA5 temperature and precip data are stored
  # * heat_vars = list obtained with heat_index_generator function
  
  
  # read tmp data > format
  s_tas <-
    read_ncdf(str_glue("{dir}/era5_2m-temperature_mon_{as_date(d)}.nc")) |>
    suppressMessages() |>
    adrop() |> 
    mutate(t2m = t2m %>% units::set_units(degC)) %>%
    units::drop_units()
  
  
  # calculate PET
  s_pet <-
    c(s_tas, heat_vars$s_hi, heat_vars$s_alpha, pluck(heat_vars$K_mon, month(d))) |>
    
    mutate(t2m = if_else(t2m < 0, 0, t2m),
           pet = K * 16 * (10 * t2m / ann_h_ind)^alpha,
           pet = if_else(is.na(pet) | is.infinite(pet), 0, pet),
           pet = pet |> units::set_units(mm) |> units::set_units(m),
           pet = pet/days_in_month(str_glue("1970-{month(d)}-01"))) |>
    select(pet)
  
  
  # read precip data
  s_pr <- 
    read_ncdf(str_glue("{dir_tmp}/era5_total-precipitation_mon_{as_date(d)}.nc")) |>
    suppressMessages() |>
    adrop()
  
  
  # calculate water balance
  s_wb <- 
    c(s_pr, s_pet) |> 
    mutate(wb = tp - pet) |> 
    select(wb)
  
  return(s_wb)
  
}
