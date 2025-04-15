
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



# *****


wb_calculator <- function(d, s_tas, s_pr, heat_vars) {
  
  # ARGUMENTS:
  # * d = date to process
  # * s_tas = stars obj of average temp; variable should be named tas with units degC
  # * s_pr = stars obj of mean daily precip; variable should be named pr with units m
  # * heat_vars = list obtained with heat_index_generator function
  # * tas_pr = should underlying tas and pr data be provided?
  
  
  # calculate PET
  s_pet <-
    c(s_tas |> units::drop_units(),
      heat_vars$s_hi, heat_vars$s_alpha, pluck(heat_vars$K_mon, month(d))) |>
    
    mutate(tas = if_else(tas < 0, 0, tas),
           pet = K * 16 * (10 * tas / ann_h_ind)^alpha,
           pet = if_else(is.na(pet) | is.infinite(pet), 0, pet),
           pet = pet |> units::set_units(mm) |> units::set_units(m),
           pet = pet/days_in_month(str_glue("1970-{month(d)}-01"))) |>
    select(pet)
  
  
  # calculate water balance
  s_wb <- 
    c(s_pr, s_pet) |> 
    mutate(wb = pr - pet) |> 
    select(wb)
  
  return(s_wb)
  
}



# *****


# function to generate an url to download forecast data from IRI

nmme_url_generator <- function(model, date, variable, lead = 5) {
  
  # ARGUMENTS:
  # - model: model name
  # - date: month to download (only 1)
  # - variable: "tref" or "prec"
  # - lead: number of lead months
  
  
  d <- 
    as_date(date) # format as date if it's not
  
  # subset model row
  src <- 
    df_sources |>
    filter(model == {{model}})
  
  # part of url 
  cast <- 
    case_when(model == "ncep-cfsv2" & date <= src$end_hindcast ~ "HINDCAST/.PENTAD_SAMPLES",
              model == "ncep-cfsv2" & date > src$end_hindcast ~ "FORECAST/.PENTAD_SAMPLES",
              model != "ncep-cfsv2" & date <= src$end_hindcast ~ "HINDCAST",
              model != "ncep-cfsv2" & date > src$end_hindcast ~ "FORECAST")
  
  # glue model part     
  model_cast <- 
    src$model_cast_url |> 
    str_glue()
  
  # glue everything together
  str_glue("https://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.{model_cast}/.MONTHLY/.{variable}/L/%280.5%29%28{lead}.5%29RANGEEDGES/S/%280000%201%20{format(d,'%b')}%20{year(d)}%29VALUES/M/%281.0%29%2810.0%29RANGEEDGES/data.nc")
  
}



# *****



# function to format IRI's ncdfs into a simpler form:
# four dimensions only (lat, lon, member, lead) and with
# existing units

nmme_formatter <- function(f, variable, lead = 5) {
  
  # ARGUMENTS:
  # - f: file name of IRI ncdf
  # - variable: "tref" or "prec"
  # - lead: ...
  
  
  # extract units
  un <- 
    ncmeta::nc_atts(f) |> 
    filter(variable == {{variable}}) |> 
    filter(name == "units") |> 
    pull(value) |> 
    pluck(1)
  
  # read ncdf
  s <- 
    f |> 
    read_mdim() |> 
    suppressMessages() |> 
    suppressWarnings() |> 
    adrop() |> 
    st_set_dimensions("L", values = seq(lead+1)) # simplify lead dimension
  
  
  if (un == "Kelvin_scale") {
    
    s <- 
      s |> 
      mutate(!!sym(variable) := units::set_units(!!sym(variable), K))
    
  }
  
  return(s)
  
}



# *****


# function to write formatted stars obj to ncdf

write_nc <- function(stars_obj, f) {
  
  dims <- vector("list", length(dim(stars_obj)))
  names(dims) <- names(dim(stars_obj))
  
  # define dimensions
  dims[[1]] <- 
    ncdf4::ncdim_def(name = names(st_dimensions(stars_obj)[1]), 
                     units = "degrees_east", 
                     vals = stars_obj |> st_get_dimension_values(1))
  
  dims[[2]] <- 
    ncdf4::ncdim_def(name = names(st_dimensions(stars_obj)[2]), 
                     units = "degrees_north", 
                     vals = stars_obj |> st_get_dimension_values(2))
  
  
  
  extra_dim_names <- names(dim(stars_obj)) |> tail(-2)
  
  for (dimen in extra_dim_names) {
    
    dims[[dimen]] <- ncdf4::ncdim_def(name = dimen, 
                                      units = "", 
                                      vals = seq(dim(stars_obj)[dimen]))
    
  }
  
  
  var_names <- names(stars_obj)
  
  var_units <- map_chr(seq_along(var_names), function(x) {
    
    un <- try(units::deparse_unit(pull(stars_obj)), silent = T)
    if (class(un) == "try-error") un <- ""
    return(un)
    
  })
  
  varis <- 
    purrr::map2(var_names, var_units, 
                ~ncdf4::ncvar_def(name = .x,
                                  units = .y,
                                  dim = dims))
  
  
  
  # create file
  ncnew <- 
    ncdf4::nc_create(filename = f, 
                     vars = varis,
                     force_v4 = TRUE)
  
  # global attribute
  ncdf4::ncatt_put(ncnew,
                   varid = 0,
                   attname = "source code",
                   attval = "https://github.com/carlosdobler/drought/tree/main/monitor_forecast")
  
  
  # write data
  walk(seq_along(var_names),
       ~ncdf4::ncvar_put(nc = ncnew, 
                         varid = varis[[.x]], 
                         vals = stars_obj |> select(all_of(.x)) |> pull()))
  
  
  ncdf4::nc_close(ncnew)
  
}




