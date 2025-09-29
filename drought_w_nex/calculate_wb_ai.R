
scenario <- "ssp585"






library(tidyverse)
library(stars)
library(furrr)
box::use(../functions/general_tools[...])

options(future.fork.enable = T)
options(future.rng.onMisuse = "ignore")
plan(multicore)


dir_tmp <- "/mnt/pers_disk/tmp"
fs::dir_create(dir_tmp)


# load elevation
s_z <- 
  rt_gs_download_files("gs://clim_data_reg_useast1/era5/era5_geopotential.nc",
                       dir_tmp,
                       quiet = T) |> 
  read_ncdf() |> 
  suppressMessages() |> 
  adrop() |> 
  units::drop_units() |>  # masl
  mutate(z = if_else(z > 10000, 10000, z))

# calculate gamma
s_gamma <- 
  s_z |>
  mutate(P = 101.3 * ((293 - 0.0065 * z) / 293)^5.26,
         gamma = 1.013e-3 * P / (0.622 * 2.45)) |>  
  select(gamma)

# gamma <- 
#   rep(list(gamma), 12) %>% 
#   {do.call(c, c(., along = "time"))}

# load extraterrestrial radiation
s_tisr <- 
  "drought_w_nex/tisr_cal_month.nc" |> 
  read_ncdf() |> 
  suppressMessages() # MJ/m^2









vars <- c("tasmax",
          "tasmin",
          "hurs",
          "sfcWind",
          "rsds",
          "pr")

yrs <- seq(1970,2099)

df_nex_files <- 
  read_csv("drought_w_nex/df_nex_files.csv") |> 
  filter(year >= first(yrs),
         year <= last(yrs)) |>
  filter(scenario %in% c("historical", {{scenario}}))

models <- 
  df_nex_files |> 
  pull(model) |> 
  unique()


for (model in models[-19] |> tail(-13)) {
  
  message(str_glue("  PROCESSING MODEL {which(model == models)} / {length(models)} ({model})"))
  
  
  df_nex_files_model <- 
    df_nex_files |> 
    filter(model == {{model}}) |> 
    mutate(path = str_glue("https://nex-gddp-cmip6.s3-us-west-2.amazonaws.com/{path}"))
  
  
  if (!all(vars %in% unique(df_nex_files_model$variable))) {
   
    message(str_glue("    model does not have all variables! skipping...")) 
    next
  }
  
  s_ref <- 
    df_nex_files_model |> 
    head(1) |> 
    pull(path) 
  
  download.file(s_ref,
                str_glue("{dir_tmp}/{fs::path_file(s_ref)}"),
                method = "wget", 
                quiet = T)
  
  s_ref <- 
    str_glue("{dir_tmp}/{fs::path_file(s_ref)}") |> 
    read_ncdf(proxy = T) |> 
    suppressMessages()
  
  s_z_nex_grid <- 
    s_z |> 
    st_warp(st_as_stars(st_bbox(), dx = 0.25)) |> 
    st_warp(st_as_stars(dimensions = st_dimensions(s_ref)[1:2]))
  
  s_gamma_nex_grid <- 
    s_gamma |> 
    st_warp(st_as_stars(st_bbox(), dx = 0.25)) |> 
    st_warp(st_as_stars(dimensions = st_dimensions(s_ref)[1:2]))
  
  s_tisr_nex_grid <- 
    s_tisr |> 
    st_warp(st_as_stars(st_bbox(), dx = 0.25)) |> 
    st_warp(st_as_stars(dimensions = st_dimensions(s_ref)[1:2]))
  
  
  
  
  walk(yrs, \(yr){
    
    # yr = yrs[11]
    
    message(str_glue("  year {yr}"))
    message(str_glue("    downloading files + aggregating"))
    
    f_vars <- 
      
      future_map(vars |> set_names(), \(var){
        
        f <- 
          df_nex_files_model |> 
          filter(year == {{yr}}) |> 
          filter(variable == {{var}}) |> 
          pull(path)
        
        download.file(f,
                      str_glue("{dir_tmp}/{fs::path_file(f)}"),
                      method = "wget", 
                      quiet = T)
        
        f <- 
          str_glue("{dir_tmp}/{fs::path_file(f)}")
        
        
        # aggregate to monthly
        f_mon <- str_glue("{str_replace(f, '.nc', '_mon.nc')}")
        
        str_glue("cdo monmean {f} {f_mon}") |> 
          system(ignore.stdout = T, ignore.stderr = T)
        
        return(f_mon)  
        
      })
    
    
    message(str_glue("    importing + converting units"))
    
    s_vars <- 
      f_vars |> 
      future_imap(\(f, i){
        
        # message(str_glue("       {i}"))
        
        s <- 
          f |> 
          read_ncdf(proxy = F) |> 
          suppressMessages() |> 
          suppressWarnings()
        
        un <- 
          s |> 
          pull() |> 
          units::deparse_unit()
        
        # s <- 
        #   s |> 
        #   aggregate(by = "1 month", FUN = mean) |> 
        #   aperm(c(2,3,1))
        
        if (un == "K") {
          
          v <- 
            names(s)
          
          s <- 
            s |> 
            mutate(!!sym(v) := units::set_units(!!sym(v), degC))
          
        } else if (un == "W m-2") {
          
          v <- 
            names(s)
          
          s <- 
            s |> 
            mutate(!!sym(v) := units::set_units(!!sym(v), MJ/d/m^2))
          
        } else if (un == "kg m-2 s-1") {
          
          v <- 
            names(s)
          
          s <- 
            s |> 
            mutate(!!sym(v) := units::set_units(!!sym(v), kg/m^2/d))
          
        }
        
        return(s)
        
      })
    
    
    
    
    message(str_glue("    calculating pet, wb, and ai"))
    
    
    
    s_PET <- 
      seq(12) |> 
      map(\(mon){
        
        # mon = 1
        
        s_vars_f <- 
          s_vars[which(names(s_vars) != "pr")] |>
          map(slice, time, mon) |> 
          c(z = list(s_z_nex_grid),
            gamma = list(s_gamma_nex_grid),
            tisr = list(s_tisr_nex_grid |> slice(cal_month, mon)))
        
        s_vars_f <- 
          do.call(c, s_vars_f) |> 
          units::drop_units() |> 
          mutate(
            tasmean = (tasmax + tasmin) / 2,
            
            e_tmax = 0.6108 * exp((17.27 * tasmax) / (tasmax + 237.3)),
            e_tmin = 0.6108 * exp((17.27 * tasmin) / (tasmin + 237.3)),
            es = (e_tmax + e_tmin) / 2,
            
            hurs = ifelse(hurs < 0, 0, hurs),
            ea = es * hurs / 100,
            
            delta = 4098 * es / (tasmean + 237.3)^2,
            
            # fix some errors in NEX (outliers)
            rsds = if_else(rsds < 0.00001, 0.00001, rsds),
            
            Rns = (1 - 0.23) * rsds,
            
            Rso = (0.75 + 2e-5 * z) * tisr,
            Rso = ifelse(Rso == 0, 1e-10, Rso),
            Rnl = 4.903e-9 * ((tasmax + 273.16)^4 + (tasmin + 273.16)^4) / 2 *
              (0.34 - 0.14 * sqrt(ea)) *
              (1.35 * rsds / Rso - 0.35),
            
            Rn = Rns - Rnl,
            
            G = 0.3 * Rn) |>  
          
          select(delta, Rn, tasmean, gamma, sfcWind, es, ea, G)
        
        s_ET <- 
          s_vars_f |> 
          mutate(
            
            numerator = 0.408 * delta * (Rn - G) + gamma * (900 / (tasmean + 273)) * sfcWind * (es - ea),
            
            denominator = delta + gamma * (1 + 0.34 * sfcWind),
            
            ET = numerator/denominator,
            
            ET = ifelse(ET < 0, 0, ET)
            
          ) |>  
          select(ET) |> 
          mutate(ET = ET |> units::set_units(mm)) # daily mean
        
        return(s_ET)
        
      })
    
    
    s_PET <- 
      do.call(c, c(s_PET, along = "time")) |> 
      st_set_dimensions("time", values = str_glue("{yr}-{seq(12)}-01") |> as_date())
    
    f_res <- str_glue("{dir_tmp}/pet-pm_mon_{model}_NEX_{yr}.nc")
    
    rt_write_nc(s_PET,
                f_res,
                gatt_name = "source_code",
                gatt_val = "https://github.com/carlosdobler/drought/drought_w_nex/")  
    
    str_glue("gcloud storage mv {f_res} gs://clim_data_reg_useast1/nex/monthly_aggregates/potential_evapotranspiration_pm/{model}/") |> 
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    
    
    
    # WATER BALANCE AND ARIDITY INDEX
    
    st_dimensions(s_vars$pr) <- st_dimensions(s_PET)
    
    
    s_WB <- units::drop_units(s_vars$pr) - units::drop_units(s_PET)
    
    s_WB <- 
      s_WB |> 
      setNames("wb") |> 
      mutate(wb = wb |> units::set_units("mm")) # daily mean
    
    f_res <- str_glue("{dir_tmp}/wb-pm_mon_{model}_NEX_{yr}.nc")
    
    rt_write_nc(s_WB,
                f_res,
                gatt_name = "source_code",
                gatt_val = "https://github.com/carlosdobler/drought/drought_w_nex/")  
    
    str_glue("gcloud storage mv {f_res} gs://clim_data_reg_useast1/nex/monthly_aggregates/water_balance_pm/{model}/") |> 
      system(ignore.stdout = T, ignore.stderr = T)
    
    # ******
    
    s_AI <- units::drop_units(s_vars$pr)/units::drop_units(s_PET)
    
    s_AI <- 
      s_AI |> 
      setNames("aridity") |> 
      mutate(aridity = if_else(is.infinite(aridity), NA, aridity)) # PET = 0
    
    f_res <- str_glue("{dir_tmp}/ai-pm_mon_{model}_NEX_{yr}.nc")
    
    rt_write_nc(s_AI,
                f_res,
                gatt_name = "source_code",
                gatt_val = "https://github.com/carlosdobler/drought/drought_w_nex/")  
    
    str_glue("gcloud storage mv {f_res} gs://clim_data_reg_useast1/nex/monthly_aggregates/aridity_index_pm/{model}/") |> 
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    fs::dir_ls(dir_tmp) |> 
      fs::file_delete()
    
  })
  
}



fs::dir_delete(dir_tmp)

# ERROR IN DATA NOTES:
# - tasmin 1980 of model HadGEM3-GC31-MM is all 0
