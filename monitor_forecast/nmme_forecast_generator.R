

# SCRIPT TO DOWNLOAD AND PRE-PROCESS NMME DATA TO GENERATE A FORECAST OF 
# WATER BALANCE ANOMALIES. THE RESULTING FORECAST HAS A 6-MONTH LEAD AT 
# A MONTHLY RESOLUTION. 

# PRE-PROCESSING STEPS INCLUDE BIAS CORRECTION WITH ERA5 DATA.






# SETUP -----------------------------------------------------------------------


library(tidyverse)
library(stars)
library(furrr)

options(future.fork.enable = T)
plan(multicore)


# load special functions
source("https://raw.github.com/carlosdobler/spatial-routines/master/general_tools.R")
source("monitor_forecast/functions.R")

# load date to process
source("monitor_forecast/date_to_proc.R")

# load nmme data source table
source("monitor_forecast/nmme_sources_df.R")

# temporary directory to save files
dir_tmp <- "/mnt/pers_disk/tmp"
# if (fs::dir_exists(dir_tmp)) fs::dir_delete(dir_tmp) # clean slate
fs::dir_create(dir_tmp)

# key bucket directories
dir_gs_era <- "gs://clim_data_reg_useast1/era5"
dir_gs_nmme <- "gs://clim_data_reg_useast1/nmme/climatologies"

# date to process plus 5 lead months
date_to_proc <- as_date(date_to_proc)
dates_fcst <- seq(date_to_proc, date_to_proc + months(5), by = "1 month")


# generate heat index constants
heat_vars <- heat_index_var_generator()






# ERA5 SECTION ---------------------------------------------------------------- 

# Download and prepare: (1) ERA5 distribution parameters and (2) two
# months of water balance prior to the date to process to apply a three-month
# rolling sum.


vars_era <- 
  c("total-precipitation", "2m-temperature", "water-balance-th-rollsum3") |> 
  set_names(c("pr", "tas", "wb_rollsum3"))


# download ERA5 dist parameters files

ff_era <- 
  map(vars_era, \(var) {
    
    f <- 
      rt_gs_list_files(str_glue("{dir_gs_era}/climatologies")) |> 
      str_subset(str_glue("_{var}_")) |> 
      str_subset("1991-2020")
    
    # rearrange to follow order of dates_fcst
    # (instead of numerical)
    f <- 
      str_sub(dates_fcst, -5,-4) |> 
      map_chr(\(m){
        
        f |> str_subset(str_glue("_{m}"))
        
      })
    
    # download
    f <- 
      rt_gs_download_files(f, dir_tmp)
    
    return(f)
    
  })



# load ERA5 dist files

era_params <- 
  ff_era |> 
  map(\(f) {
    
    s <- 
      f |> 
      set_names(f %>% str_sub(-5,-4)) |>  
      map(read_ncdf) |>  
      suppressMessages()
    
    s <- 
      do.call(c, c(s, along = "L")) |>  
      st_set_dimensions("L", values = dates_fcst)
    
    return(s)
    
  })



# dates of the two wb months
dates_era <- seq(date_to_proc - months(2), date_to_proc - months(1), by = "1 month")


# calculate two wb months from tas and pr

era_2months_wb <- 
  map(dates_era, \(d){
    
    ss_var <- 
      vars_era[1:2] |> # only pr and tas
      map(\(v) {
        
        f <- 
          str_glue("{dir_gs_era}/monthly_means/{str_replace(v, '-', '_')}/era5_{v}_mon_{d}.nc") 
        
        system(str_glue("gcloud storage cp {f} {dir_tmp}"), 
               ignore.stdout = T, ignore.stderr = T)
        
        f <- 
          str_glue("{dir_tmp}/{fs::path_file(f)}")
        
        r <- 
          read_ncdf(f) |> 
          suppressMessages() |> 
          adrop()
        
        fs::file_delete(f)
        
        return(r)
        
      })
    
    
    s_wb <- 
      wb_calculator(d, 
                    ss_var$tas |> 
                      setNames("tas") |> 
                      mutate(tas = tas |> units::set_units(degC)), 
                    ss_var$pr |> 
                      setNames("pr"), 
                    heat_vars)
    
    return(s_wb)
    
  })





# NMME SECTION ---------------------------------------------------------------

# Downloads a month of NMME tas and precip data (with a 6-month lead). Bias-adjusts 
# it with ERA5 distr parameters. Calculates water balance. Roll-sums with two 
# previous ERA5 months. Calculates deviations (quantiles).


vars_nmme <- c("prec", "tref") |> set_names(c("pr", "tas"))



# loop through models
walk(df_sources$model |> set_names(), \(mod){
  
  # mod <- df_sources$model[1]
  
  
  s_1model <- 
    imap(vars_nmme, \(var, v){
      
      # var = "prec"
      # v = "pr"
      
      # var = "tref"
      # v = "tas"
      
      var_l <- 
        case_when(var == "prec" ~ "precipitation",
                  var == "tref" ~ "average-temperature")
      
      
      
      # download and read nmme data
      
      url <- 
        nmme_url_generator(mod,
                           date_to_proc,
                           var)
      
      f <- 
        str_glue("{dir_tmp}/nmme_{mod}_{var_l}_mon_{date_to_proc}_plus5_pre.nc")
      
      
      a <- "a" # empty vector
      class(a) <- "try-error" # assign error class
      
      while(class(a) == "try-error") {
        
        a <- 
          try(
            download.file(url, f, method = "wget", quiet = T)
          )
        
        if (class(a) == "try-error") {
          
          print(stringr::str_glue("      download failed - waiting to retry..."))
          Sys.sleep(3)
          
        }
        
      }
      
      
      fcst <- 
        nmme_formatter(f, var) |> 
        st_set_dimensions("L", values = dates_fcst)
      
      if (var == "prec") {
        
        fcst <- 
          fcst |> 
          mutate(prec = prec |> units::set_units(m/d))
        
      }
      
      
      fs::file_delete(f)
      
      
      
      # download and read model's distr params
      
      f_nmme_params <- 
        case_when(var == "prec" ~ str_glue("nmme_{mod}_{var_l}_mon_gamma-params_1991-2020_{str_sub(date_to_proc, 6,7)}_plus5.nc"),
                  var == "tref" ~ str_glue("nmme_{mod}_{var_l}_mon_norm-params_1991-2020_{str_sub(date_to_proc, 6,7)}_plus5.nc"))
      
      str_glue("gcloud storage cp {dir_gs_nmme}/{mod}/{f_nmme_params} {dir_tmp}") |> 
        system(ignore.stdout = T, ignore.stderr = T)
      
      nmme_params <- 
        str_glue("{dir_tmp}/{f_nmme_params}") |> 
        read_ncdf() |> 
        suppressMessages() |> 
        st_set_dimensions("L", values = dates_fcst)
      
      fs::file_delete(str_glue("{dir_tmp}/{f_nmme_params}"))
      
      
      
      
      # BIAS CORRECTION
      
      # loop through lead months
      
      s_1var <- 
        map(seq_along(dates_fcst), \(d_fcst_in){
          
          # d_fcst_in <- 1
          
          # era params for 1 lead month
          era_params_1mon <- 
            era_params |> 
            pluck(v) |> 
            slice(L, d_fcst_in)
          
          # nmme params for 1 lead month
          nmme_params_1mon <- 
            nmme_params |> 
            slice(L, d_fcst_in)
          
          # forecast for 1 lead month
          fcst_1mon <- 
            fcst |> 
            units::drop_units() |>  
            slice(L, d_fcst_in)
          
          
          
          # bias-adjust 1 lead month
          s_bias_adj_1mon <- 
            
            # loop through members
            map(seq(dim(fcst_1mon)[3]), \(mem){
              
              
              print(str_glue("  {mod}  |  {var}  |  lead {d_fcst_in}  |  member {mem}"))
              
              
              # calculate quantile of nmme data
              # based on nmme distributions
              
              nmme_quantile <- 
                fcst_1mon |> 
                slice(M, mem) |>  
                c(nmme_params_1mon) |> 
                merge() |> 
                st_apply(c(1,2), \(x) {
                  
                  if(any(is.na(x))) {
                    NA
                  } else {
                    
                    if (var == "prec") {
                      
                      if (x[2] == -9999) { # no precip in historical period
                        
                        NA
                        
                      } else {
                        
                        min(lmom::cdfgam(x[1], c(x[2], x[3])), 0.999)
                        
                      }
                      
                    } else if (var == "tref") {
                      
                      lmom::cdfgno(x[1], c(x[2], x[3], x[4]))
                      
                    }
                  }
                },
                .fname = "quantile",
                FUTURE = T) |> 
                st_warp(era_params_1mon)
              
              
              
              # quantile mapping with ERA5
              # distributions
              
              fcst_biasadj <- 
                nmme_quantile %>% 
                c(era_params_1mon) %>%
                merge() |> 
                st_apply(c(1,2), \(x){
                  
                  if(any(is.na(x))) {
                    NA
                  } else if (x[2] == -9999) {
                    0
                  } else {
                    
                    if (var == "prec") {
                      
                      lmom::quagam(x[1], c(x[2], x[3]))
                      
                    } else if (var == "tref") {
                      
                      lmom::quagno(x[1], c(x[2], x[3], x[4]))
                      
                    }
                  }   
                },
                .fname = {{var}},
                FUTURE = T)
              
              return(fcst_biasadj)
              
            })  
          
          
          s_bias_adj_1mon <-
            do.call(c, c(s_bias_adj_1mon, along = "M"))
          
          return(s_bias_adj_1mon)
          
          
        })
      
      s_1var <-
        do.call(c, c(s_1var, along = "L"))
      
      return(s_1var)
      
    })
  
  
  # merge pr and tas
  
  s_1model <- 
    do.call(c, unname(s_1model))
  
  write_rds(s_1model, str_glue("{dir_tmp}/{mod}_bias-corr-vars.rds"))
  
  
  
  
  
  
  # CALCULATE WB QUANTILES (ANOMALIES)
  
  s_1model_wb <-
    
    # loop through members
    map(seq(dim(s_1model)["M"]), \(mem){
      
      # mem = 1
      
      
      print(str_glue("  {mod}  |  wb quantiles  |  member {mem}"))
      
      
      
      # tas and pr data 
      s_vars <- 
        s_1model |> 
        slice(M, mem)
      
      
      # calculate wb for 6 lead months
      ss_1mem <- 
        map(seq(dim(s_vars)["L"]), \(d_fcst_in){
          
          wb_calculator(dates_fcst[d_fcst_in],
                        s_vars |>
                          select(tref) |> 
                          slice(L, d_fcst_in) |> 
                          setNames("tas") |> 
                          mutate(tas = tas |> units::set_units(K) |> units::set_units(degC)),
                        s_vars |>
                          select(prec) |> 
                          slice(L, d_fcst_in) |> 
                          setNames("pr") |> 
                          mutate(pr = pr |> units::set_units(m)),
                        heat_vars)
          
        })
      
      
      # merge ERA 2 months wb with nmme 6 months 
      s_wb_era_nmme <- 
        do.call(c, c(c(era_2months_wb, ss_1mem), along = "L"))
      
      # rollsum
      s_wb_rolled <- 
        s_wb_era_nmme |> 
        st_apply(c(1,2), \(x) {
          
          
          if (any(is.na(x))) {
            rep(NA, length(x))
          } else {
            
            # zoo::rollsum(x, k = 3, fill = NA, align = "right")
            slider::slide_dbl(x, .f = sum, .before = 2, .complete = T)
            
          }
        }, 
        .fname = "L",
        FUTURE = F) |> 
        aperm(c(2,3,1)) |> 
        slice(L, 3:8) |> # remove traling 2 first months
        st_set_dimensions("L", values = dates_fcst) |> 
        setNames("wb_rollsum3")
      
      
      # calculate quantiles with ERA5 distr parameters
      s_wb_quantile <- 
        c(s_wb_rolled, era_params$wb_rollsum3) |> 
        merge() |> 
        st_apply(c(1,2,3), \(x) {
          
          if(any(is.na(x))) {
            NA
          } else {
            
            lmom::cdfglo(x[1], c(x[2], x[3], x[4]))
            
          }
        },
        .fname = "perc",
        FUTURE = F)
      
      return(s_wb_quantile)
      
    })
  
  
  s_1model_wb <- 
    do.call(c, c(s_1model_wb, along = "M"))
  
  write_rds(s_1model_wb, str_glue("{dir_tmp}/{mod}_wb-quantiles.rds"))
  
  
})

    
wb_quantiles <- 
  str_glue("{dir_tmp}/{df_sources$model}_wb-quantiles.rds") |> 
  map(\(f){
    
    read_rds(f)
    
  })


wb_quantiles <- 
  do.call(c, c(wb_quantiles, along = "M"))


wb_quantiles_stats <- 
  wb_quantiles |> 
  st_apply(c(1,2,3), \(x) {
    
    if (any(is.na(x))) {
      c(mean = NA, sd = NA, agree = NA)
    } else {
      
      mean_perc <- mean(x)
      
      sd_perc <- sd(x)
      
      # how many members are in the same quintile as the mean
      upper_lim <- ceiling(mean_perc / 0.2) * 0.2
      agree <- mean(x < upper_lim & x > (upper_lim-0.2))
      
      c(mean = mean_perc, sd = sd_perc, agree = agree)
    }
  },
  .fname = "stats") |> 
  split("stats")
    

f_name <- str_glue("{dir_tmp}/nmme_ensemble_wb-quantile-stats_mon_{date_to_proc}_plus5.nc")

rt_write_nc(wb_quantiles_stats, f_name, daily = F, 
            gatt_name = "source code", 
            gatt_val = "https://github.com/carlosdobler/drought/drought_monitor")




