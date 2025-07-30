

# SCRIPT TO CALCULATE MONTHLY DROUGHT WITH ERA5 
# BASED ON A MODIFIED SPEI METHODOLOGY 
# (USES THORNWHAITES FORMULATION TO CALCULATE PET)





library(tidyverse)
library(stars)
library(furrr)

options(future.fork.enable = T)
options(future.rng.onMisuse = "ignore")

# special functions
box::use(../functions/general_tools[...])
box::use(../functions/drought[...])

# date to process
source("monitor_forecast/date_to_proc.R")

# temporary directory
dir_tmp <- "/mnt/pers_disk/tmp"
if (fs::dir_exists(dir_tmp)) fs::dir_delete(dir_tmp) # clean slate


# root bucket dir
dir_gs <- "gs://clim_data_reg_useast1/era5"



winds <- c(3, 12)

for(k in winds) {
  
  message(str_glue("*** PROCESSING {date_to_proc} (window = {k}) ***"))
  
  fs::dir_create(dir_tmp)
  
  # check what dates have already been processed
  existing_dates <- 
    rt_gs_list_files(str_glue("gs://drought-monitor/historical")) |> 
    str_subset(".nc") |> 
    str_subset(str_glue("-w{k}_")) |> 
    str_sub(-13,-4)
  
  
  if (date_to_proc %in% existing_dates) {
    
    message("THIS DATE HAS ALREADY BEEN PROCESSED! Skipping process")
    next
    
  } else {
    
    
    # STEP 1. DOWNLOAD DATA
    
    pre_months <- 
      seq(as_date(date_to_proc)-months(k-1), as_date(date_to_proc), by = "1 month")
    
    
    # check first if wb is available in bucket
    existing_dates_wb <- 
      rt_gs_list_files(str_glue("gs://clim_data_reg_useast1/era5/monthly_means/water_balance_th/")) |> 
      str_subset(".nc$") |> 
      str_sub(-13,-4)
    
    non_existing_dates_wb <- !pre_months %in% existing_dates_wb
    
    
    if (any(non_existing_dates_wb)) { # some wb dates do not exist: calculate
    
      
      # check what precip and temperature data is not available (in bucket)
      non_existing_ind_prtas <- 
        c("total_precipitation", "2m_temperature") |>
        set_names() |> 
        map(\(var){
          
          existing_dates <- 
            rt_gs_list_files(str_glue("{dir_gs}/monthly_means/{var}")) |> 
            str_sub(-13,-4)
          
          !pre_months[non_existing_dates_wb] %in% existing_dates
          
        })
      
      # import cdsapi module if not all data is available
      # (to download it from cds)
      if(any(unname(unlist(non_existing_ind_prtas)))) {
        
        # if attrs causes issues, update it:
        # pip install --upgrade attrs
        
        reticulate::use_python(python = "/usr/bin/python3")
        cdsapi <- reticulate::import("cdsapi")
        # reticulate::py_require("cdsapi")
        # cdsapi <- reticulate::import("cdsapi")
        
      }
      
      
      
      # STEP 1. Download precip and tas from bucket/cds
      
      message("Downloading precip/tas data...")
      
      non_existing_ind_prtas |>  
        iwalk(\(i, var){   # for each variable (precip, tas)
          
          # if some dates are in bucket
          # download those from bucket
          if(length(pre_months[!i]) > 0) {
            
            pre_months[non_existing_dates_wb][!i] |> 
              walk(\(d) {
                
                message(str_glue("   {var} {d} from bucket"))
                
                f <- str_glue("era5_{str_replace_all(var, '_', '-')}_mon_{d}.nc")
                
                # use rt_gs_download
                str_glue("gcloud storage cp {dir_gs}/monthly_means/{var}/{f} {dir_tmp}") |> 
                  system(ignore.stdout = T, ignore.stderr = T)
                
              })
            
          }
          
          
          # if some dates are NOT in bucket
          # download those from cds
          if(length(pre_months[i]) > 0) {
            
            pre_months[non_existing_dates_wb][i] |> 
              walk(\(d) {
                
                message(str_glue("   {var} {d} from cds"))
                
                f <- str_glue("era5_{str_replace_all(var, '_', '-')}_mon_{d}.nc") 
                
                a <- "a" # empty vector
                class(a) <- "try-error" # assign error class
                
                while(class(a) == "try-error") {
                  
                  a <- 
                    try(
                      cdsapi$Client()$retrieve(
                        
                        name = "reanalysis-era5-single-levels-monthly-means",
                        
                        request = reticulate::dict(format = "netcdf",
                                                   product_type = "monthly_averaged_reanalysis",
                                                   variable = var,
                                                   year = year(d),
                                                   month = str_pad(month(d), 2, "left", "0"),
                                                   time = "00:00"),
                        
                        target = str_glue("{dir_tmp}/{f}"))
                    )
                  
                  if (class(a) == "try-error") {
                    
                    message("      waiting to retry...")
                    Sys.sleep(3)
                    
                  }
                }
                
                # upload to bucket
                str_glue("gcloud storage cp {dir_tmp}/{f} {dir_gs}/monthly_means/{var}/") |> 
                  system(ignore.stdout = T, ignore.stderr = T)
                
              })
            
          }
          
        })
      
      
      # STEP 2. CALCULATE WB
      
      message("Calculating water balance")
      
      # heat index constants
      heat_vars <- heat_index_var_generator()
      
      
      # TO DO: SAVE MONTHLY WB; CHECK FIRST IF EXISTS AND LOAD IF SO
      # ff_wb <- 
      #   
      
      
      # ************** block 01 *************************************************** 
      
      s_wb <- 
        c(
          
          map(pre_months[non_existing_dates_wb] |> set_names(), \(d) {    # for each month
            
            # load temperature
            s_tas <-
              read_ncdf(str_glue("{dir_tmp}/era5_2m-temperature_mon_{as_date(d)}.nc")) |>
              suppressMessages() |>
              adrop()
            
            # load precipitation
            s_pr <-
              read_ncdf(str_glue("{dir_tmp}/era5_total-precipitation_mon_{as_date(d)}.nc")) |>
              suppressMessages() |>
              adrop()
            
            # calculate wb
            s_wb <- 
              wb_calculator_th(d, 
                               s_tas |> 
                                 setNames("tas") |> 
                                 mutate(tas = tas |> units::set_units(degC)), 
                               s_pr |> 
                                 setNames("pr"), 
                               heat_vars)
            
            # save to bucket
            f_res <- str_glue("{tempdir()}/era5_water-balance-th_mon_{d}.nc")
            rt_write_nc(s_wb, f_res)
            str_glue("gcloud storage mv {f_res} {dir_gs}/monthly_means/water_balance_th/") |> 
              system(ignore.stdout = T, ignore.stderr = T)
            
            return(s_wb)
            
          }),
          
          
          
          map(pre_months[!non_existing_dates_wb] |> set_names(), \(d){
            
            f <- str_glue("era5_water-balance-th_mon_{d}.nc")
            
            rt_gs_download_files(str_glue("{dir_gs}/monthly_means/water_balance_th/{f}"), dir_tmp, quiet = T) |> 
              read_ncdf() |> 
              suppressMessages()
            
          })
          
        )
        
      s_wb <- s_wb[order(names(s_wb))]
      
    
        
    } else if (all(!non_existing_dates_wb)) { # all wb dates are in bucket
      
      s_wb <- 
        map(pre_months |> set_names(), \(d){
          
          f <- str_glue("era5_water-balance-th_mon_{d}.nc")
          
          rt_gs_download_files(str_glue("{dir_gs}/monthly_means/water_balance_th/{f}"), dir_tmp, quiet = T) |> 
            read_ncdf() |> 
            suppressMessages()
          
        })
      
    }
    
    # concatenate all months
    s_wb <- 
      do.call(c, c(s_wb, along = "time")) |>  
      st_set_dimensions("time", values = pre_months)
    
    
    # aggregate (rollsum)
    s_wb_rolled <- 
      s_wb |> 
      st_apply(c(1,2), sum, .fname = str_glue("wb_rollsum{k}"), FUTURE = T)
    
    
    # ************** end of block 01 ********************************************
    
    
    
    
    # STEP 3. CALCULATE ANOMALY (PERCENTILE)
    
    message(str_glue("Calculating anomalies"))
    
    
    # import baseline distribution parameters
    
    f_distr <- 
      str_glue("era5_water-balance-th-rollsum{k}_mon_log-params_1991-2020_{str_pad(month(as_date(date_to_proc)), 2, 'left', '0')}.nc")
    
    s_dist_params <- 
      rt_gs_download_files(str_glue("{dir_gs}/climatologies/{f_distr}"), 
                           dir_tmp, quiet = T) |> 
      read_ncdf() |> 
      suppressMessages()
    
    # calculate quantile
    
    s_perc <- 
      c(s_wb_rolled, s_dist_params) |> 
      merge() |>
      st_apply(c(1,2), \(x){
        
        if(any(is.na(x))) {
          NA
        } else {
          
          lmom::cdfglo(x[1], c(x[2], x[3], x[4]))
          
        }
        
      },
      FUTURE = T,
      .fname = "perc")
    
    
    # save result
    
    
    # ************* block 2 *****************************************************
    
    res_file <- 
      str_glue("era5_water-balance-perc-w{k}_bl-1991-2020_mon_{date_to_proc}.nc")
    
    res_path <- 
      str_glue("{dir_tmp}/{res_file}")
    
    rt_write_nc(s_perc,
                res_path,
                gatt_name = "source code",
                gatt_val = "https://github.com/carlosdobler/drought/drought_monitor")
    
    # upload to gcloud
    # str_glue("gsutil mv {res_path} {dir_gs}/water_balance_th_perc/") %>% 
    #   system(ignore.stdout = T, ignore.stderr = T)
    str_glue("gcloud storage mv {res_path} gs://drought-monitor/historical/") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    # ************* end of block 2 ***********************************************
    
    
    fs::dir_delete(dir_tmp)  
    
    
  }
  
  
}


















# s_perc |>
#   st_warp(st_as_stars(st_bbox(), dx = 0.25, values = NA)) |>
#   as_tibble() |>
#   ggplot(aes(x, y, fill = perc)) +
#   geom_raster() +
#   colorspace::scale_fill_binned_divergingx("spectral",
#                                            mid = 0.5,
#                                           na.value = "transparent",
#                                           rev = F,
#                                           limits = c(0,1),
#                                           n.breaks = 11)





