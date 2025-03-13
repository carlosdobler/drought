
# SCRIPT TO CALCULATE MONTHLY DROUGHT WITH ERA5 
# BASED ON A MODIFIED SPEI METHODOLOGY 
# USES THORNWHAITES FORMULATION TO CALCULATE PET


date_to_proc <- "2025-02-01"






library(tidyverse)
library(stars)
library(furrr)

options(future.fork.enable = T)
options(future.rng.onMisuse = "ignore")


source("https://raw.github.com/carlosdobler/spatial-routines/master/general_tools.R")
source("monitor_forecast/functions_monitor.R")


dir_tmp <- "/mnt/pers_disk/tmp"
if (fs::dir_exists(dir_tmp)) fs::dir_delete(dir_tmp)
fs::dir_create(dir_tmp)


dir_gs <- "gs://clim_data_reg_useast1/era5"



existing_dates <- 
  rt_gs_list_files(str_glue("{dir_gs}/monthly_means/water_balance_th_perc")) |> 
  str_sub(-13,-4)
# CHANGE TO JOSHUA'S BUCKET



if (date_to_proc %in% existing_dates) {
  
  print(str_glue("THIS DATE HAS ALREADY BEEN PROCESSED!"))
  
} else {
  
  
  
  # STEP 1. DOWNLOAD DATA
  
  
  tri_month <- 
    seq(as_date(date_to_proc)-months(2), as_date(date_to_proc), by = "1 month")
  
  
  vars <- c("total_precipitation", "2m_temperature")
  
  existing_ind <- 
    vars |>
    set_names() |> 
    map(\(var){
      
      existing_dates <- 
        rt_gs_list_files(str_glue("{dir_gs}/monthly_means/{var}")) |> 
        str_sub(-13,-4)
      
      tri_month %in% existing_dates
      
    })
  
  
  if(!all(unname(unlist(existing_ind)))) {
    
    reticulate::use_python(python = "/usr/bin/python3")
    cdsapi <- reticulate::import("cdsapi")
    
  }
  
  
  
  existing_ind |> 
    iwalk(\(i, var){
      
      if(length(tri_month[i]) > 0) {
        
        tri_month[i] |> 
          walk(\(d) {
        
            print(str_glue("downloading {var} {d} from bucket..."))
            
            f <- str_glue("era5_{str_replace_all(var, '_', '-')}_mon_{d}.nc")
                
            str_glue("gcloud storage cp {dir_gs}/monthly_means/{var}/{f} {dir_tmp}") |> 
              system()
            
          })
        
      }
      
      
      if(length(tri_month[!i]) > 0) {
      
        tri_month[!i] |> 
          walk(\(d) {
            
            print(str_glue("downloading {var} {d} from cds..."))
            
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
                
                print(stringr::str_glue("  waiting to retry..."))
                Sys.sleep(3)
                
              }
            }
            
            str_glue("gcloud storage cp {dir_tmp}/{f} {dir_gs}/monthly_means/{var}/") |> 
              system()
            
          })
        
      }
      
    })
  
  
  
  # STEP 2. CALCULATE WB
  
  heat_vars <- heat_index_var_generator()
  
  s_wb <- 
    tri_month |> 
    map(wb_calculator, dir = dir_tmp, heat_vars = heat_vars)
  
  
  s_wb <- 
    do.call(c, c(s_wb, along = "time")) |>  
    st_set_dimensions("time", values = tri_month)
  
  
  # aggregate (rollsum)
  
  s_wb_rolled <- 
    s_wb |> 
    st_apply(c(1,2), sum, .fname = "wb_rollsum3", FUTURE = T)
  
  
  
  
  # STEP 3. CALCULATE ANOMALY (PERCENTILE)
  
  # import baseline parameters
  f_distr <- 
    str_glue("era5_water-balance-th-rollsum3_mon_gamma-params_1991-2000_{str_pad(month(as_date(date_to_proc)), 2, 'left', '0')}.nc")
  
  str_glue("gcloud storage cp {dir_gs}/climatologies/{f_distr} {tempdir()}") |> 
    system()
  
  s_dist_params <- 
    str_glue("{tempdir()}/{f_distr}") |> 
    read_ncdf() |> 
    suppressMessages()
  
  fs::file_delete(str_glue("{tempdir()}/{f_distr}"))
  
  
  s_perc <- 
    c(s_wb_rolled, s_dist_params) |> 
    merge() |>
    st_apply(c(1,2), \(x){
      
      if(any(is.na(x))) {
        NA
      } else {
        
        lmom::cdfgam(x[1] + x[2], c(x[3], x[4]))
        
      }
      
    },
    FUTURE = T,
    .fname = "perc")
  
  
  
  res_file <- 
    str_glue("era5_water-balance-perc-w3_bl-1991-2020_mon_{date_to_proc}.nc")
  
  res_path <- 
    str_glue("{dir_data}/{res_file}")
  
  rt_write_nc(s_perc,
              res_path,
              gatt_name = "source code",
              gatt_val = "https://github.com/carlosdobler/drought")
  
  # upload to gcloud
  # str_glue("gsutil mv {res_path} {dir_gs}/water_balance_th_perc/") %>% 
  #   system(ignore.stdout = T, ignore.stderr = T)
  str_glue("gsutil mv {res_path} gs://drought-monitor/input_data/raster_monthly/") %>%
    system(ignore.stdout = T, ignore.stderr = T)
  
  
  
  fs::dir_delete(dir_tmp)  
  
  
}




s_perc |> 
  st_warp(st_as_stars(st_bbox(), dx = 0.25, values = NA)) |> 
  as_tibble() |>
  ggplot(aes(x, y, fill = perc)) +
  geom_raster() +
  colorspace::scale_fill_binned_divergingx("spectral",
                                           mid = 0.5,
                                          na.value = "transparent",
                                          rev = F,
                                          limits = c(0,1),
                                          n.breaks = 11)
    
  


