

library(tidyverse)
library(stars)
library(furrr)

options(future.fork.enable = T)
options(future.rng.onMisuse = "ignore")

plan(multicore)

dest_dir <- "/mnt/pers_disk/tmp"

source("monitor_forecast/functions.R")


vars <- c("tref", "prec")
vars_long <- c("average-temperature", "precipitation")


walk2(vars, vars_long, \(var, var_l) {
  
  for (mod in df_sources$model) {
    
    print(str_glue("variable:{var}  |  model: {mod}"))
    
    
    future_walk(seq(as_date("1991-01-01"), as_date("2020-12-01"), by = "1 month"), \(d) {
      
      url <- 
        url_generator(mod,
                      d,
                      var)
      
      f <- 
        str_glue("{dest_dir}/nmme_{mod}_{var}_mon_{as_date(d)}_plus5_pre.nc")
      
      download.file(url, f, method = "wget", quiet = T)
      
      
      s <- 
        formatter(f, var)
      
      
      f_formatted <- 
        f |> str_replace("_pre", "")
      
      write_nc(s, f_formatted)
      
      str_glue("gcloud storage mv {f_formatted} gs://clim_data_reg_useast1/nmme/monthly/{mod}/{var_l}/") |> 
        system(ignore.stdout = T, ignore.stderr = T)
      
      fs::file_delete(f)
      
    })
    
  }
  
})















# ******




"gcloud storage mv"



  

