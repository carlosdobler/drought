

library(tidyverse)
library(stars)
library(furrr)

options(future.fork.enable = T)
options(future.rng.onMisuse = "ignore")

plan(multicore)

dir_data <- "/mnt/pers_disk/tmp"

source("monitor_forecast/df_sources_nmme.R")
source("monitor_forecast/functions_forecast.R")

vars <- c("tref", "prec")
vars_long <- c("average-temperature", "precipitation")



# PROCESS ---------------------------------------------------------------------




walk2(vars, vars_long, \(var, var_l) {
  
  for (mod in df_sources$model) {
    
    print(str_glue("variable:{var}  |  model: {mod}"))
    
    
    future_walk(seq(as_date("1991-01-01"), as_date("2020-12-01"), by = "1 month"), \(d) {
      
      url <- 
        url_generator(mod,
                      d,
                      var)
      
      f <- 
        str_glue("{dir_data}/nmme_{mod}_{var_l}_mon_{as_date(d)}_plus5_pre.nc")
      
      download.file(url, f, method = "wget", quiet = T)
      
      
      s <- 
        formatter(f, var)
      
      f_formatted <- 
        f |> str_replace("_pre.nc$", ".nc")
      
      write_nc(s, f_formatted)
      
      str_glue("gcloud storage mv {f_formatted} gs://clim_data_reg_useast1/nmme/monthly/{mod}/{str_replace(var_l, '-', '_')}/") |> 
        system(ignore.stdout = T, ignore.stderr = T)
      
      fs::file_delete(f)
      
    })  
    
  }
  
})


fs::dir_delete(dir_data)

