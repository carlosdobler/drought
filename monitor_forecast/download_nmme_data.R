
library(tidyverse)
library(stars)
library(furrr)

options(future.fork.enable = T)
plan(multicore, workers = 5)

dir_data <- "/mnt/pers_disk/tmp"
fs::dir_create(dir_data)



# dates <- seq(as_date("1991-01-01"), as_date("2020-12-01"), by = "1 month")
dates <- seq(as_date("2017-02-01"), as_date("2020-12-01"), by = "1 month")
model <- "nasa-geoss2s" # "cola-rsmas-ccsm4" # "cola-rsmas-cesm1" #   #"gem5p2-nemo"

# ******

"%28" = "("
"%29" = ")"
"%20" = " "

"https://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.CanSIPS-IC4/.CanESM5/.FORECAST/.MONTHLY/.tref/M/(1.0)(10.0)RANGEEDGES/L/(0.5)(5.5)RANGEEDGES/S/(0000%201%20Mar%202025)VALUES/data.nc"
url <- "https://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.CanSIPS-IC4/.CanESM5/.FORECAST/.MONTHLY/.tref/M/%281.0%29%2810.0%29RANGEEDGES/L/%280.5%29%285.5%29RANGEEDGES/S/%280000%201%20Mar%202025%29VALUES/data.nc"

# *******


future_walk(dates, \(d){
  
  # print(d)
  
  # url <- 
  #   str_glue(
  #     "http://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.GFDL-SPEAR/.HINDCAST/.MONTHLY/.prec/L/%280.5%29%285.5%29RANGEEDGES/S/%280000%201%20{format(d, '%b')}%20{year(d)}%29VALUES/M/%281.0%29%2810.0%29RANGEEDGES/data.nc"
  #   )
  
  # url <- 
  #   str_glue(
  #     "http://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.CanSIPS-IC4/.CanESM5/.HINDCAST/.MONTHLY/.prec/L/%280.5%29%285.5%29RANGEEDGES/S/%280000%201%20{format(d, '%b')}%20{year(d)}%29VALUES/M/%281.0%29%2810.0%29RANGEEDGES/data.nc"
  #   )
  
  # url <- 
  #   str_glue(
  #     "http://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.CanSIPS-IC4/.GEM5.2-NEMO/.HINDCAST/.MONTHLY/.prec/L/%280.5%29%285.5%29RANGEEDGES/S/%280000%201%20{format(d, '%b')}%20{year(d)}%29VALUES/M/%281.0%29%2810.0%29RANGEEDGES/data.nc"
  #   )
  
  # url <- 
  #   str_glue(
  #     "http://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.NASA-GEOSS2S/.HINDCAST/.MONTHLY/.prec/L/%280.5%29%285.5%29RANGEEDGES/S/%280000%201%20{format(d, '%b')}%20{year(d)}%29VALUES/data.nc"
  #   )
  
  url <- 
    str_glue(
      "http://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.NASA-GEOSS2S/.FORECAST/.MONTHLY/.prec/L/%280.5%29%285.5%29RANGEEDGES/S/%280000%201%20{format(d, '%b')}%20{year(d)}%29VALUES/data.nc"
    )
  
  # url <- 
  #   str_glue(
  #     "http://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.COLA-RSMAS-CESM1/.MONTHLY/.prec/L/%280.5%29%285.5%29RANGEEDGES/S/%280000%201%20{format(d, '%b')}%20{year(d)}%29VALUES/data.nc"
  #   )
  
  # url <-
  #   str_glue(
  #     "http://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.COLA-RSMAS-CCSM4/.MONTHLY/.prec/L/%280.5%29%285.5%29RANGEEDGES/S/%280000%201%20{format(d, '%b')}%20{year(d)}%29VALUES/data.nc"
  #   )
  
  
  f <-
    str_glue("{dir_data}/nmme_{model}_precipitation_mon_{d}_plus5.nc")
  
  
  download.file(url, f, method = "wget", quiet = T)
  
  str_glue("gsutil mv {f} gs://clim_data_reg_useast1/nmme/monthly/{model}/precipitation/{fs::path_file(f)}") %>% 
    system(ignore.stdout = T, ignore.stderr = T)
  
})




