
library(tidyverse)
library(stars)
library(furrr)
box::use(../functions/general_tools[...])

options(future.fork.enable = T)
options(future.rng.onMisuse = "ignore")
plan(multicore)

dir_tmp <- "/mnt/pers_disk/dir_tmp"
fs::dir_create(dir_tmp)


# Download

ff <- 
  rt_gs_list_files("gs://clim_data_reg_useast1/era5/monthly_means/toa_incident_solar_radiation")

ff <- 
  ff |> 
  str_subset(seq(1970,2000) %>% str_flatten("|")) |> 
  rt_gs_download_files(dir_tmp)

# Calculate mean per calendar month

tisr_cal_mon <- 
  str_pad(seq(12), 2, "left", "0") |> 
  map(\(mon){
    
    message(str_glue("Processing month {mon}"))
    
    s <- 
      ff |> 
      str_subset(str_glue("-{mon}-")) |> 
      future_map(read_ncdf, proxy = F) |> 
      suppressMessages()
    
    s <- 
      do.call(c, c(s, along = "time"))
    
    s <- 
      s |> 
      mutate(tisr = tisr |> units::set_units(MJ/m^2)) |> 
      st_apply(c(1,2), mean, .fname = "tisr", FUTURE = T)
    
    return(s)
    
  })


do.call(c, c(tisr_calmon, along = "cal_month")) |> 
  rt_write_nc("drought_w_nex/tisr_cal_month.nc")

fs::dir_delete(dir_tmp)

