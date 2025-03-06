
library(tidyverse)
library(stars)


dest_dir <- "/mnt/pers_disk/tmp"



source("monitor_forecast/functions.R")


mod <- "canesm5"
d <- "1991-01-01"
var <- "prec"



url <- 
  url_generator(mod,
                d,
                var)

f <- 
  str_glue("{dest_dir}/nmme_{mod}_{var}_mon_{d}_plus5.nc")

download.file(url, f, method = "wget", quiet = T)

un <- 
  ncmeta::nc_atts(f) |> 
  filter(variable == var) |> 
  filter(name == "units") |> 
  pull(value) |> 
  pluck(1)

s <- 
  f |> 
  read_mdim() |> 
  suppressMessages() |> 
  adrop() |> 
  st_set_dimensions("L", values = seq(6))

if (var == "tref" & un == "Kelvin_scale") {
  
  s <- 
    s |> 
    mutate(!!sym(var) := units::set_units(!!sym(var), K)) |> 
    mutate(!!sym(var) := units::set_units(!!sym(var), degC))
  
}


# ******

# define dimensions
dim_x <- ncdf4::ncdim_def(name = names(st_dimensions(s)[1]), 
                            units = "degrees_east", 
                            vals = s |> st_get_dimension_values(1))

dim_y <- ncdf4::ncdim_def(name = names(st_dimensions(s)[2]), 
                            units = "degrees_north", 
                            vals = s |> st_get_dimension_values(2))

dim_l <- ncdf4::ncdim_def(name = names(st_dimensions(s)[3]), 
                          units = "", 
                          vals = s |> st_get_dimension_values(3))

dim_m <- ncdf4::ncdim_def(name = names(st_dimensions(s)[4]), 
                          units = "", 
                          vals = s |> st_get_dimension_values(4))






# define variables
vari <- ncdf4::ncvar_def(name = names(s),
                         units = s |> pull() |> units::deparse_unit(),
                         dim = list(dim_x, dim_y, dim_l, dim_m))

# create file
ncnew <- ncdf4::nc_create(filename = "/mnt/pers_disk/test.nc", 
                          vars = vari,
                          force_v4 = TRUE)


# global attribute
ncdf4::ncatt_put(ncnew,
                   varid = 0,
                   attname = gatt_name,
                   attval = gatt_val)



# write data
purrr::walk(seq_along(n),
            ~ncdf4::ncvar_put(nc = ncnew, 
                              varid = varis[[.x]], 
                              vals = s |> dplyr::select(all_of(.x)) |> dplyr::pull()))

ncdf4::nc_close(ncnew)


"gcloud storage mv"

fs::file_delete()

  

