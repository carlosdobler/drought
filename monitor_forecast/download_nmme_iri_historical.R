

library(tidyverse)
library(stars)
library(furrr)

options(future.fork.enable = T)
options(future.rng.onMisuse = "ignore")

plan(multicore)

dir_data <- "/mnt/pers_disk/tmp"

source("monitor_forecast/df_sources_nmme.R")

vars <- c("tref", "prec")
vars_long <- c("average-temperature", "precipitation")



# FUNCTIONS -------------------------------------------------------------------

# function to generate an url to download forecast data from IRI

url_generator <- function(model, date, variable, lead = 5) {
  
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




# function to format IRI's ncdfs into a simpler form:
# four dimensions only (lat, lon, member, lead) and with
# existing units

formatter <- function(f, variable, lead = 5) {
  
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



# function to write formatted stars obj to ncdf

write_nc <- function(stars_obj, f) {
  
  
  # define dimensions
  dim_x <- 
    ncdf4::ncdim_def(name = names(st_dimensions(stars_obj)[1]), 
                     units = "degrees_east", 
                     vals = stars_obj |> st_get_dimension_values(1))
  
  dim_y <- 
    ncdf4::ncdim_def(name = names(st_dimensions(stars_obj)[2]), 
                     units = "degrees_north", 
                     vals = stars_obj |> st_get_dimension_values(2))
  
  dim_l <- 
    ncdf4::ncdim_def(name = names(st_dimensions(stars_obj)[3]), 
                     units = "", 
                     vals = stars_obj |> st_get_dimension_values(3)) # lead dim
  
  dim_m <- 
    ncdf4::ncdim_def(name = names(st_dimensions(stars_obj)[4]), 
                     units = "", 
                     vals = stars_obj |> st_get_dimension_values(4)) # member dim
  
  
  # define variables
  vari <- 
    ncdf4::ncvar_def(name = names(stars_obj),
                     units = stars_obj |> pull() |> units::deparse_unit(),
                     dim = list(dim_x, dim_y, dim_l, dim_m))
  
  # create file
  ncnew <- 
    ncdf4::nc_create(filename = f, 
                     vars = vari,
                     force_v4 = TRUE)
  
  # global attribute
  ncdf4::ncatt_put(ncnew,
                   varid = 0,
                   attname = "source_code",
                   attval = "https://github.com/carlosdobler/drought/tree/main/monitor_forecast")
  
  
  # write data
  ncdf4::ncvar_put(nc = ncnew, 
                   varid = vari, 
                   vals = stars_obj |> pull())
  
  ncdf4::nc_close(ncnew)
  
}



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

