


df_sources <- 
  
  bind_rows(
    
    tibble(model = "canesm5",          end_hindcast = as_date("2000-12-01"), model_cast_url = "CanSIPS-IC4/.CanESM5/.{cast}"),
    tibble(model = "gem5p2-nemo",      end_hindcast = as_date("2000-12-01"), model_cast_url = "CanSIPS-IC4/.GEM5.2-NEMO/.{cast}"),
    tibble(model = "gfdl-spear",       end_hindcast = as_date("2000-12-01"), model_cast_url = "GFDL-SPEAR/.{cast}"),
    tibble(model = "cola-rsmas-cesm1", end_hindcast = NA,                    model_cast_url = "COLA-RSMAS-CESM1"),
    tibble(model = "cola-rsmas-ccsm4", end_hindcast = NA,                    model_cast_url = "COLA-RSMAS-CCSM4"),
    tibble(model = "nasa-geoss2s",     end_hindcast = as_date("2017-01-01"), model_cast_url = "NASA-GEOSS2S/.{cast}"),
    tibble(model = "ncep-cfsv2",       end_hindcast = as_date("2010-12-01"), model_cast_url = "NCEP-CFSv2/.{cast}")
    
  )




url_generator <- function(model, date, variable, lead = 5) {
  
  d <- 
    as_date(date)
  
  src <- 
    df_sources |>
    filter(model == {{model}})
  
  cast <- 
    if (!is.na(src$end_hindcast)) {
      if (date <= src$end_hindcast) {
        "HINDCAST"
      } else {
        if (model == "ncep-cfsv2") {
          "FORECAST/.PENTAD_SAMPLES"
        } else {
          "FORECAST"
        }
      }
    }
  
  
  model_cast <- 
    src$model_cast_url |> 
    str_glue()
  
  str_glue("https://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.{model_cast}/.MONTHLY/.{variable}/L/%280.5%29%28{lead}.5%29RANGEEDGES/S/%280000%201%20{format(d,'%b')}%20{year(d)}%29VALUES/M/%281.0%29%2810.0%29RANGEEDGES/data.nc")
  
}










# variable_long <- 
#   if (variable == "prec") {
#     "precipitation"
#   } else if (variable == "tref") {
#     "temperature"
#   }


# f <- "{dest_dir}/nmme_{model}_{variable_long}_mon_{date}_plus{lead}.nc"
# 
# download.file(str_glue(url), str_glue(f), method = "wget", quiet = T)




fn_formatter <- function(f) {
  
  if (str_detect(f, "temperature")) {
    
  }
  
}




url_generator(df_sources$model[7],
              as_date("2024-12-01"),
              variable = "tref")










url = "https://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.{model_cast}/.MONTHLY/.{var}/L/%280.5%29%28{lead}.5%29RANGEEDGES/S/%280000%201%20{format(d,'%b')}%20{year(d)}%29VALUES/M/%281.0%29%2810.0%29RANGEEDGES/data.nc"

# model <- "CanSIPS-IC4/.GEM5.2-NEMO"
model_cast <- "COLA-RSMAS-CCSM4"

d <- as_date("2024-09-01")
cast <- "FORECAST"
lead <- 5
var <- "prec"

f <- "/mnt/pers_disk/test.nc"



ncdf4::nc_open(f)              
stars::read_mdim(f)


d <- as_date("2010-05-01")
cast <- "HINDCAST"
lead <- 5
var <- "tref"

download.file(str_glue(url), f, method = "wget", quiet = T)

ncdf4::nc_open(f)              
stars::read_mdim(f)









"https://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.CanSIPS-IC4/.CanESM5/.FORECAST/.MONTHLY/.prec/L/%280.5%29%285.5%29RANGEEDGES/S/%280000%201%20Mar%202025%29VALUES/M/%281.0%29%2810.0%29RANGEEDGES/data.nc" %>% 
  str_replace_all("%28", "(") %>% 
  str_replace_all("%29", "(") %>% 
  str_replace_all("%20", " ")

"%28" = "("
"%29" = ")"
"%20" = " "