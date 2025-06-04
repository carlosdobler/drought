
# SCRIPT TO REGRID SPAM PRODUCTION MAPS TO MATCH ERA5's GRID.

# NEEDS ACCESS TO GOOGLE CLOUD BUCKET "clim_data_reg_useast1", MOUNTED 
# AT /mnt/bucket_mine.

# ALSO NEEDS ACCESS TO GOOGLE CLOUD BUCKET "drought-monitor" (NOT MOUNTED)
# TO STORE RESULTS.



library(tidyverse)
library(stars)

# get list of all crops
crop_list <- 
  "/mnt/bucket_mine/misc_data/agriculture/spam/spam2020/Readme.txt" |> 
  read_delim(skip = 90, col_names = c("id", "name_spam", "name_full"))

# remove generic
crop_list <- 
  crop_list |> 
  filter(!name_full %in% c("Other Cereals",
                           "Other Roots",
                           "Other Pulses",
                           "Other Oil Crops",
                           "Other Fibre Crops",
                           "Other Tropical Fruit",
                           "Temperate Fruit",
                           "Other Vegetables",
                           "Rest Of Crops"))

# add all coffee
crop_list <- 
  crop_list |>
  bind_rows(tibble(id = NA, name_spam = "coffee_all", name_full = "Coffee All"))

# import reference raster (era5)
s_ref_era <- 
  "/mnt/bucket_mine/era5/annual_aggregates/average_temperature/era5_average-temperature_yr_1971-01-01.nc" |> 
  read_ncdf()


pwalk(crop_list, \(name_spam, name_full, ...){
  
  message(str_glue("PROCESSING CROP {which(name_full == crop_list$name_full)} / {nrow(crop_list)} ({name_full})"))
  
  if (name_spam == "coffee_all") {
    
    # import both coffee varieties and sum them
    s_crop <- 
      
      c("/mnt/bucket_mine/misc_data/agriculture/spam/spam2020/production/spam2020_v1r0_global_P_COFF_A.tif",
        "/mnt/bucket_mine/misc_data/agriculture/spam/spam2020/production/spam2020_v1r0_global_P_RCOF_A.tif") |> 
      read_stars()
      
    s_crop <- 
      s_crop |> 
      merge() |> 
      setNames("p") |> 
      mutate(p = if_else(is.na(p), 0, p)) |> 
      st_apply(c(1,2), sum, .fname = "p")
      
    
  } else {
    
    s_crop <- 
      
      # import crop raster
      "/mnt/bucket_mine/misc_data/agriculture/spam/spam2020/production/spam2020_v1r0_global_P_{str_to_upper(name_spam)}_A.tif" |> 
      str_glue() |> 
      read_stars() |> 
      setNames("p") |> 
      mutate(p = if_else(is.na(p), 0, p))
    
  }
  
  s_crop <- 
    s_crop |> 
    
    # resample to 1/4 degree res (sum)
    st_warp(st_as_stars(st_bbox(), dx = 0.25, values = NA),
            use_gdal = T,
            method = "sum") |> 
    setNames("p") |> 
    
    # remove pixels with no production
    mutate(p = if_else(p == 0, NA, p)) |> 
    
    # reample to era5 resolution (nearest neighbor)
    st_warp(s_ref_era)
    
  
  # save result and upload to cloud bucket
  f_res <- str_glue("crop_production_era5-grid_{name_full |> str_replace(' ', '-') |> str_to_lower()}.tif")
  
  write_stars(s_crop, f_res)
  
  str_glue("gcloud storage mv {f_res} gs://drought-monitor/spam/") |> 
    system(ignore.stdout = T, ignore.stderr = T)
  
})













