
#'@export
rt_tile_table <- function(s, tile_size, land = NULL) {
  
  box::use(purrr[...],
           stars[...],
           dplyr[...],
           stringr[...],
           sf[...])
  
  
  # Function to split a stars object (can be a proxy)
  # into tiles of a given size. Output is a table specifying 
  # where each tile starts and ends in terms of cell positions. 
  
  # If "land" is provided, an additional column is added indicating 
  # whether a tile covers a portion of land. In this case, the output 
  # is an sf object (tiles as polygons) with an associated table.
  
  # ARGUMENTS:
  # * s = stars object or proxy
  # * tile_size = number of cells per side of tile
  # * land = stars object in which land is represented with any 
  #   value and oceans as NA. This object *must* have the exact 
  #   same spatial dimensions as s.
  
  
  # dimensions' indices
  dims <- 
    c(1,2) |> 
    set_names(c("x", "y"))
  
  
  # create table of positions, one for each dimension
  df <- 
    imap(dims, function(dim_id, dim_name){
      
      d <- 
        dim(s)[dim_id] |> 
        seq_len()
      
      n <- 
        round(dim(s)[dim_id]/tile_size)
      
      split(d, ceiling(d/(length(d)/n))) |> 
        
        map_dfr(~tibble(start = first(.x),
                        end = last(.x)) |> 
                  mutate(count = end - start + 1)) |> 
        
        rename(!!str_glue("start_{dim_name}") := 1,
               !!str_glue("end_{dim_name}") := 2,
               !!str_glue("count_{dim_name}") := 3)
      
    })
  
  # combine both tables
  df <- 
    tidyr::expand_grid(df$x, df$y) 
  
  # add index column with appropriate padding
  length_id <- 
    df |> 
    nrow() |> 
    str_length()
  
  df <- 
    df |>
    mutate(tile_id = row_number() |> str_pad(length_id, "left", "0"), .before = 1)
  
  
  # if land is provided:
  if (!is.null(land)) {
    
    df <-
      # loop through tiles (rows of table)
      mutate(df, land = pmap_dfr(df, function(start_x, end_x, start_y, end_y, ...){
        
        # crop the land raster to the tile
        land_tile <- 
          land[,
               start_x:end_x,
               start_y:end_y]
        
        # turn to polygon
        pol_tile <-
          land_tile %>%
          st_bbox() %>%
          st_as_sfc() %>%
          st_sf()
        
        # evaluate if there is land in the cropped land tile
        pol_tile %>%
          mutate(land = if_else(all(is.na(pull(land_tile))), F, T))
        
      })) |> 
      tidyr::unnest(cols = c(land)) |> 
      st_as_sf()
    
  }
  
  return(df)
  
}


# *****

#'@export
rt_tile_load <- function(start_x, start_y, count_x, count_y, list_files, parallel = NULL) {
  
  box::use(stars[...],
           future[...],
           furrr[...],
           stars[...])
  
  
  # Helper function to load a tile of a list of files
  
  # NEEDS A WAY TO SPECIFY TIME STEPS TO IMPORT!
  # OR TO ACCOUNT FOR 1 TIME STEP
  
  
  if (!is.null(parallel)) {
    if (parallel) {
      oplan <- plan(multicore)
    } else {
      oplan <- plan(sequential)
    }
    
    on.exit(plan(oplan))
  }
  
  s_tile <-
    list_files |>
    future_map(\(f){
      
      read_ncdf(f,
                ignore_bounds = T,
                ncsub = cbind(start = c(start_x,
                                        start_y,
                                        1),
                              
                              count = c(count_x,
                                        count_y,
                                        NA)))
      
    }) |>
    suppressMessages() |>
    do.call(c, args = _)
  
  # if (grid_360) {
  # 
  #   lon <- st_get_dimension_values(s_tile, 1, center = F)
  # 
  #   if (any(lon < 0)) {
  # 
  #     lon[lon < 0] <- lon[lon < 0] + 360
  #     s_tile <-
  #       s_tile |>
  #       st_set_dimensions(1, values = lon)
  # 
  #   }
  # }
  
  return(s_tile)
  
}



# *****

#'@export
rt_tile_loop <- function(df_tiles, list_files, FUN, dir_tiles) {
  
  # Function that first loads data from list_files, then runs a 
  # function (FUN) on each tile in df_tiles, and then saves the 
  # output (a processed tile) in dir_tiles.
  
  
  
  
  # if dir_tiles does not exist, create it
  if (!fs::dir_exists(dir_tiles)) {
    fs::dir_create(dir_tiles)
  }
  
  # should rt_tile_h_load run in parallel?
  # depends on current plan
  if (is(plan(), "sequential")) {
    parallel_load <- T
  } else {
    parallel_load <- F
  }
  
  # loop through tiles
  future_pwalk(df_tiles, function(tile_id, start_x, start_y, count_x, count_y, ...){
    
    # load all data within the tile
    s_tile <-
      rt_tile_h_load(start_x, start_y, count_x, count_y, list_files, parallel_load)
    
    # apply function
    s_tile <-
      FUN(s_tile)
    
    # save tile
    s_tile |>
      write_rds(str_glue("{dir_tiles}/tile_{tile_id}.rds"))
    
  })
  
}


# *****

#'@export
rt_tile_mosaic <- function(df_tiles, dir_tiles, spatial_dims, time_dim = NULL) {
  
  box::use(stars[...],
           sf[...],
           dplyr[...],
           purrr[...],
           future[...],
           furrr[...],
           stringr[...])
  
  s <- 
    st_as_stars(dimensions = spatial_dims)
  
  df_tiles <- 
    df_tiles |> 
    st_drop_geometry()
  
  
  if (!is.null(time_dim)) {
    
    time_dim_full <-
      dir_tiles |>
      fs::dir_ls() |>
      first() |>
      read_ncdf(proxy = T) |>
      suppressMessages() |> 
      st_get_dimension_values("time")
    
    time_lims <-
      c(which(time_dim_full == first(time_dim)),
        which(time_dim_full == last(time_dim)))
    
  }
  
  
  
  
  # if tiling accounts for land coverage
  if (df_tiles |> names() |> str_detect("land") |> any()) {
    
    rows <-
      # loop through rows
      map(unique(df_tiles$start_y), function(row_i) {
        
        one_row <- 
          df_tiles |>  
          filter(start_y == row_i) |>
          future_pmap(function(tile_id, start_x, end_x, start_y, end_y, land, ...) {
            
            
            # if tile has no land
            # create an empty stars object of the size of the tile
            if (land == FALSE) {
              
              
              ss <- s[, start_x:end_x, start_y:end_y]
              
              if (is.null(time_dim)) {
                
                empty_array <- 
                  array(NA, dim = c(dim(ss)[1], 
                                    dim(ss)[2]))
                
                
              } else {
                
                empty_array <- 
                  array(NA, dim = c(dim(ss)[1], 
                                    dim(ss)[2],
                                    time = length(time_dim)))
                
              }
              
              empty_stars <- 
                st_as_stars(empty_array)
              
              st_dimensions(empty_stars)[1] <- st_dimensions(ss)[1]
              st_dimensions(empty_stars)[2] <- st_dimensions(ss)[2]
              
              
              if (!is.null(time_dim)) {
                
                empty_stars <- 
                  st_set_dimensions(empty_stars, 3, values = time_dim)
                
              }
              
              return(empty_stars)
              
              
              
              # tile has land:
              # load from saved file
            } else {
              
              
              f_tile <-
                dir_tiles |>  
                fs::dir_ls(regexp = str_glue("{tile_id}.nc"))
              
              if (is.null(time_dim)) {
                
                f_tile |> 
                  read_ncdf() |>  
                  suppressMessages()
                
              } else {
                
                f_tile |> 
                  read_ncdf(ncsub = cbind(start = c(1, 1, time_lims[1]),
                                          count = c(NA,NA, time_lims[2] - time_lims[1] + 1))) |>  
                  suppressMessages()
                
              }
              
            }
            
          })
        
        # concatenate tiles from one row
        one_row <- 
          do.call(c, c(one_row, along = 1))
        
        # fix dimension
        st_dimensions(one_row)[1] <- st_dimensions(s)[1]
        
        return(one_row)
        
      })
    
    
    
    # tiling does not account for land coverage:
    # load all tiles from saved files
  } else {
    
    
    rows <-
      map(unique(df_tiles$start_y), function(row_i) {
        
        one_row <- 
          df_tiles |>  
          filter(start_y == row_i) |>  
          future_pmap(function(tile_id, ...) {
            
            f_tile <-
              dir_tiles |>  
              fs::dir_ls(regexp = str_glue("{tile_id}.nc"))
            
            if (is.null(time_dim)) {
              
              f_tile |> 
                read_ncdf() |>  
                suppressMessages()
              
            } else {
              
              f_tile |> 
                read_ncdf(ncsub = cbind(start = c(1, 1, time_lims[1]),
                                        count = c(NA,NA, time_lims[2] - time_lims[1] + 1))) |>  
                suppressMessages()
              
            }
            
            
          })
        
        st_dimensions(one_row)[1] <- st_dimensions(s)[1]
        
        return(one_row)
        
      })
    
  }
  
  
  
  # concatenate all rows
  mos <- 
    do.call(c, c(rows, along = 2))
  
  # fix dimension
  st_dimensions(mos)[2] <- st_dimensions(s)[2]
  
  return(mos)
  
}




# *****

#'@export
rt_tile_mosaic_gdal <- function(tile_files, dir_res, spatial_dims = NULL, time_dim = NULL, time_full = NULL) {
  
  box::use(stars[...],
           sf[...],
           dplyr[...],
           purrr[...],
           future[...],
           furrr[...],
           stringr[...])
  
  if (!is.null(time_dim)) {
    
    # identify positions of time range in relation to full time vector
    time_pos <-
      c(which(time_full == first(time_dim)),
        which(time_full == last(time_dim)))
    
    # subset tile files
    # update paths
    
    dir_tiles_sub <- str_glue("{dir_res}/tiles")
    fs::dir_create(dir_tiles_sub)
    
    tile_files <- 
      future_map(tile_files, \(f){
        
        new_f <- 
          str_glue("{dir_tiles_sub}/{f |> fs::path_file() |> fs::path_ext_remove()}.tif")
        
        read_ncdf(f,
                  ncsub = cbind(start = c(1, 1, time_pos[1]),
                                count = c(NA,NA,time_pos[2]-time_pos[1]+1))) |> 
          suppressMessages() |> 
          write_stars(new_f)
        
        return(new_f)
        
      }) |> 
      unlist()
    
  }
  
  
  # mosaic with gdalwarp
  dir_mos <- str_glue("{dir_res}/mos")
  fs::dir_create(dir_mos)
  
  f_res <- 
    str_glue("{dir_mos}/mos.tif")
  
  gdal_utils(
    "warp",
    source = tile_files,
    destination = f_res,
    options = c("-dstnodata", "-9999")
  )
  
  s <- 
    f_res |> 
    read_stars()
  
  if (length(dim(s)) > 2 | dim(s)[3] > 2) {
    
    s <- 
      s |> 
      st_set_dimensions(3, 
                        name = "time",
                        values = time_dim)
    
  }
  
  if (!is.null(spatial_dims)) {
    
    s_ref <- 
      st_as_stars(dimensions = spatial_dims)
    
    s <- 
      st_warp(s, s_ref)
    
  }
  
  
  
  
  
  
  fs::dir_delete(dir_mos)
  if (fs::dir_exists(dir_tiles_sub)) fs::dir_delete(dir_tiles_sub)
  
  return(s)
  
}


