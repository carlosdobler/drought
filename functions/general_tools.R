

#' List files in a google bucket directory
#' 
#' @description
#' Function to list all files in a google cloud bucket 
#' directory
#'   
#' @param dir google cloud bucket directory
#' 
#' @export
rt_gs_list_files <- function(dir){

  box::use(stringr[...])
  
  str_glue("gcloud storage ls {dir}") |> 
    system(intern = T)
  
}





#' Download file(s) from a google bucket directory
#' 
#' @description
#' Function to download one or multiple files from a google cloud bucket 
#' to a local directory. When assigned to an object, that object will contain 
#' the updated file name(s) (with path reflecting the destination directory). 
#' The function downloads files in parallel by default with future. A multicore 
#' or multisession plan needs to be called beforehand; otherwise downloads will 
#' be sequential.
#' 
#' @param f file(s) in the bucket to download (with full path; e.g. gs://...)
#' @param dest local destination directory
#' 
#' @export
rt_gs_download_files <- function(f, dest, quiet = F){
  
  box::use(stringr[...],
           furrr[...],
           future[...])
  
  # create directory "dest" if inexistent
  if (!fs::dir_exists(dest)) {
    fs::dir_create(dest)
  }
  
  
  if (any(str_sub(f, end = 2) != "gs")) {
    
    print(str_glue("ERROR: not a google cloud directory"))
    
  } else {
    
    # download files
    if (!quiet) {
      
      if (methods::is(plan(), "sequential")) {
        message(str_glue("   downloading sequentially..."))
      } else {
        message(str_glue("   downloading in parallel..."))
      }
      
    }
    
    
    f |> 
      future_walk(\(f_) {
        str_glue("gcloud storage cp {f_} {dest}") |> 
          system(ignore.stdout = T, ignore.stderr = T)
      })
    
    
    # update names
    updated <- 
      str_glue("{dest}/{fs::path_file(f)}")
    
    return(updated)
    
  }
    
}





#' Save stars objects into NetCDFs
#' 
#' @description
#' Function to write stars objects as NetCDFs. It can save 
#' objects with multiple variables. If variables have units, 
#' they will be saved.
#' 
#' @param stars_obj a stars object with either two or more 
#' dimensions; lon (x) and lat (y) should be the first two, in that order.
#' @param filename where should the file be saved?
#' @param calendar one of "360_day", "gregorian", or "noleap". If not
#' provided, it will guess from the stars obj (needs February months to work)
#' @param gatt_name global attribute name
#' @param gatt_val global attribute value (or text)
#' 
#' @export
rt_write_nc <- function(stars_obj, filename, calendar = NA, gatt_name = NA, gatt_val = NA) {
  
  box::use(r/core[...],
           stars[...],
           purrr[...],
           dplyr[...],
           PCICt[...],
           ncdf4[...])
  
  
  # DEFINE DIMENSIONS *************
  
  dims <- vector("list", length(dim(stars_obj)))
  names(dims) <- names(dim(stars_obj))
  
  # lat and lon
  
  dims[[1]] <- 
    ncdim_def(name = names(dims)[1], 
                     units = "degrees_east", 
                     vals = stars_obj |> st_get_dimension_values(1))
  
  dims[[2]] <- 
    ncdim_def(name = names(dims)[2], 
                     units = "degrees_north", 
                     vals = stars_obj |> st_get_dimension_values(2))
  
  
  # rest of dimensions
  
  for (dim_i in seq_along(dims) |> tail(-2)) {
    
    dim_vals <- 
      stars_obj |> 
      st_get_dimension_values(dim_i)
    
    
    # dimension is time
    if (is(dim_vals, "Date") | 
        is(dim_vals, "POSIXct") | 
        is(dim_vals, "PCICt")) {
      
      
      time_vector_str <- 
        dim_vals |> 
        as.character() |> 
        stringr::str_sub(end = 10)
      
      
      # time dim is daily
      if (all(diff(dim_vals) == 1)) {
        
        
        # calendar is specified
        if(!is.na(calendar)) {
          
          cal_spec <- calendar
          
        # calendar is not specified:
        # guess
        } else {
          
          # Obtain calendar type
          feb <- 
            dim_vals_str[stringr::str_sub(time_vector_str, 6,7) == "02"] # filter feb months
          
          if (length(feb) > 1) {
            
            max_feb <-
              feb |> 
              stringr::str_sub(9,10) |> # extract days
              as.numeric() |>
              max()
            
            cal_spec <-
              case_when(max_feb == 30 ~ "360_day",
                        max_feb == 29 ~ "gregorian",
                        max_feb == 28 ~ "noleap")
            
          } else {
            
            message("ERROR: no multiple Februaries: cannot guess calendar")
            
          }
          
        }
        
        time_vector <- 
          as.PCICt(time_vector_str, cal = cal_spec)
        
        
      # time dim is not daily
      } else {
        
        time_vector <- 
          as.PCICt(time_vector_str, cal = "gregorian")
        
      }
      
      
      cal <- 
        case_when(attributes(time_vector)$cal == "360" ~ "360_day",
                  attributes(time_vector)$cal == "365" ~ "365_day",
                  attributes(time_vector)$cal == "proleptic_gregorian" ~ "gregorian")
      
      dims[[dim_i]] <-
        ncdim_def(name = names(dims)[dim_i],
                         units = "days since 1970-01-01", 
                         vals = as.numeric(time_vector)/86400,
                         calendar = cal)

    # dimension is not time  
    } else {
      
      dims[[dim_i]] <- 
        ncdim_def(name = names(dims)[dim_i], 
                         units = "", 
                         vals = dim_vals)
      
    }
    
    
  }
    
  # DEFINE VARIABLES *************
  
  var_names <- names(stars_obj)
  
  var_units <- 
    map_chr(seq_along(var_names), \(x) {
      
      un <- try(stars_obj |> 
                  select(all_of(x)) |> 
                  pull() |> 
                  units::deparse_unit(),
                silent = T)
      
      if (class(un) == "try-error") un <- ""
      
      return(un)
      
    })
  
  varis <- 
    map2(var_names, var_units, 
         ~ncvar_def(name = .x,
                           units = .y,
                           dim = dims))
  
  
  
  # CREATE FILE ***********
  
  ncnew <- 
    nc_create(filename = filename, 
                     vars = varis,
                     force_v4 = TRUE)
  
  
  
  # GLOBAL ATTRIBUTES ************
  
  if (!is.na(gatt_name)) {
    
    ncatt_put(ncnew,
                     varid = 0,
                     attname = gatt_name,
                     attval = gatt_val)
  }
  
  
  
  # WRITE DATA ****************
  
  walk(seq_along(var_names),
       ~ncvar_put(nc = ncnew, 
                         varid = varis[[.x]], 
                         vals = stars_obj |> select(all_of(.x)) |> pull()))
  
  nc_close(ncnew)
  
}





#' Get cell position (index) from coordinates
#' 
#' @description
#'  Function to obtain the position of cells in a grid
#'  given a set of coordinates. If four coordinates
#'  are provided, then the functions outputs start positions,
#'  end positions, and counts.
#' 
#' @export
rt_from_coord_to_ind <- function(stars_obj, xmin, ymin, xmax = NA, ymax = NA) {
  
  box::use(stars[...],
           purrr[...])
  
  if (is.na(xmax) & is.na(ymax)) {
    
    coords <- 
      map2(c(xmin, ymin), c(1,2), \(coord, dim_id){
        
        s <- 
          stars_obj |>  
          st_get_dimension_values(dim_id)
        
        which.min(abs(s - coord))
        
      }) |> 
      set_names(c("x", "y"))
    
    r <- 
      list(x = coords$x,
           y = coords$y)
    
  } else {
    
    coords <- 
      map2(list(c(xmin, xmax), c(ymin, ymax)), c(1,2), \(coords, dim_id){
        
        s <- 
          stars_obj |>  
          st_get_dimension_values(dim_id)
        
        map(coords, \(x) which.min(abs(s - x)))
        
        
      }) |>
      unlist(recursive = F) |> 
      set_names(c("xmin", "xmax", "ymin", "ymax"))
    
    
    if(coords$ymax > coords$ymin) {
      
      r <- 
        list(x_start = coords$xmin,
             y_start = coords$ymin,
             x_end = coords$xmax,
             y_end = coords$ymax,
             x_count = coords$xmax - coords$xmin + 1,
             y_count = coords$ymax - coords$ymin + 1)  
      
    } else {
      
      r <- 
        list(x_start = coords$xmin,
             y_start = coords$ymax,
             x_end = coords$xmax,
             y_end = coords$ymin,
             x_count = coords$xmax - coords$xmin + 1,
             y_count = coords$ymin - coords$ymax + 1)
      
    }
    
  }
  
  return(r)
}
