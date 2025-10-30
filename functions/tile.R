#' Split a stars raster into tiles
#'
#' Function to split a stars object (can be a proxy) into tiles of a given size.
#' Output is a table specifying where each tile starts and ends in terms of cell positions.
#' If "land" is provided, an additional column is added indicating whether a tile covers
#' a portion of land. In this case, the output is an sf object (tiles as polygons) with
#' an associated table.
#'
#' @param s stars object or proxy
#' @param tile_size number of cells per side of tile
#' @param land stars object in which land is represented with any value and oceans as NA.
#'   This object * must * have the exact same spatial dimensions as s.
#'
#' @export
rt_tile_table <- function(s, tile_size, land = NULL) {
  # dimensions' indices
  dims <-
    c(1, 2) |>
    purrr::set_names(c("x", "y"))

  # create table of positions, one for each dimension
  df <-
    purrr::imap(dims, function(dim_id, dim_name) {
      d <-
        dim(s)[dim_id] |>
        seq_len()

      n <-
        round(dim(s)[dim_id] / tile_size)

      split(d, ceiling(d / (length(d) / n))) |>

        purrr::map_dfr(
          ~ dplyr::tibble(start = dplyr::first(.x), end = dplyr::last(.x)) |>
            dplyr::mutate(count = end - start + 1)
        ) |>

        dplyr::rename(
          !!stringr::str_glue("start_{dim_name}") := 1,
          !!stringr::str_glue("end_{dim_name}") := 2,
          !!stringr::str_glue("count_{dim_name}") := 3
        )
    })

  # combine both tables
  df <-
    tidyr::expand_grid(df$x, df$y)

  # add index column with appropriate padding
  length_id <-
    df |>
    nrow() |>
    stringr::str_length()

  df <-
    df |>
    dplyr::mutate(
      tile_id = dplyr::row_number() |> stringr::str_pad(length_id, "left", "0"),
      .before = 1
    )

  # if land is provided:
  if (!is.null(land)) {
    df <-
      # loop through tiles (rows of table)
      dplyr::mutate(
        df,
        land = purrr::pmap_dfr(df, function(start_x, end_x, start_y, end_y, ...) {
          # crop the land raster to the tile
          land_tile <-
            land[,
              start_x:end_x,
              start_y:end_y
            ]

          # turn to polygon
          pol_tile <-
            land_tile %>%
            sf::st_bbox() %>%
            sf::st_as_sfc() %>%
            sf::st_sf()

          # evaluate if there is land in the cropped land tile
          pol_tile %>%
            dplyr::mutate(land = dplyr::if_else(all(is.na(dplyr::pull(land_tile))), F, T))
        })
      ) |>
      tidyr::unnest(cols = c(land)) |>
      sf::st_as_sf()
  }

  return(df)
}


# *****

#' Load a tile from a list of files
#'
#' Helper function to load a tile of a list of files.
#'
#' @param start_x Starting x position
#' @param start_y Starting y position
#' @param count_x Number of cells in x dimension
#' @param count_y Number of cells in y dimension
#' @param list_files List of files to load from
#' @param parallel Parallelization method. NULL for sequential, "mirai" or "future"
#'
#' @export
rt_tile_load <- function(start_x, start_y, count_x, count_y, list_files, parallel = NULL) {
  # NEEDS A WAY TO SPECIFY TIME STEPS TO IMPORT!
  # OR TO ACCOUNT FOR 1 TIME STEP

  if (is.null(parallel)) {
    s_tile <-
      list_files |>
      purrr::map(
        \(f) {
          stars::read_ncdf(
            f,
            ignore_bounds = T,
            ncsub = cbind(
              start = c(start_x, start_y, 1),
              count = c(count_x, count_y, NA)
            )
          ) |>
            suppressMessages()
        }
      )
  } else if (parallel == "mirai") {
    s_tile <-
      list_files |>
      purrr::map(purrr::in_parallel(
        \(f) {
          stars::read_ncdf(
            f,
            ignore_bounds = T,
            ncsub = cbind(
              start = c(start_x, start_y, 1),
              count = c(count_x, count_y, NA)
            )
          ) |>
            suppressMessages()
        },
        start_x = start_x,
        start_y = start_y,
        count_x = count_x,
        count_y = count_y
      ))
  } else if (parallel == "future") {
    s_tile <-
      list_files |>
      furrr::future_map(
        \(f) {
          stars::read_ncdf(
            f,
            ignore_bounds = T,
            ncsub = cbind(
              start = c(start_x, start_y, 1),
              count = c(count_x, count_y, NA)
            )
          ) |>
            suppressMessages()
        }
      )
  }

  s_tile <-
    do.call(c, s_tile)

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

#' Loop through tiles and process them
#'
#' Function that first loads data from list_files, then runs a function (FUN)
#' on each tile in df_tiles, and then saves the output (a processed tile) in dir_tiles.
#'
#' @param df_tiles Data frame containing tile information
#' @param list_files List of files to load data from
#' @param FUN Function to apply to each tile
#' @param dir_tiles Directory to save processed tiles
#'
#' @export
rt_tile_loop <- function(df_tiles, list_files, FUN, dir_tiles) {
  # if dir_tiles does not exist, create it
  if (!fs::dir_exists(dir_tiles)) {
    fs::dir_create(dir_tiles)
  }

  # should rt_tile_h_load run in parallel?
  # depends on current plan
  if (is(future::plan(), "sequential")) {
    parallel_load <- T
  } else {
    parallel_load <- F
  }

  # loop through tiles
  furrr::future_pwalk(df_tiles, function(tile_id, start_x, start_y, count_x, count_y, ...) {
    # load all data within the tile
    s_tile <-
      rt_tile_h_load(start_x, start_y, count_x, count_y, list_files, parallel_load)

    # apply function
    s_tile <-
      FUN(s_tile)

    # save tile
    s_tile |>
      readr::write_rds(stringr::str_glue("{dir_tiles}/tile_{tile_id}.rds"))
  })
}


# *****

#' Mosaic tiles back together
#'
#' @param df_tiles Data frame containing tile information
#' @param dir_tiles Directory containing saved tiles
#' @param spatial_dims Spatial dimensions for the output
#' @param time_dim Time dimension subset (optional)
#'
#' @export
rt_tile_mosaic <- function(df_tiles, dir_tiles, spatial_dims, time_dim = NULL) {
  box::use(stars[...], sf[...], dplyr[...], purrr[...], future[...], furrr[...], stringr[...])

  s <-
    stars::st_as_stars(dimensions = spatial_dims)

  df_tiles <-
    df_tiles |>
    sf::st_drop_geometry()

  if (!is.null(time_dim)) {
    time_dim_full <-
      dir_tiles |>
      fs::dir_ls() |>
      dplyr::first() |>
      stars::read_ncdf(proxy = T) |>
      suppressMessages() |>
      stars::st_get_dimension_values("time")

    time_lims <-
      c(
        which(time_dim_full == dplyr::first(time_dim)),
        which(time_dim_full == dplyr::last(time_dim))
      )
  }

  # if tiling accounts for land coverage
  if (df_tiles |> names() |> stringr::str_detect("land") |> any()) {
    rows <-
      # loop through rows
      purrr::map(unique(df_tiles$start_y), function(row_i) {
        one_row <-
          df_tiles |>
          dplyr::filter(start_y == row_i) |>
          furrr::future_pmap(function(tile_id, start_x, end_x, start_y, end_y, land, ...) {
            # if tile has no land
            # create an empty stars object of the size of the tile
            if (land == FALSE) {
              ss <- s[, start_x:end_x, start_y:end_y]

              if (is.null(time_dim)) {
                empty_array <-
                  array(NA, dim = c(dim(ss)[1], dim(ss)[2]))
              } else {
                empty_array <-
                  array(NA, dim = c(dim(ss)[1], dim(ss)[2], time = length(time_dim)))
              }

              empty_stars <-
                stars::st_as_stars(empty_array)

              stars::st_dimensions(empty_stars)[1] <- stars::st_dimensions(ss)[1]
              stars::st_dimensions(empty_stars)[2] <- stars::st_dimensions(ss)[2]

              if (!is.null(time_dim)) {
                empty_stars <-
                  stars::st_set_dimensions(empty_stars, 3, values = time_dim)
              }

              return(empty_stars)

              # tile has land:
              # load from saved file
            } else {
              f_tile <-
                dir_tiles |>
                fs::dir_ls(regexp = stringr::str_glue("{tile_id}.nc"))

              if (is.null(time_dim)) {
                f_tile |>
                  stars::read_ncdf() |>
                  suppressMessages()
              } else {
                f_tile |>
                  stars::read_ncdf(
                    ncsub = cbind(
                      start = c(1, 1, time_lims[1]),
                      count = c(NA, NA, time_lims[2] - time_lims[1] + 1)
                    )
                  ) |>
                  suppressMessages()
              }
            }
          })

        # concatenate tiles from one row
        one_row <-
          do.call(c, c(one_row, along = 1))

        # fix dimension
        stars::st_dimensions(one_row)[1] <- stars::st_dimensions(s)[1]

        return(one_row)
      })

    # tiling does not account for land coverage:
    # load all tiles from saved files
  } else {
    rows <-
      purrr::map(unique(df_tiles$start_y), function(row_i) {
        one_row <-
          df_tiles |>
          dplyr::filter(start_y == row_i) |>
          furrr::future_pmap(function(tile_id, ...) {
            f_tile <-
              dir_tiles |>
              fs::dir_ls(regexp = stringr::str_glue("{tile_id}.nc"))

            if (is.null(time_dim)) {
              f_tile |>
                stars::read_ncdf() |>
                suppressMessages()
            } else {
              f_tile |>
                stars::read_ncdf(
                  ncsub = cbind(
                    start = c(1, 1, time_lims[1]),
                    count = c(NA, NA, time_lims[2] - time_lims[1] + 1)
                  )
                ) |>
                suppressMessages()
            }
          })

        stars::st_dimensions(one_row)[1] <- stars::st_dimensions(s)[1]

        return(one_row)
      })
  }

  # concatenate all rows
  mos <-
    do.call(c, c(rows, along = 2))

  # fix dimension
  stars::st_dimensions(mos)[2] <- stars::st_dimensions(s)[2]

  return(mos)
}


# *****

#' Mosaic tiles using GDAL utilities
#'
#' @param tile_files Vector of tile file paths
#' @param dir_res Directory for results
#' @param spatial_dims Spatial dimensions for output (optional)
#' @param time_dim Time dimension subset (optional)
#' @param time_full Full time vector for subsetting (optional)
#'
#' @export
rt_tile_mosaic_gdal <- function(
  tile_files,
  dir_res,
  spatial_dims = NULL,
  time_dim = NULL,
  time_full = NULL
) {
  if (!is.null(time_dim)) {
    # identify positions of time range in relation to full time vector
    time_pos <-
      c(which(time_full == dplyr::first(time_dim)), which(time_full == dplyr::last(time_dim)))

    # subset tile files
    # update paths

    dir_tiles_sub <- stringr::str_glue("{dir_res}/tiles")
    fs::dir_create(dir_tiles_sub)

    tile_files <-
      purrr::map(
        tile_files,
        purrr::in_parallel(
          \(f) {
            new_f <-
              stringr::str_glue(
                "{dir_tiles_sub}/{f |> fs::path_file() |> fs::path_ext_remove()}.tif"
              )

            stars::read_ncdf(
              f,
              ncsub = cbind(
                start = c(1, 1, time_pos[1]),
                count = c(NA, NA, time_pos[2] - time_pos[1] + 1)
              )
            ) |>
              suppressMessages() |>
              stars::write_stars(new_f)

            return(new_f)
          },
          dir_tiles_sub = dir_tiles_sub,
          time_pos = time_pos
        )
      ) |>
      unlist()
  }

  # mosaic with gdalwarp
  dir_mos <- stringr::str_glue("{dir_res}/mos")
  fs::dir_create(dir_mos)

  f_res <-
    stringr::str_glue("{dir_mos}/mos.tif")

  sf::gdal_utils(
    "warp",
    source = tile_files,
    destination = f_res,
    options = c("-dstnodata", "-9999")
  )

  s <-
    f_res |>
    stars::read_stars()

  if (!is.null(spatial_dims)) {
    s_ref <-
      stars::st_as_stars(dimensions = spatial_dims)

    s <-
      stars::st_warp(s, s_ref)
  }

  fs::dir_delete(dir_mos)
  if (fs::dir_exists(dir_tiles_sub)) {
    fs::dir_delete(dir_tiles_sub)
  }

  return(s)
}
