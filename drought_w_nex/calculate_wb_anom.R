scenario <- "ssp585"
baseline <- seq(1991, 2020)
int_windws <- c(3, 12)


library(tidyverse)
library(stars)
# library(furrr)
library(mirai)

source("functions/general_tools.R")
source("functions/tile.R")

# options(future.fork.enable = T)
# options(future.rng.onMisuse = "ignore")
# plan(multicore)

daemons(n = 13)
cl <- make_cluster(13)


dir_tmp <- "/mnt/pers_disk/tmp"
dir_tiles <- "/mnt/pers_disk/tiles"
dir_mos <- "/mnt/pers_disk/mos"

fs::dir_create(dir_tmp)
fs::dir_create(dir_tiles)
fs::dir_create(dir_mos)


yrs <- seq(1970, 2099)

model_dirs <-
  rt_gs_list_files("gs://clim_data_reg_useast1/nex/monthly_aggregates/water_balance_pm/")


# TILE

f <-
  rt_gs_list_files(model_dirs[1]) |>
  first() |>
  rt_gs_download_files(tempdir(), quiet = T)

s_proxy <-
  f |>
  read_ncdf() |>
  slice(time, 1)

df_tiles <-
  rt_tile_table(s_proxy, 50, land = s_proxy)

df_tiles_land <-
  df_tiles |>
  filter(land == T)

fs::file_delete(f)


walk(model_dirs |> tail(-21), \(model_dir) {
  # model_dir = model_dirs[2]

  walk(int_windws, \(windw) {
    fs::dir_create(str_glue("{dir_tiles}/w{windw}"))
  })

  model <- model_dir |> str_split("/") |> unlist() %>% .[7]

  message(str_glue("PROCESSING MODEL {model}"))

  # download files
  ff <-
    rt_gs_list_files(model_dir) |>
    # head(20) |>
    rt_gs_download_files(dir_tmp, quiet = T)

  yrs_vector <-
    ff |>
    str_extract("(?<=NEX_)[:digit:]{4}(?=.nc)") |>
    as.numeric()

  yrs_baseline_ind <-
    which(yrs_vector %in% baseline)

  # load data
  pwalk(df_tiles_land, function(tile_id, start_x, start_y, count_x, count_y, ...) {
    # tile_id = "124"
    # start_x = 497
    # count_x = 50
    # start_y = 151
    # count_y = 50
    #
    # tile_id = "342"
    # start_x = 1391
    # count_x = 50
    # start_y = 251
    # count_y = 50

    message(str_glue(
      "importing tile {which(df_tiles_land$tile_id == tile_id)} / {nrow(df_tiles_land)}"
    ))

    # load all data within the tile
    s_tile <-
      ff |>
      map(purrr::in_parallel(
        \(f) rt_tile_load(start_x, start_y, count_x, count_y, f),
        rt_tile_load = rt_tile_load,
        start_x = start_x,
        start_y = start_y,
        count_x = count_x,
        count_y = count_y
      ))

    s_tile <-
      do.call(c, c(s_tile, along = "time"))

    # tictoc::tic()
    walk(int_windws, \(windw) {
      s_wb_perc <-
        s_tile |>
        st_apply(
          c(1, 2),
          \(x, yrs_baseline_ind) {
            if (all(is.na(x))) {
              rep(NA, length(x))
            } else {
              x_rolled <-
                slider::slide_dbl(x, .f = sum, .before = windw - 1, .complete = T)

              x_rolled |>
                matrix(ncol = 12, byrow = T) |>
                apply(2, \(m) {
                  params <-
                    m[yrs_baseline_ind] |>
                    lmom::samlmu() |>
                    lmom::pelglo()

                  m <-
                    m |>
                    lmom::cdfglo(params)

                  round(m * 100)
                  #
                }) |>
                t() |>
                as.vector()
            }
          },
          yrs_baseline_ind = yrs_baseline_ind,
          CLUSTER = cl,
          .fname = "time"
        ) |>
        aperm(c(2, 3, 1))

      st_dimensions(s_wb_perc)[3] <- st_dimensions(s_tile)[3]

      s_wb_perc |>
        rt_write_nc(str_glue("{dir_tiles}/w{windw}/tile_w{windw}_{tile_id}.nc"))
    })
    # tictoc::toc()
  })

  # MOSAIC

  ff_tiles <-
    map(int_windws |> set_names(), \(windw) {
      str_glue("{dir_tiles}/w{windw}") |>
        fs::dir_ls()
    })

  full_time_vector <-
    ff_tiles[[1]] |>
    first() |>
    read_ncdf(ncsub = cbind(start = c(1, 1, 1), count = c(1, 1, NA))) |>
    suppressMessages() |>
    st_get_dimension_values("time")

  yrs_vector |>
    tail(-1) |>
    walk(\(yr) {
      iwalk(ff_tiles, \(f, windw) {
        message(str_glue("  W {windw}: {yr}"))

        mos <-
          rt_tile_mosaic_gdal(
            f,
            dir_mos,
            spatial_dims = st_dimensions(s_proxy),
            time_dim = seq(
              as_date(str_glue("{yr}-01-01")),
              as_date(str_glue("{yr}-12-01")),
              by = "1 month"
            ),
            time_full = full_time_vector
          ) |>
          setNames("wb-percentile")

        f_res <- str_glue(
          "{dir_mos}/wb-pm-perc_w{windw}_bl-{str_flatten(range(baseline), '-')}_mon_{model}_NEX_{yr}.nc"
        )

        rt_write_nc(
          mos,
          f_res,
          gatt_name = "source code",
          gatt_val = "https://github.com/carlosdobler/drought/drought_w_nex"
        )

        system(
          str_glue(
            "gcloud storage mv {f_res} gs://clim_data_reg_useast1/nex/monthly_aggregates/water_balance_pm_perc/{model}/"
          ),
          ignore.stdout = T,
          ignore.stderr = T
        )
      })
    })

  # df_tiles_land |> filter(tile_id == "292") -> tile
  # "/mnt/pers_disk/tiles/tile_w3_292.nc" |> read_ncdf() -> tile_data
  # "/mnt/bucket_mine/nex/monthly_aggregates/water_balance_pm_quantile/ACCESS-CM2/wb-pm-quantile_w3_bl-1991-2020_mon_ACCESS-CM2_NEX_1971.nc" |> read_ncdf() -> mos_data
  # mos_data[tile]
  # tile_data |> slice(time, 13:24)
  # mos_data[tile] |> slice(time, 1) |> plot()
  # tile_data |> slice(time, 13) |> plot()

  fs::dir_ls(dir_tmp) |> fs::file_delete()
  fs::dir_ls(dir_tiles) |> fs::file_delete()
})

fs::dir_delete(dir_tmp)
fs::dir_delete(dir_tiles)
fs::dir_delete(dir_mos)
stop_cluster(cl)
