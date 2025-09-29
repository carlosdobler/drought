library(reticulate)

py_require("xarray[complete]")

xr <- import("xarray")
np <- import("numpy")

file_path <- "~/projects/drought/functions/pet-pm_mon_UKESM1-0-LL_NEX_1971.nc"


tictoc::tic()
a <- stars::read_ncdf(file_path)
tictoc::toc()

tictoc::tic()
nc <- ncdf4::nc_open(file_path)
b <- ncdf4::ncvar_get(nc)
tictoc::toc()

file_path <- "~/projects/drought/functions/pet-pm_mon_UKESM1-0-LL_NEX_1971.nc"
nc <- ncdf4::nc_open(file_path)

bench::mark(
  stars::read_ncdf(file_path, make_time = F, make_units = F) |> dplyr::pull() |> dim() |> unname(),
  ncdf4::ncvar_get(nc) |> dim()
)




ds <- xr$open_dataset(file_path)

ds$to_dataframe() -> a
a

py_to_r(ds$values) -> a
st_as_stars(a)


library(stars)
read_mdim(ds)

read_netcdf_chunk <- function(file_path, var_name = NULL, 
                             time_slice = NULL, lat_slice = NULL, lon_slice = NULL,
                             chunks = NULL) {
  
  ds <- xr$open_dataset(file_path, chunks = chunks)
  
  if (!is.null(var_name)) {
    data <- ds[[var_name]]
  } else {
    data <- ds
  }
  
  if (!is.null(time_slice)) {
    data <- data$isel(time = slice(time_slice[1], time_slice[2]))
  }
  
  if (!is.null(lat_slice)) {
    data <- data$isel(lat = slice(lat_slice[1], lat_slice[2]))
  }
  
  if (!is.null(lon_slice)) {
    data <- data$isel(lon = slice(lon_slice[1], lon_slice[2]))
  }
  
  result <- data$load()
  ds$close()
  
  return(result)
}

file_path <- "pet-pm_mon_UKESM1-0-LL_NEX_1971.nc"

chunk_data <- read_netcdf_chunk(
  file_path = file_path,
  time_slice = c(0, 5),
  lat_slice = c(0, 100),
  lon_slice = c(0, 100),
  chunks = list(time = 1L, lat = 50L, lon = 50L)
)

print(chunk_data)