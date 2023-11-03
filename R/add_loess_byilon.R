add_loess_byilon <- function(ilon){

  # for the example in parallel_computation.Rmd

  source(paste0(here::here(), "/R/add_loess.R"))

  # read from file that contains tidy data for a single longitudinal band
  filnam <- list.files(paste0(here::here(), "/data/"),
                       pattern = paste0("_", as.character(ilon), ".rds"),
                       full.names = TRUE)

  df <- readr::read_rds(filnam)

  df <- df |>

    # Uncomment code below to nest data by gridcell, if not already nested.
    # # group data by gridcells and wrap time series for each gridcell into a new
    # # column, by default called 'data'.
    # dplyr::group_by(lon, lat) |>
    # tidyr::nest() |>

    # apply the custom function on the time series data frame separately for
    # each gridcell.
    dplyr::mutate(data = purrr::map(data, ~add_loess(.)))

  # write (complemented) data to file
  readr::write_rds(df,
                   paste0(here::here(),
                          "/data/demo_data_2017_ilon_",
                          ilon,
                          "_COMPLEMENTED.rds"))
}
