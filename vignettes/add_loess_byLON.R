add_loess_byLON <- function(LON_str){
  # R CMD Check HACK, use .data$ syntax (or {{...}}) for correct fix https://stackoverflow.com/a/63877974
  index <- lat <- lon <- name <- value <- out <- datetime <- lon_value <- data <- time <- NULL

  # for the example in parallel_computation.Rmd
  source(file.path(here::here(), "vignettes/add_loess.R"))

  # read from file that contains tidy data for a single longitudinal band
  filnam <- file.path(tempdir(), paste0("demo_data_2017_", LON_str, ".rds"))

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
                   file.path(tempdir(), paste0("demo_data_2017_", LON_str, "_COMPLEMENTED.rds")))
}
