add_loess <- function(df){
  # for the example in parallel_computation.Rmd
  df <- df |>
    dplyr::mutate(year_dec = lubridate::decimal_date(time)) %>%
    dplyr::mutate(loess = stats::loess( et ~ year_dec,
                                        data = .,
                                        span = 0.05 )$fitted)
  return(df)
}
