add_loess <- function(df){
  # R CMD Check HACK, use .data$ syntax (or {{...}}) for correct fix https://stackoverflow.com/a/63877974
  index <- lat <- lon <- name <- value <- out <- datetime <- lon_value <- data <- time <- NULL

  # for the example in parallel_computation.Rmd
  df <- df |>
    mutate(year_dec = lubridate::decimal_date(lubridate::ymd(datetime))) %>%
    dplyr::mutate(loess = stats::loess( et ~ year_dec,
                                        data = .,
                                        span = 0.05 )$fitted)
  return(df)
}
