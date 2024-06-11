#' Returns a tidy data frame from a NetCDF file for a longitudinal band
#'
#' @param nclist A vector of character strings specifying the complete paths to
#' files.
#' @param varnam The variable name(s) for which data is to be read from NetCDF
#' files.
#' @param lonnam The dimension name of longitude in the NetCDF files.
#' @param latnam The dimension name of latitude in the NetCDF files.
#' @param timenam The name of dimension variable used for timein the NetCDF
#' files. Defaults to \code{NA}.
#' @param timedimnam The name of the dimension (axis) used for time.
#' Defaults to \code{NA}.
#' @param noleap A logical specifying whether the calendar of the NetCDF time
#' axis contains leap years. If it doesn't, \code{noleap} is \code{TRUE}.
#' Defaults to \code{NA} - no prior information specified and dynamically inter-
#' preted from NetCDF file.
#' @param res_time A character specifying the resolution of the time axis.
#' Available: \code{c("mon", "day")}. Defaults to \code{"day"}.
#' @param outdir A character string specifying output directory where data
#' frames are written using the \code{save} statement. If omitted (defaults to
#' \code{NA}), a tidy data frame containing all data is returned.
#' @param fileprefix A character string specifying the file name prefix.
#' @param ilon An integer specifying an individual longitude index for chunking
#' all processing and writing chunks to separate output files. If provided,
#' it overrides that the function extracts data for all longitude indices. If
#' omitted (\code{ilon = NA}), the function returns tidy data for all longitude
#' indexes.
#' @param basedate not sure
#' @param fgetdate A function to derive the date used for the time dimension
#' based on the file name.
#' @param overwrite A logical indicating whether time series files are to be
#' overwritten.
#'
#' @return not sure
#' @export

nclist_to_df_byilon <- function(
    nclist,
    ilon,
    outdir,
    fileprefix,
    varnam,
    lonnam,
    latnam,
    basedate,
    timenam,
    timedimnam,
    noleap,
    res_time,
    fgetdate,
    overwrite
){

  # CRAN HACK, use .data$ syntax for correct fix
  lat <- lon <- value <- NULL

  if (!is.na(outdir)){
    # check whether output has been created already (otherwise do nothing)
    if (!dir.exists(outdir)){system(paste0("mkdir -p ", outdir))}
    outpath <- paste0(outdir, "/", fileprefix, "_ilon_", ilon, ".rds")
  } else {
    outpath <- dirname(nclist[1])
  }

  if (!file.exists(outpath) || overwrite || is.na(outdir)){

    # get data from all files at given longitude index ilon
    df <- purrr::map(
      as.list(nclist),
      ~nclist_to_df_byfil(.,
                          ilon,
                          basedate = basedate,
                          varnam = varnam,
                          lonnam = lonnam,
                          latnam = latnam,
                          timenam = timenam,
                          timedimnam = timedimnam,
                          noleap = noleap,
                          res_time = res_time,
                          fgetdate
      )
    )

    # check if any element has zero rows and drop that element
    drop_zerorows <- function(y) { return(y[!sapply(y,
                                                    function(x) nrow(x)==0 )]) }
    df <- df |>
      drop_zerorows()

    # nest only if there is a time dimension
    if (identical(timenam, NA)){

      df <- df |>
        dplyr::bind_rows()

    } else if (length(df) > 0){

      df <- df |>
        dplyr::bind_rows() |>
        dplyr::group_by(lon, lat) |>
        tidyr::nest() |>
        dplyr::mutate(data = purrr::map(data, ~dplyr::arrange(., time)))

    }

    if (!is.na(outdir)){
      message(paste("Writing file", outpath, "..."))
      readr::write_rds(df, file = outpath)
      rm("df")
    }

  } else {
    message(paste("File exists already:", outpath))
  }

  if (is.na(outdir)){
    return(dplyr::bind_rows(df))
  } else {
    return(ilon)
  }
}


#' Returns a tidy data frame from a NetCDF file, optionally for a longitudinal
#' band
#'
#' @param filnam file name
#' @param ilon An integer specifying an individual longitude index for chunking
#' all processing and writing chunks to separate output files. If provided,
#' it overrides that the function extracts data for all longitude indices. If
#' omitted (\code{ilon = NA}), the function returns tidy data for all longitude
#' indexes.
#' @param basedate reference data for NetCDF time dimension
#' @param varnam The variable name(s) for which data is to be read from NetCDF
#' files.
#' @param lonnam The dimension name of longitude in the NetCDF files.
#' @param latnam The dimension name of latitude in the NetCDF files.
#' @param timenam The name of dimension variable used for timein the NetCDF
#' files. Defaults to \code{"time"}.
#' @param timedimnam The name of the dimension (axis) used for time.
#' Defaults to \code{"time"}.
#' @param noleap A logical specifying whether the calendar of the NetCDF time
#' axis contains leap years. If it doesn't, \code{noleap} is \code{TRUE}.
#' Defaults to \code{NA} - no prior information specified and dynamically inter-
#' preted from NetCDF file.
#' @param res_time A character specifying the resolution of the time axis.
#' Available: \code{c("mon", "day")}. Defaults to \code{"day"}.
#' @param fgetdate A function to derive the date used for the time dimension
#' based on the file name.
#'
#' @return not sure
#' @export

nclist_to_df_byfil <- function(
    filnam,
    ilon = NA,
    basedate,
    varnam,
    lonnam,
    latnam,
    timenam,
    timedimnam,
    noleap,
    res_time,
    fgetdate
){

  # CRAN HACK, use .data$ syntax for correct fix
  index <- lat <- lon <- name <- value <- NULL

  if (is.na(basedate) && is.na(fgetdate) && !is.na(timenam)){
    # get base date (to interpret time units in 'days since X')
    basedate <- ncmeta::nc_atts(filnam, timenam) |>
      dplyr::filter(name != "_FillValue") |>
      tidyr::unnest(cols = c("value")) |>
      dplyr::filter(name == "units") |>
      dplyr::pull(value) |>
      stringr::str_remove("days since ") |>
      stringr::str_remove(" 00:00:00") |>
      stringr::str_remove(" 0:0:0") |>
      lubridate::ymd()
  }

  if (identical(NA, ilon)){
    # get all data
    df <- tidync::tidync(filnam) |>
      tidync::hyper_tibble(tidyselect::vars_pull(varnam))

  } else {
    # subset data to longitudinal band
    df <- tidync::tidync(filnam) |>
      tidync::hyper_filter(lon = dplyr::near(index, ilon)) |>
      tidync::hyper_tibble(tidyselect::vars_pull(varnam))

    # # Deal with dynamic longitude dimension name
    # df <- tidync::tidync(filnam) |>
    #   tidync::hyper_tibble(tidyselect::vars_pull(varnam))
    #
    # lon_vec <- sort(unique(df[[lonnam]]))
    # lon_select <- lon_vec[ilon]
    #
    # df <- df |>
    #   dplyr::filter(!!lonnam == lon_select)
  }

  if (nrow(df)>0){

    if (!is.na(timenam)){
      if (!is.na(fgetdate)){
        df <- df |>
          dplyr::rename(lon = !!lonnam, lat = !!latnam) |>
          dplyr::mutate(time = fgetdate(filnam))

      } else {
        if (noleap){
          # if calendar of netcdf time dimension is no-leap, then do...
          # determine last year in file.
          last_date <- basedate + lubridate::days(floor(time[length(time)])) - lubridate::days(1)
          last_year <- lubridate::year(last_date)

          # "manually" remove additional day in leap years
          df_noleap <- tibble(
            time = seq(from = basedate, to = lubridate::ymd(paste0(last_year , "-12-31")), by = "days")
          ) |>
            dplyr::mutate(month = lubridate::month(date), mday = lubridate::mday(time)) |>
            dplyr::filter(!(month == 2 & mday == 29))

          if (res_time == "mon"){
            # monthly resolution - interpret for the 15th of each month
            df_noleap <- df_noleap |>
              dplyr::filter(mday == 15)
          }

          # at this stage, number of rows in df_noleap should correspond to length of the time dimension in netcdf file
          df <- df |>
            dplyr::select(-!!timedimnam) |>
            dplyr::bind_cols(
              df_noleap
            ) |>
            dplyr::rename(lon = !!lonnam, lat = !!latnam)

        } else {
          df <- df |>
            dplyr::rename(time = !!timedimnam, lon = !!lonnam, lat = !!latnam) |>
            dplyr::mutate(time = basedate + lubridate::days(floor(time)) - lubridate::days(1))

        }
      }
    }

  }

  return(df)
}
