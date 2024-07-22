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
#' @param outdir A character string specifying output directory where data
#' frames are written using the \code{save} statement. If omitted (defaults to
#' \code{NA}), a tidy data frame containing all data is returned.
#' @param fileprefix A character string specifying the file name prefix.
#' @param ilon An integer specifying an individual longitude index for chunking
#' all processing and writing chunks to separate output files. If provided,
#' it overrides that the function extracts data for all longitude indices. If
#' omitted (\code{ilon = NA}), the function returns tidy data for all longitude
#' indexes.
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
    timenam,
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
                          varnam = varnam,
                          lonnam = lonnam,
                          latnam = latnam,
                          timenam = timenam,
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
#' @param varnam The variable name(s) for which data is to be read from NetCDF
#' files.
#' @param lonnam The dimension name of longitude in the NetCDF files.
#' @param latnam The dimension name of latitude in the NetCDF files.
#' @param timenam The name of dimension variable used for timein the NetCDF
#' files. Defaults to \code{"time"}.
#' @param fgetdate A function to derive the date used for the time dimension
#' based on the file name.
#'
#' @return not sure
#' @export

nclist_to_df_byfil <- function(
    filnam,
    ilon = NA,
    varnam,
    lonnam,
    latnam,
    timenam,
    fgetdate
){

  # CRAN HACK, use .data$ syntax for correct fix
  index <- lat <- lon <- name <- value <- NULL

  # Setup extraction
  ncdf <- tidync::tidync(filnam)

  # check if requested dimensions and variables exist
  ncdf_available_dims <- tidync::hyper_dims(ncdf)
  ncdf_available_vars <- tidync::hyper_vars(ncdf)
  err_msg_lon <- sprintf(
    "For file %s:\n  Provided name of longitudinal dimension as '%s', which is not among available dims: %s",
    filnam, lonnam, paste0(ncdf_available_dims$name, collapse = ","))
  err_msg_lat <- sprintf(
    "For file %s:\n  Provided name of latitudinal dimension as '%s', which is not among available dims: %s",
    filnam, latnam, paste0(ncdf_available_dims$name, collapse = ","))
  err_msg_time <- sprintf(
    "For file %s:\n  Provided name of time dimension as '%s', which is not among available dims: %s",
    filnam, timenam, paste0(ncdf_available_dims$name, collapse = ","))
  lonnam %in% ncdf_available_dims$name || stop(err_msg_lon)
  latnam %in% ncdf_available_dims$name || stop(err_msg_lat)
  timenam%in% ncdf_available_dims$name || is.na(timenam) || stop(err_msg_time)
  err_msg_var <- sprintf(
    "For file %s:\n  Requested variable '%s', which is not among available variables: %s",
    filnam, varnam, paste0(ncdf_available_vars$name, collapse = ","))
  varnam %in% ncdf_available_vars$name || stop(err_msg_var)

  # get data
  if (identical(NA, ilon)){
    # get all data i.e. do not filter ncdf
  } else {
    # filter data to longitudinal band `ilon`
    # deal with dynamic longitude dimension name
    if (lonnam == "lon"){
      ncdf <- tidync::hyper_filter(ncdf, lon = dplyr::near(index, ilon))
    } else if (lonnam == "longitude"){
      ncdf <- tidync::hyper_filter(ncdf, longitude = dplyr::near(index, ilon))
    } else {
      stop(sprintf("Received lonnam argument: '%s'. Currently only 'lon' or 'longitude' are supported by map2tidy. Your case needs to be added.", lonnam))
    }
  }
  # collect data into tibble
  df <- ncdf |>
    tidync::hyper_tibble(tidyselect::vars_pull(varnam),
                         drop=FALSE) |>
    # hardcode: lon and lat as longitude and latitude
    dplyr::rename(lon = !!lonnam, lat = !!latnam) |>
    # transform to numeric
    dplyr::mutate(lon = as.numeric(lon), lat = as.numeric(lat))
  # NOTE: df based on tidync (< v.0.3.0.9002) contains integer time column
  # NOTE: df based on tidync (>= v.0.3.0.9002) contains string time column

  # Overwrite parsed time if fgetdate provided
  if (!is.na(fgetdate)){
    # if fgetdate provided, use this function to derive time(s) from filename
    if (!is.na(timenam)){
      df <- df |>
        dplyr::mutate(!!timenam := fgetdate(filnam))
    } else {
      warning("Ignored argument 'fgetdate', since no argument 'timenam' provided.")
    }
  }

  if (is.na(fgetdate)){ # IF NEEDED (i.e. only with old version of tidync)
    if (!is.na(timenam)){# parse time if timenam provided

      # with tidync v 0.3.0.9002 nothing needed: https://github.com/ropensci/tidync/issues/54
      # with previous tidync:
      if (packageVersion("tidync") >= "0.3.0.9002") {
        # or equivalently: typeof(df[[timenam]]) == "character"
        # nothing needed: https://github.com/ropensci/tidync/issues/54
      } else {
        # else, i.e. if (typeof(df[[timenam]]) == "numeric")

        # otherwise use package CFtime to get time strings from metadata
                      # get_time_strings <- function(filename, timenam="time"){
                      #   cf_list <- lapply(
                      #     filename,
                      #     function(fn){CFtime::as_timestamp(get_CFtime(fn, timenam="time"))})
                      #   return(sort(purrr::list_c(cf_list)))
                      # }
                      # get_CFtime <- function(filename, timenam="time"){
                      #   nc <- ncdf4::nc_open(filename)
                      #   cf <- CFtime::CFtime( # create a CFtime instance
                      #     nc$dim[[timenam]]$units,
                      #     nc$dim[[timenam]]$calendar,
                      #     nc$dim[[timenam]]$vals)
                      #   ncdf4::nc_close(nc)
                      #   return(cf)
                      # }
                      # time_strings <- get_time_strings(filnam, timenam = timenam)
        nc <- ncdf4::nc_open(filnam)
        units    <- nc$dim[[timenam]]$units
        calendar <- nc$dim[[timenam]]$calendar
        ncdf4::nc_close(nc)

        df <- df |>
          dplyr::mutate(
            !!timenam :=
              CFtime::as_timestamp(CFtime::CFtime(definition = units,
                                                  calendar = calendar,
                                                  offsets = time)))
      }
    }
  }

  return(df)
}
