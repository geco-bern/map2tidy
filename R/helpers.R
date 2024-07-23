#' Returns a tidy data frame from a NetCDF file for a longitudinal band
#'
#' @param nclist A vector of character strings specifying the complete paths to
#' files.
#' @param varnames The variable name(s) for which data is to be read from NetCDF
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
#' omitted (\code{ilon = NA}), the function outputs/returns tidy data for all
#' longitude indexes.
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
    varnames,
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
      ~ncfile_to_df(.,
                    ilon,
                    varnames = varnames,
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
        dplyr::mutate(data = purrr::map(data, ~dplyr::arrange(., datetime)))

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


#' Returns a tidy data.frame from a single NetCDF file,
#' optionally subsetting a single longitudinal band
#'
#' @param filnam file name
#' @param ilon An integer specifying an individual longitude index to subset.
#' If provided, ilon overrides that the function extracts data for all longitude indices. If
#' omitted (\code{ilon = NA}), the function returns tidy data for all longitude
#' indexes.
#' @param varnames The variable name(s) for which data is to be read from the
#' NetCDF file.
#' @param lonnam The dimension name of longitude in the NetCDF file.
#' @param latnam The dimension name of latitude in the NetCDF file.
#' @param timenam The name of dimension variable used for time in the NetCDF file.
#' @param fgetdate A function to derive the date(s) used for the time dimension
#' based on the file name.
#'
#' @return Tidy tibble containing the variables 'varnames'.
#'         Tibble contains columns 'lon' (double), 'lat' (double), and a nested
#'         column 'data'. Column 'data' contains requested variables (probably
#'         as doubles) and might contain a column 'datetime' (as string).
#'         Note that the datetime is defined by package CFtime and can contain
#'         dates such as "2021-02-30 12:00:00", which are valid for 360-day
#'         calendars but not for POSIXt. Because of that these dates need to be
#'         parsed separately.
#' @export

ncfile_to_df <- function(
    filnam,
    ilon = NA, # ilon_lower, ilon_upper,
    varnames,
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
    filnam, varnames, paste0(ncdf_available_vars$name, collapse = ","))
  varnames %in% ncdf_available_vars$name || stop(err_msg_var)

  # get data
  if (identical(NA, ilon)){
    # get all data i.e. do not subset ncdf
  } else {
    # subset data to longitudinal band `ilon`
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
    tidync::hyper_tibble(tidyselect::vars_pull(varnames),
                         drop=FALSE) |>
    # hardcode colnames: lon and lat as longitude and latitude, and datetime
    # dplyr::rename(lon = !!lonnam, lat = !!latnam) |>
    dplyr::rename(dplyr::any_of(c(
      lon      = lonnam,
      lat      = latnam,
      datetime = timenam))) |> # columns might or might not exist
    # transform to numeric
    dplyr::mutate(lon = as.numeric(lon), lat = as.numeric(lat))
  # NOTE: df based on tidync (< v.0.3.0.9002) contains integer datetime column
  # NOTE: df based on tidync (>= v.0.3.0.9002) contains string datetime column

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

  # IF NEEDED (i.e. only with tidync version <0.3.0.9002)
  if (is.na(fgetdate)){
    if (!is.na(timenam)){# parse integer datetime if timenam provided

      # with tidync v 0.3.0.9002 nothing needed: https://github.com/ropensci/tidync/issues/54
      # with previous tidync:
      if (utils::packageVersion("tidync") >= "0.3.0.9002") {
        # or equivalently: typeof(df[[timenam]]) == "character"
        # nothing needed: https://github.com/ropensci/tidync/issues/54
      } else {
        # else, i.e. if (typeof(df[[timenam]]) == "numeric")
        # parse integer datetime

        # use package CFtime to get time strings from metadata and integers
        nc <- ncdf4::nc_open(filnam)
        units    <- nc$dim[[timenam]]$units
        calendar <- nc$dim[[timenam]]$calendar
        ncdf4::nc_close(nc)

        df <- df |>
          dplyr::mutate(
            !!timenam :=
              CFtime::as_timestamp(CFtime::CFtime(definition = units,
                                                  calendar = calendar,
                                                  offsets = datetime)))
      }
    }
  }

  return(df)
}
