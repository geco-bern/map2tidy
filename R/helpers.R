#' Returns a data.frame defining longitude value and longitude index for a given
#' NetCDF file
#'
#' @param ncdf Either a character string specifying the complete path to a
#' NetCDF file, or an object of class \code{tidync::tidync}.
#' @param lonnam The dimension name of longitude in the NetCDF files.
#'
#' @return Tidy tibble containing the variables 'varnam'.

get_longitude_value_indices <- function(ncdf, lonnam){
  if (methods::is(ncdf, "tidync")) {
  } else {
    ncdf <- tidync::tidync(ncdf)
  }

  # check if requested longitude dimension exists
  ncdf_available_dims <- tidync::hyper_dims(ncdf)
  err_msg_lon <- sprintf(
    "For file %s:\n  Provided name of longitudinal dimension as '%s', which is not among available dims: %s",
    ncdf$source$source, lonnam, paste0(ncdf_available_dims$name, collapse = ","))
  lonnam %in% ncdf_available_dims$name || stop(err_msg_lon)

  res <- ncdf$transforms[[lonnam]][, c(lonnam, "index")]
  res <- dplyr::rename(res, dplyr::all_of(c(lon_value=lonnam, lon_index="index")))
  return(res)
}

#' Returns a tidy data.frame from a list of NetCDF file(s),
#' optionally subsetting a single a longitudinal band
#'
#' @param nclist A vector of character strings specifying the complete paths to
#' files.
#' @param varnam The variable name(s) for which data is to be read from the
#' NetCDF files.
#' @param lonnam The dimension name of longitude in the NetCDF files.
#' @param latnam The dimension name of latitude in the NetCDF files.
#' @param timenam The name of dimension variable used for time in the NetCDF
#' files.
#' @param outdir A character string specifying output directory where data
#' frames are written using the \code{save} statement. If omitted (defaults to
#' \code{NA}), a tidy data frame containing all data is returned.
#' @param fileprefix A character string specifying the file name prefix.
#' @param ilon An integer specifying an individual longitude index to subset.
#' If provided, only longitude index 'ilon' is extracted. If omitted
#' (\code{ilon = NA}), the function returns tidy data for all longitude
#' indexes.
#' @param fgetdate A function to derive the date(s) used for the time dimension
#' based on the file name.
#' @param overwrite A logical indicating whether time series files are to be
#' overwritten.
#'
#' @return Tidy tibble containing the variables 'varnam'.
#'         Tibble contains columns 'lon' (double), 'lat' (double), and a nested
#'         column 'data'.
#'         Column 'data' contains requested variables (probably
#'         as doubles) and might contain a column 'datetime' (as string).
#'         Note that the datetime is defined by package CFtime and can contain
#'         dates such as "2021-02-30 12:00:00", which are valid for 360-day
#'         calendars but not for POSIXt. Because of that these dates need to be
#'         parsed separately.
#'         If \code{!is.na(outdir)}, then an RDS file is generated in
#'         \code{outdir} and column 'data' of return value contains a status
#'         message only instead of data.
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

  # R CMD Check HACK, use .data$ syntax (or {{...}}) for correct fix https://stackoverflow.com/a/63877974
  index <- lat <- lon <- name <- value <- out <- datetime <- lon_index <- lon_value <- data <- time <- NULL

  # create file name
  if (!is.na(outdir)){
    df_indices <- get_longitude_value_indices(nclist[1], lonnam)
    lon_values <- ifelse(
      is.na(ilon),
      sprintf("%+.3f_to_%+.3f",
              min(df_indices$lon_value), max(df_indices$lon_value)),
      sprintf("%+.3f",   # "%+.3g",
              dplyr::pull(dplyr::filter(df_indices, lon_index == ilon), "lon_value"))
    )
    outpath <- paste0(file.path(outdir, fileprefix), "_LON_", lon_values, ".rds")
  }

  if (is.na(outdir) || !file.exists(outpath) || overwrite){

    # get data from all files at given longitude index ilon
    df_list <- purrr::map(
      as.list(nclist),
      ~ncfile_to_df(.,
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
    df <- df_list |>
      # drop_zerorows() |>
      dplyr::bind_rows()

    # nest only if there is a time dimension
    if ("datetime" %in% names(tidyr::unnest(df, data))){
      df <- df |>
        tidyr::unnest(data) |>               # unnest the rows from individual NetCDF files
        tidyr::nest(data = !c(lon, lat)) |>  # nest the rows for same coords across NetCDF files
        dplyr::arrange(lon, lat) |>
        dplyr::mutate(data = purrr::map(data, ~dplyr::arrange(., datetime)))
    } else {
      df <- df |>
        tidyr::unnest(data) |>               # unnest the rows from individual NetCDF files
        dplyr::arrange(lon, lat)
    }

    if (!is.na(outdir)){
      if (nrow(df) > 0){
        # message(paste("Writing file", outpath, "..."))
        readr::write_rds(df, file = outpath)
        return(df |> dplyr::select(lon) |> dplyr::distinct() |> dplyr::mutate(
          data = paste0("Written data by worker with jobid: ", Sys.getpid(), " into file: ", outpath)))
      } else {
        warning(paste("Omitting file", outpath, "..."))
        return(dplyr::filter(df_indices, lon_index == ilon) |>
                 dplyr::select(lon=lon_value) |> dplyr::mutate(
          data = paste0("No data read in, and hence omitted writing out file: ", outpath)))
      }
    } else {
      return(df)
    }

  } else {
    warning(paste0("File exists already: ", outpath))
    # return(df |> dplyr::select(lon) |> dplyr::distinct() |> dplyr::mutate(
    #   data = paste0("File exists already: ", outpath)))  # NOTE: can't output lon value if we don't read the NCfile
    return(data.frame(lon=NA, data=paste0("File exists already: ", outpath)))
  }
}


#' Returns a tidy data.frame from a single NetCDF file,
#' optionally subsetting a single longitudinal band
#'
#' @param filnam file name
#' @param ilon An integer specifying an individual longitude index to subset.
#' If provided, ilon overrides that the function extracts data for all longitude indices. If
#' omitted (the default: \code{ilon = NA}), the function returns tidy data for
#' all longitude indices.
#' @param varnam The variable name(s) for which data is to be read from the
#' NetCDF file.
#' @param lonnam The dimension name of longitude in the NetCDF file.
#' @param latnam The dimension name of latitude in the NetCDF file.
#' @param timenam The name of dimension variable used for time in the NetCDF file.
#' @param fgetdate A function to derive the date(s) used for the time dimension
#' based on the file name.
#'
#' @return Tidy tibble containing the variables 'varnam'.
#'         Tibble contains columns 'lon' (double), 'lat' (double), and a nested
#'         column 'data'.
#'         Column 'data' contains requested variables (probably
#'         as doubles) and might contain a column 'datetime' (as string).
#'         Note that the datetime is defined by package CFtime and can contain
#'         dates such as "2021-02-30 12:00:00", which are valid for 360-day
#'         calendars but not for POSIXt. Because of that these dates need to be
#'         parsed separately.
#' @export

ncfile_to_df <- function(
    filnam,
    ilon = NA,
    varnam,
    lonnam,
    latnam,
    timenam,
    fgetdate
){
  # R CMD Check HACK, use .data$ syntax (or {{...}}) for correct fix https://stackoverflow.com/a/63877974
  index <- lat <- lon <- name <- value <- out <- datetime <- lon_index <- lon_value <- data <- time <- NULL

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
    "For file %s:\n  Requested variable(s) '%s', which are not all among available variables: %s",
    filnam, paste0(varnam, collapse = ","), paste0(ncdf_available_vars$name, collapse = ","))
  all(varnam %in% ncdf_available_vars$name) || stop(err_msg_var)

  # get data
  if (is.na(ilon)){
    # get all data i.e. do not subset ncdf
  } else {
    # subset data to longitudinal band `ilon`
    # deal with dynamic longitude dimension name
    if (lonnam == "lon"){
      ncdf <- tidync::hyper_filter(ncdf, lon       = dplyr::near(index, ilon))
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
    # hardcode colnames: lon and lat as longitude and latitude, and datetime
    # dplyr::rename(lon = !!lonnam, lat = !!latnam) |>
    dplyr::rename(dplyr::any_of(stats::na.omit(c(
      lon      = lonnam,  # with any_of columns might or might not exist
      lat      = latnam,  # with na.omit, NA in timenam is gracefully dealt with
      datetime = timenam)))) |>
    # transform to numeric
    dplyr::mutate(lon = as.numeric(lon), lat = as.numeric(lat))
  # NOTE: df based on tidync (< v.0.3.0.9002) contains integer datetime column
  # NOTE: df based on tidync (>= v.0.3.0.9002) contains string datetime column

  # Overwrite parsed time if fgetdate provided
  if (!is.na(fgetdate)){
    # if fgetdate provided, use this function to derive time(s) from nc file name
    if (!is.na(timenam)){
      df <- df |>
        dplyr::arrange(datetime) |> # ensure properly ordered
        dplyr::group_by(lon, lat) |> dplyr::mutate(datetime = fgetdate(filnam)) |> dplyr::ungroup()
    } else {
      warning("Ignored argument 'fgetdate', since no argument 'timenam' provided.")
    }
  }

  # IF NEEDED (i.e. only with tidync version prior to 0.3.0.9002)
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
        if (length(calendar)==0 || is.null(calendar) || is.na(calendar) ||
            units == "" ||
            length(units)==0    || is.null(units)    || is.na(units)) {
          stop(sprintf(paste0("The following file appears not to have valid timestamps, ",
                              "please specify the times using argument 'fgetdate':\n",
                              "%s"),
                       filnam))
        }
        ncdf4::nc_close(nc)

        df <- df |>
          dplyr::mutate(
            datetime =
              CFtime::as_timestamp(CFtime::CFtime(definition = units,
                                                  calendar = calendar,
                                                  offsets = datetime)))
      }
    }
  }

  return(df |> tidyr::nest(data = !c(lon, lat)))
}
