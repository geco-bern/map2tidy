#' To back fill
#'
#' @param nclist A vector of character strings specifying the complete paths to
#' files.
#' @param varnam The variable name(s) for which data is to be read from NetCDF
#' files.
#' @param lonnam The dimension name of longitude in the NetCDF files.
#' @param latnam The dimension name of latitude in the NetCDF files.
#' @param timenam The name of dimension variable used for timein the NetCDF
#' files. Defaults to \code{"time"}.
#' @param timedimnam The name of the dimension (axis) used for time.
#' Defaults to \code{"time"}.
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
    fgetdate,
    overwrite
){

  # CRAN HACK, use .data$ syntax for correct fix
  lat <- lon <- value <- NULL

  if (!is.na(outdir)){
    # check whether output has been created already (otherwise do nothing)
    if (!dir.exists(outdir)){system(paste0("mkdir -p ", outdir))}
    outpath <- paste0(outdir, fileprefix, "_ilon_", ilon, ".rds")
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
                          fgetdate
      )
    )

    # check if any element has zero rows and drop that element
    drop_zerorows <- function(y) { return(y[!sapply(y,
                                                    function(x) nrow(x)==0 )]) }
    df <- df %>%
      drop_zerorows()

    if (length(df)>0){
      df <- df %>%
        dplyr::bind_rows() %>%
        dplyr::group_by(lon, lat) %>%
        tidyr::nest() %>%
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


#' To back fill
#'
#' @param filnam file name
#' @param varnam The variable name(s) for which data is to be read from NetCDF
#' files.
#' @param lonnam The dimension name of longitude in the NetCDF files.
#' @param latnam The dimension name of latitude in the NetCDF files.
#' @param timenam The name of dimension variable used for timein the NetCDF
#' files. Defaults to \code{"time"}.
#' @param timedimnam The name of the dimension (axis) used for time.
#' Defaults to \code{"time"}.
#' @param ilon An integer specifying an individual longitude index for chunking
#' all processing and writing chunks to separate output files. If provided,
#' it overrides that the function extracts data for all longitude indices. If
#' omitted (\code{ilon = NA}), the function returns tidy data for all longitude
#' indexes.
#' @param basedate not sure
#' @param fgetdate A function to derive the date used for the time dimension
#' based on the file name.
#'
#' @return not sure
#' @export

nclist_to_df_byfil <- function(
    filnam,
    ilon,
    basedate,
    varnam,
    lonnam,
    latnam,
    timenam,
    timedimnam,
    fgetdate
){

  # CRAN HACK, use .data$ syntax for correct fix
  index <- lat <- lon <- name <- value <- NULL

  if (is.na(basedate) && is.na(fgetdate)){
    # get base date (to interpret time units in 'days since X')
    basedate <- ncmeta::nc_atts(filnam, timenam) %>%
      tidyr::unnest(cols = c(value)) %>%
      dplyr::filter(name == "units") %>%
      dplyr::pull(value) %>%
      stringr::str_remove("days since ") %>%
      stringr::str_remove(" 00:00:00") %>%
      stringr::str_remove(" 0:0:0") %>%
      lubridate::ymd()
  }

  df <- tidync::tidync(filnam) %>%
    tidync::hyper_filter(lon = dplyr::near(index, ilon)) %>%
    tidync::hyper_tibble(tidyselect::vars_pull(varnam))

  if (nrow(df)>0){

    if (!is.na(fgetdate)){
      df <- df %>%
        dplyr::rename(lon = !!lonnam, lat = !!latnam) %>%
        dplyr::mutate(time = fgetdate(filnam))

    } else {
      df <- df %>%
        dplyr::rename(time = !!timedimnam, lon = !!lonnam, lat = !!latnam) %>%
        dplyr::mutate(time = basedate + lubridate::days(time)) #  - lubridate::days(1)

    }

  }

  return(df)
}
