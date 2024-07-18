#' Extracts full time series
#'
#' Extracts full time series from a list of NetCDF files, provided for time
#' steps (can be one time step or multiple time steps) separately and optionally
#'  writes.rds files for each longitude index instead of returning the whole
#'  data.
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
#' @param do_chunks A logical specifying whether chunks of data should be
#' written to files. Defaults to \code{FALSE}. If set to \code{TRUE}, the
#' arguments \code{outdir} and \code{fileprefix} must be specified. Chunks are
#' longitudinal bands and the number of chunks corresponds to the number length
#' of the longitude dimension.
#' @param outdir A character string specifying output directory where data
#' frames are written using the \code{save} statement. If omitted (defaults to
#' \code{NA}), a tidy data frame containing all data is returned.
#' @param fileprefix A character string specifying the file name prefix.
#' @param ncores Number of cores for parallel execution (distributing
#' extraction of longitude slices). When set to \code{"all"}, the number of
#' cores for parallelisation is determined by \code{parallel::detectCores()}.
#' Defaults to \code{1} (no parallelisation).
#' @param single_basedate A logical specifying whether all files in the file
#' list have the same
#' base date (e.g., time units given in 'days since <basedate>').
#' @param fgetdate A function to derive the date used for the time dimension
#' based on the file name.
#' @param overwrite A logical indicating whether time series files are to be
#' overwritten.
#'
#' @importFrom utils data
#' @importFrom stats time
#'
#' @return Nothing. Writes data to .rds files for each longitude index.
#' @export
#'
map2tidy <- function(
  nclist,
  varnam,
  lonnam = "lon",
  latnam = "lat",
  timenam = NA,
  timedimnam = NA,
  noleap = FALSE,
  res_time = "day",
  do_chunks = FALSE,
  outdir = NA,
  fileprefix = NA,
  ncores = 1,
  single_basedate = FALSE,
  fgetdate = NA,
  overwrite = FALSE
  ){

  # check plausibility of argument combination
  if (ncores > 1 && !do_chunks){
    warning("Warning: using multiple cores (ncores > 1) only takes effect when
            do_chunks is TRUE.")
  }

  # Determine longitude indices for chunks
  if (do_chunks){

    # check if necessary arguments are provided
    if (identical(NA, outdir) || identical(NA, fileprefix)){
      stop("Error: arguments outdir and fileprefix must be specified when do_chunks is TRUE.")
    }

    # open one file to get longitude information: length of longitude dimension
    nlon <- ncmeta::nc_dim(nclist[1], lonnam) |>
      dplyr::pull(length)
  } else {
    nlon <- 1
  }
  ilon <- seq(nlon)

  # determine if netcdf file has a calendar with or without leap years
  if (identical(noleap, NA)){
    calendar <- ncmeta::nc_atts(nclist[1], timenam) |>
      dplyr::filter(name == "calendar") |>
      dplyr::pull(value)
    if (calendar %in% c("noleap", "no_leap")){
      noleap <- TRUE
    } else {
      noleap <- FALSE
    }
  }

  if (single_basedate && is.na(fgetdate) && !is.na(timenam)){
    # get base date (to interpret time units in 'days since X')
    basedate <- ncmeta::nc_atts(nclist[1], timenam) |>
      dplyr::filter(name != "_FillValue") |>
      tidyr::unnest(cols = c("value")) |>
      dplyr::filter(name == "units") |>
      dplyr::pull("value") |>
      stringr::str_remove("days since ") |>
      stringr::str_remove(" 00:00:00") |>
      stringr::str_remove(" 0:0:0") |>
      lubridate::ymd()
  } else {
    basedate <- NA
  }

  if (ncores=="all"){
    ncores <- parallel::detectCores()
  }

  # collect time series per longitude slice and create separate files per longitude slice.
  # This step can be parallelized (dependecies: tidync, dplyr, tidyr, purrr, magrittr)
  message(paste0("Create tidy dataframes for following NetCDF map files:\n    ",
                 paste0(nclist, collapse = ",\n    "),
                 "\nand extract (for ", ifelse(length(ilon) == 1,
                                              "1 spatial chunk",
                                              sprintf("spatial chunks %d to %d", min(ilon), max(ilon))),
                 ") variable(s): ", paste0(varnam, collapse = ","), "."))

  if (ncores > 1 && length(ilon) > 1){

    # chunking by longitude and sending to cluster for parallelisation
    cl <- multidplyr::new_cluster(ncores) |>
      multidplyr::cluster_library(c("dplyr", "purrr", "tidyr", "tidync", "dplyr", "magrittr")) |>
      multidplyr::cluster_assign(
        nclist              = nclist,
        outdir              = outdir,
        fileprefix          = fileprefix,
        varnam              = varnam,
        lonnam              = lonnam,
        latnam              = latnam,
        basedate            = basedate,
        timenam             = timenam,
        timedimnam          = timedimnam,
        noleap              = noleap,
        res_time            = res_time,
        fgetdate            = fgetdate,
        overwrite           = overwrite,
        nclist_to_df_byilon = nclist_to_df_byilon,
        nclist_to_df_byfil  = nclist_to_df_byfil)

    # distribute to cores, making sure all data from a specific site is sent to the same core
    out <- dplyr::tibble(ilon = ilon) |>
      multidplyr::partition(cl) |>
      dplyr::mutate(
        out = purrr::map_int(
          ilon,
          ~map2tidy::nclist_to_df_byilon(
            nclist,
            .,
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
            )
          )
        )

  } else if (do_chunks) {

    # chunking by longitude
    out <- purrr::map(
      as.list(ilon),
      ~map2tidy::nclist_to_df_byilon(
        nclist,
        .,
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
        ))

  } else {

    # no chunking. read entire files.
    out <- purrr::map(
      as.list(nclist),
      ~map2tidy::nclist_to_df_byfil(
        .,
        ilon = NA,
        basedate,
        varnam,
        lonnam,
        latnam,
        timenam,
        timedimnam,
        noleap,
        res_time,
        fgetdate))

    if (!is.na(outdir)){
      if (!dir.exists(outdir)){system(paste0("mkdir -p ", outdir))}
      outpath <- paste0(outdir, fileprefix, ".rds")
      if (!file.exists(outpath) || overwrite){
        message(paste("Writing file", outpath, "..."))
        readr::write_rds(out, file = outpath)
        rm("out")
      } else {
        message(paste("File exists already:", outpath))
      }
    }
  }

  if (is.na(outdir)){
    return(dplyr::bind_rows(out))
  } else {
    return(NULL)
  }
}
