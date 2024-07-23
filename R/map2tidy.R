#' Extracts full time series
#'
#' Extracts full time series from a list of NetCDF files, provided for time
#' steps (can be one time step or multiple time steps) separately and optionally
#'  writes.rds files for each longitude index instead of returning the whole
#'  data.
#'
#' @param nclist A vector of character strings specifying the complete paths to
#' files.
#' @param varnames The variable name(s) for which data is to be read from the
#' NetCDF files.
#' @param lonnam The dimension name of longitude in the NetCDF files.
#' @param latnam The dimension name of latitude in the NetCDF files.
#' @param timenam The name of dimension variable used for time in the NetCDF
#' files. Defaults to \code{NA}.
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
#' @param fgetdate A function to derive the date used for the time dimension
#' based on the file name.
#' @param overwrite A logical indicating whether time series files are to be
#' overwritten.
#'
#' @importFrom utils data
#' @importFrom stats time
#'
#' @return Generates a tibble (containing columns 'lon' (double), 'lat' (double),
#'         and nested column 'data'). Column 'data' contains requested variables
#'         (probably as doubles) and potentially a column 'datetime' (as string). Note that
#'         the datetime is defined by package CFtime and can contain dates such
#'         as "2021-02-30", which are valid for 360-day calendars but not for
#'         POSIXt. Because of that these dates need to be parsed separately.
#'
#'         The function either returns this tibble or
#'         then (if out_dir is specified) returns nothing and writes the tibble
#'         to .rds files for each longitude value.
#' @export
#'

map2tidy <- function(
  nclist,
  varnames,
  lonnam     = "lon",
  latnam     = "lat",
  timenam    = NA,
  do_chunks  = FALSE,
  outdir     = NA,
  fileprefix = NA,
  ncores     = 1,
  fgetdate   = NA,
  overwrite  = FALSE
  ){

  # check plausibility of argument combination
  if ((ncores > 1 || ncores=="all") && !do_chunks){
    warning("Warning: using multiple cores (ncores > 1) only takes effect when
            do_chunks is TRUE.")
  }

  # Determine longitude indices for chunks
  # open one file to get longitude information: length of longitude dimension
  # tidync::tidync(nclist[1])
  meta_dims <- tidync::hyper_dims(tidync::tidync(nclist[1]))
  nlon <- meta_dims |>
    dplyr::filter(name == lonnam) |>
    dplyr::pull(length)
  if (do_chunks){
    # check if necessary arguments are provided
    if (identical(NA, outdir) || identical(NA, fileprefix)){
      stop("Error: arguments outdir and fileprefix must be specified when do_chunks is TRUE.")
    }
  }
  ilon_arg <- if (do_chunks){
    seq(nlon) # chunking by longitude over multiple cores or single cors
  } else {
    NA        # no chunking. read entire files.
  }


  if (ncores=="all"){
    ncores <- parallel::detectCores() - 1
  }
  ncores <- min(ncores, length(ilon_arg))

  # Message out
  message(paste0(
    "Create tidy dataframes for following NetCDF map files:\n    ",
    paste0(nclist, collapse = ",\n    "),
    "\nand extract variable(s): ", paste0(varnames, collapse = ","),
    "; (in ",
    ifelse(is.na(ilon_arg),
           "1 spatial chunk",
           sprintf("spatial chunks %d to %d", min(ilon_arg), max(ilon_arg))),
    ifelse(ncores>1,
           sprintf(", distributed over %d workers", ncores),
           ""),
    ")."))

  # if (ncores > 1 && length(ilon_arg) > 1){
  # } else if (do_chunks) {
  #   message("Writing output file to", paste0(outdir, "/", fileprefix, ".rds"), "...")
  #   if (!is.na(outdir)){
  #     # check whether output has been created already (otherwise do nothing)
  #     if (!dir.exists(outdir)){system(paste0("mkdir -p ", outdir))}
  #     outpath <- paste0(outdir, "/", fileprefix, "_ilon_", ilon, ".rds")
  #   } else {
  #     outpath <- dirname(nclist[1])
  #   }
  #   message(paste("Writing output file(s) to ", outpath, "..."))
  # } else {
  #   if (!is.na(outdir)){
  #     message("Writing output file to ", paste0(outdir, "/", fileprefix, ".rds"), "...")
  #   }
  # }


  # collect time series per longitude slice and create separate files per longitude slice.

  # Setup cluster if requested, otherwise use no-effect-placeholder-function
  if (ncores > 1){ # chunking by longitude over multiple cores
    # chunking by longitude and sending to cluster for parallelisation
    cl <- multidplyr::new_cluster(ncores) |>
      multidplyr::cluster_library(c("dplyr", "purrr", "tidyr", "tidync")) |>
      multidplyr::cluster_assign(
        nclist              = nclist,
        outdir              = outdir,
        fileprefix          = fileprefix,
        varnames            = varnames,
        lonnam              = lonnam,
        latnam              = latnam,
        timenam             = timenam,
        fgetdate            = fgetdate,
        overwrite           = overwrite,
        nclist_to_df_byilon = nclist_to_df_byilon,
        ncfile_to_df        = ncfile_to_df)

    # distribute to cores, making sure all data from a specific site is sent to the same core
    parition_if_requested <- function(x, cl) {multidplyr::partition(x, cl)}
  } else {
    parition_if_requested <- function(x, cl) {x} # no-effect-placeholder-function
  }

  # Loop over ilon_arg (and within nclist_to_df_byilon loop over nclist)
  out <- dplyr::tibble(lon_index = ilon_arg) |>
    parition_if_requested(cl) |>
    dplyr::mutate(
      out = purrr::map(
        as.list(lon_index),
        ~map2tidy::nclist_to_df_byilon(
          nclist,
          .,
          outdir,
          fileprefix,
          varnames,
          lonnam,
          latnam,
          timenam,
          fgetdate,
          overwrite
        ))
    )

  return(dplyr::collect(out) |> tidyr::unnest(out) |> dplyr::arrange(lon) |>
           dplyr::select(!lon_index))
}
