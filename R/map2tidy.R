#' Extracts full time series
#'
#' Extracts full time series from a list of NetCDF files, provided for time
#' steps (can be one time step or multiple time steps) separately and optionally
#'  writes.rds files for each longitude index instead of returning the whole
#'  data.
#'
#' @param nclist A vector of character strings specifying the complete paths to
#' files.
#' @param varnam The variable name(s) for which data is to be read from the
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
#' @param filter_lon_between_degrees Either NA (default) or a vector of two
#' numbers c(lower, upper) that define a range of longitude values to process,
#' e.g. c(-70, -68).
#'
#' @importFrom rlang .data
#' @importFrom utils capture.output
#'
#' @return Generates a tibble (containing columns 'lon' (double), 'lat' (double),
#'         and nested column 'data'). Column 'data' contains requested variables
#'         (probably as doubles) and potentially a column 'datetime' (as string). Note that
#'         the datetime is defined by package CFtime and can contain dates such
#'         as "2021-02-30", which are valid for 360-day calendars but not for
#'         POSIXt. Because of that these dates need to be parsed separately.
#'         The function either returns this tibble or
#'         then (if out_dir is specified) returns nothing and writes the tibble
#'         to .rds files for each longitude value.
#' @export
#'

map2tidy <- function(
  nclist,
  varnam,
  lonnam     = "lon",
  latnam     = "lat",
  timenam    = NA,
  do_chunks  = FALSE,
  outdir     = NA,
  fileprefix = NA,
  ncores     = 1,
  fgetdate   = NA,
  overwrite  = FALSE,
  filter_lon_between_degrees = NA # or c(-70, -68)
  ){

  # R CMD Check HACK, use .data$ syntax (or {{...}}) for correct fix https://stackoverflow.com/a/63877974
  index <- lat <- lon <- name <- value <- out <- datetime <- lon_index <- lon_value <- data <- time <- NULL

  stopifnot(length(nclist) >= 1)

  # check plausibility of argument combination
  if ((ncores > 1 || ncores=="all") && !do_chunks){
    warning("Warning: using multiple cores (ncores > 1) only takes effect when
            do_chunks is TRUE.")
  }
  if (do_chunks){
    # check if necessary arguments are provided
    if (is.na(outdir) || is.na(fileprefix)){
      stop("Error: arguments outdir and fileprefix must be specified when do_chunks is TRUE.")
    }
  }
  stopifnot(!is.numeric(filter_lon_between_degrees) || diff(filter_lon_between_degrees) > 0) # either NA or then c(lower, upper), not c(upper, lower)




  # Determine longitude indices for chunks
  # open one file to get longitude information: length of longitude dimension
  df_indices <- get_longitude_value_indices(nclist[1], lonnam)
  # meta_dims <- tidync::hyper_dims(tidync::tidync(nclist[1]))
  ilon_arg <- if (do_chunks){
    df_indices # chunking by longitude into different files
  } else {
    dplyr::tibble(lon_value="all", lon_index = NA_integer_) # no chunking. store into single file
  }


  # subset only certain, requested longitude values:
  if (is.numeric(filter_lon_between_degrees)){
    if (do_chunks) {
      ilon_arg <- dplyr::filter(ilon_arg,
                                dplyr::between(lon_value,
                                               filter_lon_between_degrees[1],
                                               filter_lon_between_degrees[2]))
    } else {
      stop("Invalid input: if only certain longitude values should be processed (filter_lon_between_degrees), the argument `do_chunks` must be TRUE.")
      # stop("Invalid input: you can't request to create a single file (do_chunks==FALSE) and also requeste to filter certain longitudes (filter_lon_between_degrees)")
    }
  }
  stopifnot(nrow(ilon_arg) > 0) # check that applying filter_lon_between_degrees still leaves some values to process



  if (ncores=="all"){
    ncores <- parallel::detectCores() - 1
  }
  ncores <- min(ncores, nrow(ilon_arg)) # No need to have more cores if we only produce 1 file

  # If needed: create out folder and check access
  if (!is.na(outdir)){
    if (!dir.exists(outdir)){system(paste0("mkdir -p ", outdir))}
    file.access(dirname(outdir), mode = 2)[[1]] == 0 ||
      stop(sprintf("Path (%s) is not writable. Do you need to 'sudo chmod'?", outdir))
  }

  # Message out
  msg1 <- paste0("START ================ ", format(Sys.time(), "%b %d, %Y, %X"))
  message(msg1)
  msg2 <- paste0(
    "Extract variable(s): ", paste0(varnam, collapse = ","), ",\n",
    "(in ",
    ifelse(dplyr::first(ilon_arg$lon_value == "all"),
           "1 spatial chunk",
           sprintf("%d spatial chunks from %.3f to %.3f degrees in %.3f degree intervals",
                   nrow(ilon_arg),
                   min(ilon_arg$lon_value),
                   max(ilon_arg$lon_value),
                   diff(ilon_arg$lon_value[1:2]))),
    ifelse(ncores>1,
           sprintf(", distributed over %d workers (i.e CPU cores)", ncores),
           ""),
    "),\n",
    sprintf("from the following %d NetCDF map file(s):\n    ", length(nclist)),
    paste0(c(utils::head(nclist), "...", utils::tail(nclist)), collapse = ",\n    "),
    ".\n"
  )
  message(msg2)
  msg3 <- tidync::tidync(nclist[1]) |> utils::capture.output()
  msg3 <- gsub("^[ ]*$","", msg3) # Remove strings containing only whitespaces
  msg3 <- msg3[nzchar(msg3)]       # Remove empty strings
  message(paste0(msg3, collapse = "\n")) # showing extent of first NetCDF file


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
        varnam            = varnam,
        lonnam              = lonnam,
        latnam              = latnam,
        timenam             = timenam,
        fgetdate            = fgetdate,
        overwrite           = overwrite,
        nclist_to_df_byilon = nclist_to_df_byilon,
        ncfile_to_df        = ncfile_to_df)

    # distribute to cores, making sure all data from a specific site is sent to the same core
    partition_if_requested <- function(x, cl) {multidplyr::partition(x, cl)}
  } else {
    cl <- NULL
    partition_if_requested <- function(x, cl) {x} # no-effect-placeholder-function
  }

  # Loop over ilon_arg (and within nclist_to_df_byilon loop over nclist)
  res <- ilon_arg |>
    partition_if_requested(cl) |>
    dplyr::mutate(
      out = purrr::map(
        as.list(lon_index),
        ~map2tidy::nclist_to_df_byilon(
          nclist,
          .,
          outdir,
          fileprefix,
          varnam,
          lonnam,
          latnam,
          timenam,
          fgetdate,
          overwrite
        ),
        .progress=TRUE)
    )

  # Message
  message(paste0("DONE ================ ", format(Sys.time(), "%b %d, %Y, %X")))
  # Warning if unusual
  if (!is.na(outdir)){
    unusual_outputs <- dplyr::collect(res) |> tidyr::unnest(out) |>
      dplyr::filter(!grepl("^Written data", .data$data)) # select only unusual
    if (nrow(unusual_outputs) > 0){
      message(paste0(c("Some data appeared unusual:",
                       capture.output(unusual_outputs)),#[-1]), # -1 drops A tibble:
                     collapse = "\n"))
    }
  }

  return(dplyr::collect(res) |> tidyr::unnest(out) |> dplyr::arrange(lon) |>
           dplyr::select(!c(lon_index, lon_value)))
  # return(dplyr::collect(res) |> tidyr::unnest(out))
}
