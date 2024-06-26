% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/map2tidy.R
\name{map2tidy}
\alias{map2tidy}
\title{Extracts full time series}
\usage{
map2tidy(
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
)
}
\arguments{
\item{nclist}{A vector of character strings specifying the complete paths to
files.}

\item{varnam}{The variable name(s) for which data is to be read from NetCDF
files.}

\item{lonnam}{The dimension name of longitude in the NetCDF files.}

\item{latnam}{The dimension name of latitude in the NetCDF files.}

\item{timenam}{The name of dimension variable used for timein the NetCDF
files. Defaults to \code{NA}.}

\item{timedimnam}{The name of the dimension (axis) used for time.
Defaults to \code{NA}.}

\item{noleap}{A logical specifying whether the calendar of the NetCDF time
axis contains leap years. If it doesn't, \code{noleap} is \code{TRUE}.
Defaults to \code{NA} - no prior information specified and dynamically inter-
preted from NetCDF file.}

\item{res_time}{A character specifying the resolution of the time axis.
Available: \code{c("mon", "day")}. Defaults to \code{"day"}.}

\item{do_chunks}{A logical specifying whether chunks of data should be
written to files. Defaults to \code{FALSE}. If set to \code{TRUE}, the
arguments \code{outdir} and \code{fileprefix} must be specified. Chunks are
longitudinal bands and the number of chunks corresponds to the number length
of the longitude dimension.}

\item{outdir}{A character string specifying output directory where data
frames are written using the \code{save} statement. If omitted (defaults to
\code{NA}), a tidy data frame containing all data is returned.}

\item{fileprefix}{A character string specifying the file name prefix.}

\item{ncores}{Number of cores for parallel execution (distributing
extraction of longitude slices). When set to \code{"all"}, the number of
cores for parallelisation is determined by \code{parallel::detectCores()}.
Defaults to \code{1} (no parallelisation).}

\item{single_basedate}{A logical specifying whether all files in the file
list have the same
base date (e.g., time units given in 'days since \if{html}{\out{<basedate>}}').}

\item{fgetdate}{A function to derive the date used for the time dimension
based on the file name.}

\item{overwrite}{A logical indicating whether time series files are to be
overwritten.}
}
\value{
Nothing. Writes data to .rds files for each longitude index.
}
\description{
Extracts full time series from a list of NetCDF files, provided for time
steps (can be one time step or multiple time steps) separately and optionally
writes.rds files for each longitude index instead of returning the whole
data.
}
