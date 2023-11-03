#' Returns indices for a chunk
#'
#' This splits a set of indices into evenly sized chunks and returns the indices
#' for a given chunk number
#'
#' @param ichunk The chunk index, an integer
#' @param nchunk The total number of chunks, an integer
#' @param nlon The total number of indices (e.g., length of dimension) that are
#' to be chunked
#'
#' @return A vector of indices for a given chunk
#' @export
#'
get_index_by_chunk <- function(ichunk, nchunk, nlon){
  nrows_chunk <- ceiling(nlon/nchunk)
  vec_ilon <- seq(1:nlon)
  irow_chunk <- split(vec_ilon, ceiling(seq_along(vec_ilon)/nrows_chunk))
  return(irow_chunk[[ichunk]])
}
