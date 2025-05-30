# list demo file path
path <- file.path(system.file(package = "map2tidy"),"extdata")

# list demo files
files <- list.files(path, pattern = "demo_data_2017_month.*.nc", full.names = TRUE)

#---- test functions ----
test_that("test map2tidy", {

  # load and convert
  df1 <- map2tidy(
    nclist = files,
    varnam = "et",
    do_chunks = FALSE,
    lonnam = "lon",
    latnam = "lat",
    timenam = "time",
  )

  # check status
  #df1:
  testthat::expect_type(df1, "list")
  testthat::expect_equal(df1$data[[1]][c(1,2),],
                         tidyr::tibble(et       = c(0.402741432189941, 0.408226490020752),
                                       datetime = c("2017-01-01", "2017-01-02")),
                         tolerance = 0.00001) # ensure data.frame is nested
  testthat::expect_equal(names(df1),
                         c("lon", "lat", "data"))
  testthat::expect_equal(names(tidyr::unnest(df1, data)),
                         c("lon", "lat", "et", "datetime"))
  testthat::expect_equal(nrow(df1),
                         900)

  tmpdir <- tempdir()

  # writing files
  res2 <- map2tidy(
    nclist = files,
    varnam = "et",
    lonnam = "lon",
    latnam = "lat",
    timenam = "time",
    do_chunks = FALSE,
    outdir = tmpdir,
    fileprefix = "demo_data_2017",
    ncores = 1,
    overwrite = TRUE
  )

  # writing files, not parallel
  res2b <- map2tidy(
    nclist = files,
    varnam = "et",
    lonnam = "lon",
    latnam = "lat",
    timenam = "time",
    # na.rm = TRUE, # default anyway
    do_chunks = TRUE,
    outdir = tmpdir,
    fileprefix = "demo_data_2017",
    ncores = 1,
    overwrite = TRUE
  )

  # writing files, parallel
  res3 <- map2tidy( # do overwrite
    nclist = files,
    varnam = "et",
    lonnam = "lon",
    latnam = "lat",
    timenam = "time",
    # na.rm = TRUE, # default anyway
    do_chunks = TRUE,
    outdir = tmpdir,
    fileprefix = "demo_data_2017",
    ncores = 6,
    overwrite = TRUE
  )

  # test whether overwriting is avoided
  testthat::expect_message(
    res2c <- map2tidy(  # do not overwrite
      nclist = files,
      varnam = "et",
      lonnam = "lon",
      latnam = "lat",
      timenam = "time",
      # na.rm = TRUE, # default anyway
      do_chunks = TRUE,
      outdir = tmpdir,
      fileprefix = "demo_data_2017",
      ncores = 1,
      overwrite = FALSE
    ),
    regexp = "File exists already:"
  )

  # Load some example files:
  df2  <- readRDS(file.path(tmpdir, "demo_data_2017_LON_-000.025_to_+001.425.rds")) # do_chunks was FALSE
  df2b3 <- readRDS(file.path(tmpdir, "demo_data_2017_LON_-000.025.rds"))            # do_chunks was TRUE


  # check status
  #res2, df2:
  testthat::expect_identical(df1, df2) # Check that return value is same as RDS value
  testthat::expect_length(unique(res2$data), 1)
  testthat::expect_true(grepl("Written data by worker with jobid: [0-9]* into file", unique(res2$data)))
  testthat::expect_equal(names(tidyr::unnest(df2, data)), c("lon", "lat", "et", "datetime"))
  testthat::expect_equal(df2$data[[1]][c(1,2),],
                         tidyr::tibble(et=c(0.403, 0.408), datetime=c("2017-01-01", "2017-01-02")),
                         tolerance = 0.001)             # ensure data.frame is nested
  testthat::expect_equal(nrow(df2), 900)                # ensure data.frame is nested (900 coords in 30x30 lon x lat)
  testthat::expect_equal(nrow(df2$data[[1]]), 365)      # ensure data.frame is nested (365 days per coord)
  #res2b, df2b3:
  testthat::expect_length(unique(res2b$data), 30)
  testthat::expect_length(unique(gsub("into file: .*","",res2b$data)), 1) # check that all files are writing by same CPU core
  testthat::expect_equal(names(tidyr::unnest(df2b3, data)), c("lon", "lat", "et", "datetime"))
  testthat::expect_equal(df2b3$data[[1]][c(1,2),],
                         tidyr::tibble(et=c(0.403, 0.408), datetime=c("2017-01-01", "2017-01-02")),
                         tolerance = 0.001)              # ensure data.frame is nested
  testthat::expect_equal(nrow(df2b3$data[[1]]), 365)      # ensure data.frame is nested (365 days per coord)
  testthat::expect_equal(nrow(df2b3), 30)                 # ensure data.frame is nested (30  coords in 1x30 lon x lat)
  testthat::expect_length(unique(df2b3$lat), 30)          # ensure data.frame is nested (30  coords in 1x30 lon x lat)
  testthat::expect_equal(unique(df2b3$lon), -0.025,       # ensure data.frame is nested (30  coords in 1x30 lon x lat)
                         tolerance = 0.000000001)
  #res3:
  testthat::expect_length(unique(gsub("into file: .*","",res3$data)), 6) # check that all files are writing by 3 different CPU cores
  #res2c:
  testthat::expect_true(all(grepl("File exists already",res2c$data)))

  # TODO: files have unfortunately no NA, thus df4 is even without
  #       the filtering identical to df2b3. TODO: Include a small NetCDF data set
  #       with NA for testing purposes.
  res4 <- map2tidy( # keep NA, by setting (na.rm = FALSE)
    nclist = files,
    varnam = "et",
    lonnam = "lon",
    latnam = "lat",
    timenam = "time",
    na.rm = FALSE, # change from default=TRUE
    do_chunks = TRUE,
    outdir = tmpdir,
    fileprefix = "demo_data_2017_withNA",
    ncores = 6,
    overwrite = TRUE
  )
  df4 <- readRDS(file.path(tmpdir, "demo_data_2017_withNA_LON_-000.025.rds"))
  testthat::expect_equal(
    df4 |> unnest(data) |> filter(!is.na(et)) |> nest(data = c(et, datetime)),
    df2b3)
})
