# list demo file path
path <- file.path(system.file(package = "map2tidy"),"extdata")

# list demo files
files <- list.files(path, pattern = "demo_data_2017_month", full.names = TRUE)

#---- test functions ----

test_that("test map2tidy", {

  # load and convert
  df <- map2tidy(
    nclist = files,
    varnam = "et",
    lonnam = "lon",
    latnam = "lat",
    timenam = "time",
    timedimnam = "time"
  )

  # check status
  expect_type(df, "list")

})
