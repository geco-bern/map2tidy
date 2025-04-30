# map2tidy v2.1.3

* added argument na.rm to map2tidy()

# map2tidy v2.1.2

* internally use pkg {parallelly} instead of {parallel}

# map2tidy v2.1.1

* activated RDS compression

# map2tidy v2.1

* Changed the naming of the files (prepending 0's to have always 6 digits: `LON_-000.250`)
* Added a feature to subset certain longitudes only (argument `filter_lon_between_degrees`)
* Made error messages more explicit.

# map2tidy v2.0

* Public release after refactored code
* Improved robustness:
    - `map2tidy` can now handle hourly data (using pkg `CFtime`)
    - generally more automatic handling of NetCDF files.
* Streamlined API:
    - function calls require less user input. Since more information is robustly derived from the data itself, the use of various data sets is facilitated.
    - Namely: get rid of input arguments: `timedimnam`, `noleap`, `res_time`
* Improved Verbosity:
    - `map2tidy` now reports what it is doing
    - `map2tidy` now suggests alternatives if user input is wrong, e.g. typos or wrong names of ‘longituded’ etc…
    - [ ] unsolved: when `ncores > 1` verbosity remains low, since `message()` does not get printed from within `multidplyr`
