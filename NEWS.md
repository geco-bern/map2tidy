# map2tidy v2.0

* Public release after refactored code

New sine previous version:
* Improve robustness:
    - `map2tidy` can now handle hourly data (using pkg `CFtime`)
    - generally more automatic handling of NetCDF files.
* Streamline API:
    - function calls require less user input. Since more information is robustly derived from the data itself, the use of various data sets is facilitated.
    - Namely: get rid of input arguments: `timedimnam`, `noleap`, `res_time`
* Improve Verbosity:
    - `map2tidy` now reports what it is doing
    - `map2tidy` now suggests alternatives if user input is wrong, e.g. typos or wrong names of ‘longituded’ etc…
    - [ ] unsolved: when `ncores > 1` verbosity remains low, since `message()` does not get printed from within `multidplyr`