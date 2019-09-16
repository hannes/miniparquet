# miniparquet
[![Travis](https://api.travis-ci.org/hannesmuehleisen/miniparquet.svg?branch=master)](https://travis-ci.org/hannesmuehleisen/miniparquet)

`miniparquet` is a reader for a common subset of Parquet files. miniparquet only supports rectangular-shaped data structures (no nested tables) and only the Snappy compression scheme. miniparquet has no (zero, none, 0) [external dependencies](https://research.swtch.com/deps). 

Miniparquet comes as both a C++ library and a R package. Install the R package like so: `devtools::install_github("hannesmuehleisen/miniparquet")`

Use the R package like so: `df <- miniparquet::parquet_read("example.parquet")` 

Folders of similar-structured Parquet files (e.g. produced by Spark) can be read like this: 

`df <- data.table::rbindlist(lapply(Sys.glob("some-folder/part-*.parquet"), miniparquet::parquet_read))`

If you find a file that should be supported but isn't, please open an issue here with a link to the file.

