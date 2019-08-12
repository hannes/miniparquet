# miniparquet

miniparquet is a reader for a common subset of Parquet files. miniparquet only supports rectangular-shaped data structures (no nested tables) and only the Snappy compression scheme. miniparquet has no (zero, none, 0) [external dependencies](https://research.swtch.com/deps). See `pq2csv.cpp` for a usage example.
