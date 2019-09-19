parquet_read <- function(file) {
	res <- .Call(miniparquet_read, file)
	# some data.frame dress up
	attr(res, "row.names") <- c(NA_integer_, as.integer(-1 * length(res[[1]])))
	class(res) <- "data.frame"
	res
}


read_parquet <- parquet_read
