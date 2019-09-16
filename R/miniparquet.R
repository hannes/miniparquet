read <- function(f) {
	res <- .Call(miniparquet_read, f)
	 attr(res, "row.names") <-
              c(NA_integer_, as.integer(-1 * length(res[[1]])))
            class(res) <- "data.frame"
            res
}
