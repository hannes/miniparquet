#!/bin/zsh
make

for F in test/nasa-uncompressed/part-00000-*.parquet  test/nasa-snappy/part-00000-*.snappy.parquet test/parquet-testing-data2/*.parquet 
do
	echo $F
	./pq2csv $F > miniparquet.tsv
	./dump.py $F > arrow.tsv

	./roundingdiff.py miniparquet.tsv arrow.tsv
done



