# TODO this is uuugly

pq2csv: libminiparquet.dylib pq2csv.cpp miniparquet.* Makefile
	g++ -I . -I thrift pq2csv.cpp miniparquet.cpp -o pq2csv -lminiparquet  -L. -std=c++11 -O3 -g -Wall

libminiparquet.dylib:
	g++ -I . -I thrift gen-cpp/parquet_constants.cpp gen-cpp/parquet_types.cpp thrift/protocol/TProtocol.cpp thrift/TOutput.cpp thrift/transport/TTransportException.cpp thrift/transport/TBufferTransports.cpp snappy/snappy.cc snappy/snappy-sinksource.cc -o libminiparquet.dylib -shared  -O3 -std=c++11
