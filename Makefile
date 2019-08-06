# TODO this is uuugly

test_bin: libminiparquet.dylib test2.cpp miniparquet.cpp
	g++ test2.cpp miniparquet.cpp -o test_bin -lminiparquet  -L. -I . -I thrift -std=c++11 -O0 -g -Wall

libminiparquet.dylib:
	g++ -I . -I thrift gen-cpp/parquet_constants.cpp gen-cpp/parquet_types.cpp thrift/protocol/TProtocol.cpp thrift/TOutput.cpp thrift/transport/TTransportException.cpp thrift/transport/TBufferTransports.cpp snappy/snappy.cc snappy/snappy-sinksource.cc -o libminiparquet.dylib -shared  -O3 -std=c++11
