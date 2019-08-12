CXX=g++
RM=rm -f
CPPFLAGS=-O3 -Ithrift -I. -std=c++11
LDFLAGS=-O3

OBJS=parquet/parquet_constants.o parquet/parquet_types.o thrift/protocol/TProtocol.o thrift/TOutput.o thrift/transport/TTransportException.o thrift/transport/TBufferTransports.o snappy/snappy.o snappy/snappy-sinksource.o miniparquet.o

all: libminiparquet.dylib pq2csv

libminiparquet.dylib: $(OBJS)
	$(CXX) $(LDFLAGS) -shared -o libminiparquet.dylib $(OBJS) 


pq2csv: libminiparquet.dylib pq2csv.o
	$(CXX) $(LDFLAGS) -o pq2csv -L. -lminiparquet pq2csv.o 

clean:
	$(RM) $(OBJS) pq2csv
