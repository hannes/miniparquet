CXX=g++
RM=rm -f
CPPFLAGS=-O3 -Ithrift -I. -std=c++11 -fPIC
LDFLAGS=-O3

SOEXT=so
ifeq ($(UNAME_S),Darwin)
	SOEXT = dylib
endif


OBJS=parquet/parquet_constants.o parquet/parquet_types.o thrift/protocol/TProtocol.o thrift/TOutput.o thrift/transport/TTransportException.o thrift/transport/TBufferTransports.o snappy/snappy.o snappy/snappy-sinksource.o miniparquet.o

all: libminiparquet.dylib pq2csv

libminiparquet.$(SOEXT): $(OBJS)
	$(CXX) $(LDFLAGS) -shared -o libminiparquet.$(SOEXT) $(OBJS) 


pq2csv: libminiparquet.$(SOEXT) pq2csv.o
	$(CXX) $(LDFLAGS) -o pq2csv $(OBJS) pq2csv.o 

clean:
	$(RM) $(OBJS) pq2csv
