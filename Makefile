CXX=g++
RM=rm -rf
CPPFLAGS=-O0 -g -Ithrift -I. -std=c++11 -fPIC
LDFLAGS=-O0 -g

SOEXT=so
UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S),Darwin)
	SOEXT = dylib
endif


OBJS=parquet/parquet_constants.o parquet/parquet_types.o thrift/protocol/TProtocol.o thrift/TOutput.o thrift/transport/TTransportException.o thrift/transport/TBufferTransports.o snappy/snappy.o snappy/snappy-sinksource.o miniparquet.o

all: libminiparquet.$(SOEXT) pq2csv

libminiparquet.$(SOEXT): $(OBJS)
	$(CXX) $(LDFLAGS) -shared -o libminiparquet.$(SOEXT) $(OBJS) 


pq2csv: libminiparquet.$(SOEXT) pq2csv.o
	$(CXX) $(LDFLAGS) -o pq2csv $(OBJS) pq2csv.o 

clean:
	$(RM) $(OBJS) pq2csv libminiparquet.$(SOEXT) *.dSYM

test: pq2csv
	./test.sh
