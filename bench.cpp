#include <iostream>
#include <fstream>
#include <sys/time.h>


#include "miniparquet.h"

using namespace miniparquet;
using namespace std;

static struct timeval tm1;


static void start()
{
    gettimeofday(&tm1, NULL);
}

static uint64_t stop()
{
    struct timeval tm2;
    gettimeofday(&tm2, NULL);

    unsigned long long t = 1000 * (tm2.tv_sec - tm1.tv_sec) + (tm2.tv_usec - tm1.tv_usec) / 1000;
//    printf("%llu ms\n", t);

    return t;
}


std::ifstream::pos_type filesize(const char* filename)
{
    std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
    return in.tellg();
}

int main(int argc, char * const argv[]) {

	for (int arg = 1; arg < argc; arg++) {
		start();
		auto f = ParquetFile(argv[arg]);

		ResultChunk rc;
		ScanState s;

		f.initialize_result(rc);
		uint64_t nrow = 0;
		while (f.scan(s, rc)) {
			nrow += rc.nrows;
		}
		double time_sec = stop()/1000.0;
		auto filesize_bytes = (uint64_t)filesize(argv[arg]);

		auto mb_s = (filesize_bytes/1000000.0) / time_sec;

		printf("%lld rows %.1f mb/s\n", nrow, mb_s);
	}

}
