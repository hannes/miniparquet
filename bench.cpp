#include <iostream>

#include "miniparquet.hpp"

using namespace miniparquet;
using namespace std;


int main(int argc, char * const argv[]) {

	for (int arg = 1; arg < argc; arg++) {
		auto f = ParquetFile(argv[arg]);

		ResultChunk rc;
		ScanState s;

		f.initialize_result(rc);
		uint64_t nrow = 0;
		while (f.scan(s, rc)) {
			nrow += rc.nrows;
			

		}
		printf("%lld\n", nrow);				
	}

}
