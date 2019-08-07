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

		while(f.scan(s, rc)) {
			for (uint64_t row = 0; row < rc.nrows; row++) {
				for (auto& col: rc.cols) {
					switch(col.type) {
					case parquet::format::Type::BOOLEAN:
						printf("%s\t", ((bool*)col.data.get())[row] ? "TRUE" : "FALSE");
						break;

					case parquet::format::Type::INT32:
						printf("%d\t", ((int32_t*)col.data.get())[row]);
						break;
					case parquet::format::Type::INT64:
						printf("%lld\t", ((int64_t*)col.data.get())[row]);
						break;
					case parquet::format::Type::FLOAT:
						printf("%f\t", ((float*)col.data.get())[row]);
						break;
					case parquet::format::Type::DOUBLE:
						printf("%lf\t", ((double*)col.data.get())[row]);
						break;
					case parquet::format::Type::BYTE_ARRAY:
						printf("%s\t", ((char**)col.data.get())[row]);
						break;


					}


				}
				printf("\n");
			}

		}

	}

}
