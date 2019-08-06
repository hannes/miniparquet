#pragma once

#include <string>
#include <vector>
#include <bitset>
#include <fstream>
#include "gen-cpp/parquet_types.h"

namespace miniparquet {

class ParquetColumn {
public:
	uint64_t id;
	parquet::format::Type::type type;
	std::string name;
	parquet::format::SchemaElement* schema_element;
};

struct Int96 {
	uint32_t value[3];
};


template<class T>
class Dictionary {
public:
	std::vector<T> dict;
	Dictionary(uint64_t n_values) {
		dict.resize(n_values);
	}
	T& get(uint64_t offset) {
		if (offset >= dict.size()) {
			throw std::runtime_error("Dictionary offset out of bounds");
		} else
			return dict.at(offset);
	}
};

class ScanState {
public:
	uint64_t row_group_idx = 0;
	uint64_t row_group_offset = 0;

	std::vector<std::unique_ptr<char[]>> string_heaps;



//	// potentially legacy
//	parquet::format::RowGroup* row_group = nullptr;
//	parquet::format::ColumnChunk* chunk = nullptr;
//	std::unique_ptr<parquet::format::PageHeader> page = nullptr;
//
//	char* payload_ptr = nullptr;
//	char* payload_end_ptr = nullptr;
//	uint64_t payload_len;
//	bool seen_dict;
//
//	uint64_t row_base = 0;
//	uint64_t row_group_nrow = 0;
//	uint64_t row_group_nread = 0;
//
//	uint64_t page_base = 0;
//	uint64_t column_idx = 0;
//	std::unique_ptr<uint32_t[]> definition_levels;
//
//	void* dict = nullptr; // this is a Dictionary

	// TODO move these to scan state to allow parallelized reading
	char* read_buf = nullptr;
	std::unique_ptr<char[]> read_buf_holder = nullptr;
	uint64_t read_buf_size = 0;


	void resize_buf(uint64_t new_size);
};


//constexpr uint64_t MAX_RESULT_COL_LEN = 1024;

struct ResultColumn {
	uint64_t id;
	std::unique_ptr<char[]> data;
	parquet::format::Type::type type;
	std::vector<bool> defined;
};

struct ResultChunk {
	std::vector<ResultColumn> cols;
	uint64_t nrows;
};



class ParquetFile {
public:
	ParquetFile(std::string filename);

	void initialize_result(ResultChunk& result);
	void initialize_column(ResultColumn& col, uint64_t num_rows);
	bool scan(ScanState &s, ResultChunk& result);

	uint64_t nrow;
	std::vector<std::unique_ptr<ParquetColumn>> columns;


private:
	void initialize(std::string filename);
	parquet::format::FileMetaData file_meta_data;
	std::ifstream pfile;

	void scan_column(ScanState& state, ResultColumn& result_col);

};

}
