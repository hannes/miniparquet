#include <iostream>
#include <fstream>
#include <string>
#include <sstream>

#include "snappy/snappy.h"

#include "miniparquet.hpp"

#include <protocol/TCompactProtocol.h>
#include <transport/TBufferTransports.h>

using ThriftBuffer = apache::thrift::transport::TMemoryBuffer;

using namespace std;

using namespace parquet;
using namespace parquet::format;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace miniparquet;

static TCompactProtocolFactoryT<ThriftBuffer> tproto_factory;

template<class T>
static void thrift_unpack(const uint8_t* buf, uint32_t* len,
		T* deserialized_msg) {
	shared_ptr<ThriftBuffer> tmem_transport(
			new ThriftBuffer(const_cast<uint8_t*>(buf), *len));
	shared_ptr<TProtocol> tproto = tproto_factory.getProtocol(tmem_transport);
	try {
		deserialized_msg->read(tproto.get());
	} catch (std::exception& e) {
		std::stringstream ss;
		ss << "Couldn't deserialize thrift: " << e.what() << "\n";
		throw std::runtime_error(ss.str());
	}
	uint32_t bytes_left = tmem_transport->available_read();
	*len = *len - bytes_left;
}

void ScanState::resize_buf(uint64_t new_size) {
	read_buf_holder = unique_ptr<char[]>(new char[new_size]);
	read_buf = read_buf_holder.get();
	read_buf_size = new_size;
}

ParquetFile::ParquetFile(std::string filename) {
	initialize(filename);
}

void ParquetFile::initialize(string filename) {
	ScanState s;

	pfile = ifstream(filename, std::ios::binary);
	s.resize_buf(4);
	// check for magic bytes at start of file
	pfile.read(s.read_buf, 4);
	if (strncmp(s.read_buf, "PAR1", 4) != 0) {
		throw runtime_error("No magic bytes found at beginning of file");
	}

	// check for magic bytes at end of file
	pfile.seekg(-4, ios_base::end);
	pfile.read(s.read_buf, 4);
	if (strncmp(s.read_buf, "PAR1", 4) != 0) {
		throw runtime_error("No magic bytes found at end of file");
	}

	// read four-byte footer length from just before the end magic bytes
	pfile.seekg(-8, ios_base::end);
	pfile.read(s.read_buf, 4);
	int32_t footer_len = *(uint32_t*) s.read_buf;
	if (footer_len == 0) {
		throw runtime_error("Footer length can't be 0");
	}

	// read footer into buffer and de-thrift
	s.resize_buf(footer_len);
	pfile.seekg(-(footer_len + 8), ios_base::end);
	pfile.read(s.read_buf, footer_len);
	if (!pfile) {
		throw runtime_error("Could not read footer");
	}
	thrift_unpack((const uint8_t*) s.read_buf, (uint32_t*) &footer_len,
			&file_meta_data);

	if (file_meta_data.__isset.encryption_algorithm) {
		throw runtime_error("Encrypted Parquet files are not supported");
	}

	// check if we like this schema
	if (file_meta_data.schema.size() < 2) {
		throw runtime_error("Need at least one column in the file");
	}
	if (file_meta_data.schema[0].num_children
			!= file_meta_data.schema.size() - 1) {
		throw runtime_error("Only flat tables are supported (no nesting)");
	}

	// skip the first column its the root and otherwise useless
	for (uint64_t col_idx = 1; col_idx < file_meta_data.schema.size();
			col_idx++) {
		auto s_ele = file_meta_data.schema[col_idx];
		if (!s_ele.__isset.type || s_ele.num_children > 0) {
			throw runtime_error("Only flat tables are supported (no nesting)");
		}
		// TODO if this is REQUIRED, no defined levels in file

		// if field is REPEATED, no bueno
		if (s_ele.repetition_type != FieldRepetitionType::OPTIONAL) {
			throw runtime_error("Only OPTIONAL fields support for now");
		}
		// TODO scale? precision?
		auto col = unique_ptr<ParquetColumn>(new ParquetColumn());
		col->id = col_idx - 1;
		col->name = s_ele.name;
		col->schema_element = &s_ele;
		col->type = s_ele.type;
		columns.push_back(move(col));
	}
	this->nrow = file_meta_data.num_rows;
}

// TODO this is sub-optimal because it always starts from scratch
static int64_t bitunpack_rev(char* source, uint64_t *source_offset,
		uint8_t encoding_length) {
	if (encoding_length > 64) {
		throw runtime_error("Can't unpack values bigger than 64 bit");
	}
	int64_t target = 0;
	for (auto j = 0; j < encoding_length; j++, (*source_offset)++) {
		target |= (1 & (source[(*source_offset) / 8] >> *source_offset % 8))
				<< j;
	}
	return target;
}

static uint64_t varint_decode(char* source, uint8_t *len_out) {
	uint64_t result = 0; // only decoding lengths here
	uint8_t shift = 0;
	while (true) {
		auto byte = *source++;
		(*len_out)++;
		result |= (byte & 127) << shift;
		if ((byte & 128) == 0)
			break;
		shift += 7;
		if (shift > 64) {
			throw runtime_error("Varint-decoding found too large number");
		}
	}
	return result;
}

/*
 * Note that the RLE encoding method is only supported for the following types of data:
 * Repetition and definition levels
 * Dictionary indices
 * Boolean values in data pages, as an alternative to PLAIN encoding
 */

// todo bit-packed-run-len and rle-run-len must be in the range [1, 2^31 - 1].
static void decode_bprle(char* payload_ptr, size_t payload_len,
		uint8_t value_width, uint32_t result[], uint32_t result_len) {
	auto val_null_idx = 0;
	auto rle_ptr = payload_ptr;
	auto rle_payload_len = ((value_width + 7) / 8);

	// given value_width bits, whats the largest number that can be encoded? below we check that the result is never bigger
	uint32_t max_val = (1 << value_width) - 1;

	// TODO make sure we read enough values at the end
	while (rle_ptr < payload_ptr + payload_len) {
		if (val_null_idx >= result_len) {
			return;
		}
		// we varint-decode the run header to find out whether its bit packed or rle. weird flex but ok.
		uint8_t rle_header_len = 0;
		auto rle_header = varint_decode(rle_ptr, &rle_header_len);
		rle_ptr += rle_header_len;

		if (rle_header & 1) { // bit packed header
			int32_t bit_pack_run_len = (rle_header >> 1) * 8;
			if (bit_pack_run_len <= 0) {
				throw runtime_error(
						"Bit pack run length invalid. Corrupted file?");
			}
			// TODO defend against huge run len, can't be bigger than remaining payload_len*8/value_width
			uint64_t source_offset = 0;

			for (auto off = 0; off < bit_pack_run_len; off++) {
				// They just use a partial byte sometimes so bit_pack_run_len might exceed n_values
				if (val_null_idx >= result_len) {
					return;
				}

				uint32_t val = bitunpack_rev(rle_ptr, &source_offset,
						value_width);

				if (val > max_val) {
					throw runtime_error("Payload bigger than allowed");
				}
				result[val_null_idx++] = val;
			}
			rle_ptr += (bit_pack_run_len * value_width) / 8;
		} else { // rle header
			auto rle_run_len = (rle_header >> 1);

			if (rle_run_len <= 0) {
				throw runtime_error(
						"Run length run length invalid. Corrupted file?");
			}

			// TODO defend against huge run len
			uint32_t val = 0;
			for (auto i = 0; i < rle_payload_len; i++) {
				val |= ((uint8_t) *rle_ptr++) << (i * 8);
			}
			if (val > max_val) {
				throw runtime_error("Payload bigger than allowed");
			}

			while (rle_run_len > 0) {
				if (val_null_idx >= result_len) {
					return;
				}
				result[val_null_idx++] = val;
				rle_run_len--;
			}
		}
	}
}

static string type_to_string(Type::type t) {
	std::ostringstream ss;
	ss << t;
	return ss.str();
}

class ColumnScan {
public:
	PageHeader page_header;
	bool seen_dict = false;
	char* page_buf_ptr = nullptr;
	char* page_buf_end_ptr = nullptr;
	void* dict = nullptr;

	uint64_t page_buf_len = 0;
	uint64_t page_start_row = 0;

	template<class T>
	void construct_dictionary() {
		auto dict_size = page_header.dictionary_page_header.num_values;
		dict = new Dictionary<T>(dict_size);
		for (int32_t dict_index = 0; dict_index < dict_size; dict_index++) {
			((Dictionary<T>*) dict)->dict[dict_index] = *(T*) page_buf_ptr;
			page_buf_ptr += sizeof(T);
		}
	}

	void scan_dict_page(ResultColumn& result_col) {
		if (page_header.__isset.data_page_header
				|| !page_header.__isset.dictionary_page_header) {
			throw runtime_error("Dictionary page header mismatch");
		}

		// make sure we like the encoding
		switch (page_header.dictionary_page_header.encoding) {
		case Encoding::PLAIN:
		case Encoding::PLAIN_DICTIONARY: // deprecated
			break;

		default:
			throw runtime_error(
					"Dictionary page has unsupported/invalid encoding");
		}

		if (seen_dict) {
			throw runtime_error("Multiple dictionary pages for column chunk");
		}
		seen_dict = true;

		auto dict_size = page_header.dictionary_page_header.num_values;

		// initialize dictionaries per type
		switch (result_col.type) {
		case Type::BOOLEAN:
			construct_dictionary<bool>();
			break;
		case Type::INT32:
			construct_dictionary<int32_t>();
			break;
		case Type::INT64:
			construct_dictionary<int64_t>();
			break;
		case Type::INT96:
			construct_dictionary<Int96>();
			break;
		case Type::FLOAT:
			construct_dictionary<float>();
			break;
		case Type::DOUBLE:
			construct_dictionary<double>();
			break;
		case Type::BYTE_ARRAY:
			dict = new Dictionary<unique_ptr<char[]>>(dict_size);

			for (int32_t dict_index = 0; dict_index < dict_size; dict_index++) {

				uint32_t str_len = *((uint32_t*) page_buf_ptr);
				page_buf_ptr += sizeof(uint32_t);

				if (page_buf_ptr + str_len > page_buf_end_ptr) {
					throw runtime_error(
							"Declared string length exceeds payload size");
				}

				auto s = std::unique_ptr<char[]>(new char[str_len + 1]);
				s[str_len] = '\0';
				memcpy(s.get(), page_buf_ptr, str_len);
				((Dictionary<unique_ptr<char[]>>*) dict)->dict[dict_index] =
						move(s);
				page_buf_ptr += str_len;
			}

			break;
		default:
			throw runtime_error(
					"Unsupported type for dictionary: "
							+ type_to_string(result_col.type));
		}
	}

	void scan_data_page(ResultColumn& result_col) {
		if (!page_header.__isset.data_page_header
				|| page_header.__isset.dictionary_page_header) {
			throw runtime_error("Data page header mismatch");
		}

		if (page_header.__isset.data_page_header_v2) {
			throw runtime_error("Data page v2 unsupported");
		}

		auto num_values = page_header.data_page_header.num_values;

		// entry is true if value is not NULL
		auto definition_levels = unique_ptr<uint32_t[]>(
				new uint32_t[num_values]);

		// TODO check if column is REQUIRED, if so, dont read def levels

		// we have to first decode the define levels
		switch (page_header.data_page_header.definition_level_encoding) {
		case Encoding::RLE: {
			// read length of define payload, always
			auto def_length = *((uint32_t*) page_buf_ptr);
			page_buf_ptr += sizeof(uint32_t);

			decode_bprle(page_buf_ptr, def_length, 1, definition_levels.get(),
					num_values);
			page_buf_ptr += def_length;
		}
			break;
		default:
			throw runtime_error(
					"Definition levels have unsupported/invalid encoding");
		}

		switch (page_header.data_page_header.encoding) {
		case Encoding::RLE_DICTIONARY:
		case Encoding::PLAIN_DICTIONARY: // deprecated
			scan_data_page_dict(result_col);
			break;

		case Encoding::PLAIN:
			scan_data_page_plain(result_col);
			break;

		default:
			throw runtime_error("Data page has unsupported/invalid encoding");
		}

		page_start_row += num_values;
	}

	template<class T> void fill_values_plain(ResultColumn& result_col) {
		T* result_arr = (T*) result_col.data.get();
		for (int32_t val_offset = 0;
				val_offset < page_header.data_page_header.num_values;
				val_offset++) {
			auto row_idx = page_start_row + val_offset;
			result_arr[row_idx] = *((T*) page_buf_ptr);
			page_buf_ptr += sizeof(T);
		}
	}

	void scan_data_page_plain(ResultColumn& result_col) {
		switch (result_col.type) {
		case Type::BOOLEAN:
			fill_values_plain<bool>(result_col);
			break;

		case Type::INT32:
			fill_values_plain<int32_t>(result_col);
			break;
		case Type::INT64:
			fill_values_plain<int64_t>(result_col);
			break;
		case Type::INT96:
			fill_values_plain<Int96>(result_col);
			break;
		case Type::FLOAT:
			fill_values_plain<float>(result_col);
			break;
		case Type::DOUBLE:
			fill_values_plain<double>(result_col);
			break;
		case Type::BYTE_ARRAY:
			for (int32_t val_offset = 0;
					val_offset < page_header.data_page_header.num_values;
					val_offset++) {
				auto row_idx = page_start_row + val_offset;

				uint32_t str_len = *((uint32_t*) page_buf_ptr);
				page_buf_ptr += sizeof(uint32_t);

				if (page_buf_ptr + str_len > page_buf_end_ptr) {
					throw runtime_error(
							"Declared string length exceeds payload size");
				}

//
//				if (state.definition_levels[val_offset]) {
//					auto s = std::unique_ptr<char[]>(new char[str_len + 1]);
//					s[str_len] = '\0';
//					memcpy(s.get(), page_buf_ptr, str_len);
//
//
//				}

				// TODO put this into the result dict and add reference

				page_buf_ptr += str_len;
			}
			break;

		default:
			throw runtime_error(
					"Unsupported type page_plain "
							+ type_to_string(result_col.type));
		}

	}

	template<class T> void fill_values_dict(ResultColumn& result_col,
			uint32_t* offsets) {
		T* result_arr = (T*) result_col.data.get();

		for (int32_t val_offset = 0;
				val_offset < page_header.data_page_header.num_values;
				val_offset++) {
			// always unpack because NULLs area also encoded (?)
			auto offset = offsets[val_offset];
			auto row_idx = page_start_row + val_offset;

			result_arr[row_idx] = ((Dictionary<T>*) dict)->get(offset);

		}
	}

	// here we look back into the dicts and emit the values we find if the value is defined, otherwise NULL
	void scan_data_page_dict(ResultColumn& result_col) {
		if (!seen_dict) {
			throw runtime_error("Missing dictionary page");
		}

		// the array offset width is a single byte
		auto enc_length = *((uint8_t*) page_buf_ptr);
		page_buf_ptr += sizeof(uint8_t);

		auto num_values = page_header.data_page_header.num_values;

		// num_values is int32, hence all dict offsets have to fit in 32 bit
		auto offsets = unique_ptr<uint32_t[]>(new uint32_t[num_values]);

		// TODO this payload_len is wrong
		decode_bprle(page_buf_ptr, page_buf_len, enc_length, offsets.get(),
				num_values);

		switch (result_col.type) {

		case Type::INT32:
			fill_values_dict<int32_t>(result_col, offsets.get());

			break;

		case Type::INT64:
			fill_values_dict<int64_t>(result_col, offsets.get());

			break;
		case Type::INT96:
			fill_values_dict<Int96>(result_col, offsets.get());

			break;

		case Type::FLOAT:
			fill_values_dict<float>(result_col, offsets.get());

			break;

		case Type::DOUBLE:
			fill_values_dict<double>(result_col, offsets.get());

			break;

		case Type::BYTE_ARRAY: {
			char** result_arr = (char**) result_col.data.get();

			for (int32_t val_offset = 0;
					val_offset < page_header.data_page_header.num_values;
					val_offset++) {
				// always unpack because NULLs area also encoded (?)
				auto offset = offsets[val_offset];
				auto row_idx = page_start_row + val_offset;

				result_arr[row_idx] =
						((Dictionary<unique_ptr<char[]>>*) dict)->get(offset).get();

			}
			break;
		}
		default:
			throw runtime_error(
					"Unsupported type page_dict "
							+ type_to_string(result_col.type));
		}
	}

};

void ParquetFile::scan_column(ScanState& state, ResultColumn& result_col) {
	// we now expect a sequence of data pages in the buffer

	auto& row_group = file_meta_data.row_groups[state.row_group_idx];
	auto& chunk = row_group.columns[result_col.id];

	if (chunk.__isset.file_path) {
		throw runtime_error(
				"Only inlined data files are supported (no references)");
	}

	if (chunk.meta_data.path_in_schema.size() != 1) {
		throw runtime_error("Only flat tables are supported (no nesting)");
	}

	auto chunk_start = chunk.meta_data.data_page_offset;
	if (chunk.meta_data.__isset.dictionary_page_offset
			&& chunk.meta_data.dictionary_page_offset >= 4) {
		// this assumes the data pages follow the dict pages directly.
		chunk_start = chunk.meta_data.dictionary_page_offset;
	}
	auto chunk_len = chunk.meta_data.total_compressed_size;

	// read entire chunk into RAM
	pfile.seekg(chunk_start);
	auto chunk_buf = unique_ptr<char[]>(new char[chunk_len]);
	auto chunk_buf_ptr = chunk_buf.get();

	pfile.read(chunk_buf.get(), chunk_len);
	if (!pfile) {
		throw runtime_error("Could not read chunk. File corrupt?");
	}

	// now we have whole chunk in buffer, proceed to read pages

	ColumnScan cs;
	auto bytes_to_read = chunk_len;

	while (bytes_to_read > 0) {
		auto page_header_len = bytes_to_read; // the header is clearly not that long but we have no idea

		// this is the only other place where we actually unpack a thrift object

		cs.page_header = PageHeader();
		thrift_unpack((const uint8_t*) chunk_buf_ptr,
				(uint32_t*) &page_header_len, &cs.page_header);

		cs.page_header.printTo(cout);
		printf("\n");

		// compressed_page_size does not include the header size
		chunk_buf_ptr += page_header_len;
		bytes_to_read -= page_header_len;

		auto payload_end_ptr = chunk_buf_ptr
				+ cs.page_header.compressed_page_size;

		string decompressed_buf;

		switch (chunk.meta_data.codec) {
		case CompressionCodec::UNCOMPRESSED:
			cs.page_buf_ptr = chunk_buf_ptr;
			cs.page_buf_len = cs.page_header.compressed_page_size;

			break;
		case CompressionCodec::SNAPPY: {
			decompressed_buf.resize(cs.page_header.uncompressed_page_size);
			auto res = snappy::Uncompress(chunk_buf_ptr,
					cs.page_header.compressed_page_size, &decompressed_buf);
			if (!res) {
				throw runtime_error("Decompression failure");
			}
			cs.page_buf_ptr = (char*) decompressed_buf.c_str();
			cs.page_buf_len = cs.page_header.uncompressed_page_size;

			// TODO make sure the amount of bits snappy decommpressed correspnod with uncompressed size
			break;
		}
		default:
			throw runtime_error(
					"Unsupported compression codec. Try uncompressed or snappy");
		}

		cs.page_buf_end_ptr = cs.page_buf_ptr + cs.page_buf_len;

		switch (cs.page_header.type) {
		case PageType::DICTIONARY_PAGE:
			cs.scan_dict_page(result_col);
			break;

		case PageType::DATA_PAGE: {
			cs.scan_data_page(result_col);
			break;
		}
		case PageType::DATA_PAGE_V2:
			throw runtime_error("v2 data page format is not supported");

		default:
			break; // ignore INDEX page type and any other custom extensions
		}

		chunk_buf_ptr = payload_end_ptr;
		bytes_to_read -= cs.page_header.compressed_page_size;
	}
}

void ParquetFile::initialize_column(ResultColumn& col, uint64_t num_rows) {
	col.defined.resize(num_rows);
	fill(col.defined.begin(), col.defined.end(), 0);

	switch (col.type) {
	case Type::BOOLEAN:
		col.data = unique_ptr<char[]>(new char[sizeof(bool) * num_rows]);
		break;
	case Type::INT32:
		col.data = unique_ptr<char[]>(new char[sizeof(int32_t) * num_rows]);
		break;
	case Type::INT64:
		col.data = unique_ptr<char[]>(new char[sizeof(int64_t) * num_rows]);
		break;
	case Type::INT96:
		col.data = unique_ptr<char[]>(new char[sizeof(Int96) * num_rows]);
		break;
	case Type::FLOAT:
		col.data = unique_ptr<char[]>(new char[sizeof(float) * num_rows]);
		break;
	case Type::DOUBLE:
		col.data = unique_ptr<char[]>(new char[sizeof(double) * num_rows]);
		break;
	case Type::BYTE_ARRAY:
		col.data = unique_ptr<char[]>(new char[sizeof(char*) * num_rows]);
		break;

	default:
		throw runtime_error("Unsupported type " + type_to_string(col.type));
	}
}

bool ParquetFile::scan(ScanState &s, ResultChunk& result) {

	if (s.row_group_idx >= file_meta_data.row_groups.size()) {
		result.nrows = 0;
		return false;
	}

	auto& row_group = file_meta_data.row_groups[s.row_group_idx];
	result.nrows = row_group.num_rows;
	// TODO check row group index

	for (auto& result_col : result.cols) {
		initialize_column(result_col, row_group.num_rows);
		scan_column(s, result_col);
	}

	// todo make sure some data was scanned

	s.row_group_idx++;
	return true;
}

void ParquetFile::initialize_result(ResultChunk& result) {
	result.nrows = 0;
	result.cols.resize(columns.size());
	for (size_t col_idx = 0; col_idx < columns.size(); col_idx++) {
		result.cols[col_idx].type = columns[col_idx]->type;
		result.cols[col_idx].id = col_idx;

	}
}

