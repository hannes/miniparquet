// motherfucker
#undef error
#undef length

#include <iostream>
#include <cmath>

#include "miniparquet.h"
#undef ERROR
#include <Rversion.h>
#include <Rdefines.h>
#undef nrows

// because we need to initialize the altrep class
#include <R_ext/Rdynload.h>
#include <R_ext/Altrep.h>

using namespace miniparquet;
using namespace std;

// surely they are joking
constexpr int64_t kJulianToUnixEpochDays = 2440588LL;
constexpr int64_t kMillisecondsInADay = 86400000LL;
constexpr int64_t kNanosecondsInADay = kMillisecondsInADay * 1000LL * 1000LL;

static int64_t impala_timestamp_to_nanoseconds(const Int96& impala_timestamp) {
	int64_t days_since_epoch = impala_timestamp.value[2]
			- kJulianToUnixEpochDays;
	int64_t nanoseconds =
			*(reinterpret_cast<const int64_t*>(&(impala_timestamp.value)));
	return days_since_epoch * kNanosecondsInADay + nanoseconds;
}

struct parquet_altrep_col {
	shared_ptr<ParquetFile> f;
	uint64_t col_idx;
};

extern "C" {

R_altrep_class_t parquet_logical;
R_altrep_class_t parquet_integer;
R_altrep_class_t parquet_numeric;
R_altrep_class_t parquet_string;

static void* parquet_finalize(SEXP eptr) {
	auto col = (parquet_altrep_col*) R_ExternalPtrAddr(eptr);
	if (col) {
		R_ClearExternalPtr(eptr);
		delete col;
	}
	return R_NilValue;
}

SEXP miniparquet_read(SEXP filesxp) {

	if (TYPEOF(filesxp) != STRSXP || LENGTH(filesxp) != 1) {
		Rf_error("miniparquet_read: Need single filename parameter");
	}

	try {
		// parse the query and transform it into a set of statements

		char *fname = (char *) CHAR(STRING_ELT(filesxp, 0));

		shared_ptr<ParquetFile> f_ptr = shared_ptr<ParquetFile>(
				new ParquetFile(fname));
		ParquetFile& f = *f_ptr.get();

		// allocate vectors

		auto ncols = f.columns.size();

		SEXP retlist = PROTECT(NEW_LIST(ncols));
		if (!retlist) {
			UNPROTECT(1); // retlist
			Rf_error("miniparquet_read: Memory allocation failed");
		}
		SEXP names = PROTECT(NEW_STRING(ncols));
		if (!names) {
			UNPROTECT(2); // retlist, names
			Rf_error("miniparquet_read: Memory allocation failed");
		}
		SET_NAMES(retlist, names);
		UNPROTECT(1); // names

		for (size_t col_idx = 0; col_idx < ncols; col_idx++) {
			SEXP varname = PROTECT(
					mkCharCE(f.columns[col_idx]->name.c_str(), CE_UTF8));
			if (!varname) {
				UNPROTECT(2); // varname, retlist
				Rf_error("miniparquet_read: Memory allocation failed");
			}
			SET_STRING_ELT(names, col_idx, varname);
			UNPROTECT(1); // varname

			// common holder object
			auto col = new parquet_altrep_col();
			col->f = f_ptr;
			col->col_idx = col_idx;

			SEXP eptr = PROTECT(
					R_MakeExternalPtr((void*) col, R_NilValue, R_NilValue));
			R_RegisterCFinalizer(eptr, (void (*)(SEXP)) parquet_finalize);

			SEXP varvalue = NULL;
			switch (f.columns[col_idx]->type) {
			case parquet::format::Type::BOOLEAN:
				varvalue = R_new_altrep(parquet_logical, eptr, R_NilValue);
				break;

			case parquet::format::Type::INT32:
				varvalue = R_new_altrep(parquet_integer, eptr, R_NilValue);
				break;

			case parquet::format::Type::INT64:
			case parquet::format::Type::DOUBLE:
			case parquet::format::Type::FLOAT:
				varvalue = R_new_altrep(parquet_numeric, eptr, R_NilValue);
				break;

			case parquet::format::Type::INT96: {
				varvalue = R_new_altrep(parquet_numeric, eptr, R_NilValue);
				SEXP cl = PROTECT(NEW_STRING(2));
				SET_STRING_ELT(cl, 0, PROTECT(mkChar("POSIXct")));
				SET_STRING_ELT(cl, 1, PROTECT(mkChar("POSIXt")));
				SET_CLASS(varvalue, cl);
				setAttrib(varvalue, install("tzone"), PROTECT(mkString("UTC")));
				UNPROTECT(4);
				break;
			}/*
			 case parquet::format::Type::FIXED_LEN_BYTE_ARRAY: { // oof
			 auto& s_ele = f.columns[col_idx]->schema_element;
			 if (!s_ele->__isset.converted_type) {
			 throw runtime_error("Missing FLBA type");
			 }
			 switch (s_ele->converted_type) {
			 case parquet::format::ConvertedType::DECIMAL:
			 varvalue = PROTECT(NEW_NUMERIC(nrows));
			 break;
			 default:
			 UNPROTECT(1); // retlist
			 auto it =
			 parquet::format::_ConvertedType_VALUES_TO_NAMES.find(
			 s_ele->converted_type);
			 Rf_error("miniparquet_read: Unknown FLBA type %s",
			 it->second);
			 }
			 break;
			 }*/
			case parquet::format::Type::BYTE_ARRAY:
				varvalue = R_new_altrep(parquet_string, eptr, R_NilValue);

				break;
			default:
				UNPROTECT(1); // retlist or eptr
				auto it = parquet::format::_Type_VALUES_TO_NAMES.find(
						f.columns[col_idx]->type);
				Rf_error("miniparquet_read: Unknown column type %s",
						it->second); // unlikely
			}
			if (!varvalue) {
				UNPROTECT(2); // varvalue, retlist
				Rf_error("miniparquet_read: Memory allocation failed");
			}
			SET_VECTOR_ELT(retlist, col_idx, varvalue);
			UNPROTECT(1); /* varvalue */
		}

		// at this point retlist is fully allocated and the only protected SEXP

		UNPROTECT(1); // retlist
		return retlist;

	} catch (std::exception &ex) {
		Rf_error(ex.what());
		// TODO this may leak
	}

}

// ALTREP magic

static R_xlen_t parquet_length(SEXP s) {
	auto col = (parquet_altrep_col*) R_ExternalPtrAddr(R_altrep_data1(s));
	return col->f->nrow;
}

static Rboolean parquet_inspect(SEXP s, int pre, int deep, int pvec,
		void (*inspect_subtree)(SEXP, int, int, int)) {

	auto col = (parquet_altrep_col*) R_ExternalPtrAddr(R_altrep_data1(s));
	Rprintf("miniparquet(%s:%d)\n", col->f->filename.c_str(), col->col_idx);
	return TRUE;
}

static const void* parquet_dataptr_or_null(SEXP s) {
	if (R_altrep_data2(s) != R_NilValue) {
		switch (TYPEOF(s)) {
		case LGLSXP:
			return LOGICAL(R_altrep_data2(s));
		case INTSXP:
			return INTEGER(R_altrep_data2(s));
		case REALSXP:
			return NUMERIC_POINTER(R_altrep_data2(s));
		case STRSXP:
			return STRING_PTR(R_altrep_data2(s));
		default:
			Rf_error("Unkown SEXP type");
		}
	}
	return NULL;
}

static void* parquet_dataptr(SEXP s, Rboolean writeable) {

	if (R_altrep_data2(s) != R_NilValue) {
		return (void*) parquet_dataptr_or_null(s);
	}

	SEXP dest;
	auto col = (parquet_altrep_col*) R_ExternalPtrAddr(R_altrep_data1(s));

	switch (TYPEOF(s)) {
	case LGLSXP:
		dest = PROTECT(NEW_LOGICAL(col->f->nrow));
		break;
	case INTSXP:
		dest = PROTECT(NEW_INTEGER(col->f->nrow));
		break;
	case REALSXP:
		dest = PROTECT(NEW_NUMERIC(col->f->nrow));
		break;
	case STRSXP:
		dest = PROTECT(NEW_STRING(col->f->nrow));
		break;
	default:
		Rf_error("eek");
	}

	if (!dest) {
		Rf_error("Memory allocation failure");
	}

	ResultColumn result_col;
	ScanState ss;

	// TODO these should not be here
	result_col.col = col->f->columns[col->col_idx].get();
	result_col.id = col->col_idx;

	uint64_t dest_offset = 0;
	while (col->f->scan_column(ss, result_col)) {

		for (uint64_t row_idx = 0; row_idx < result_col.nrows; row_idx++) {
			if (!result_col.defined.ptr[row_idx]) {
				// NULLs
				switch (col->f->columns[col->col_idx]->type) {
				case parquet::format::Type::BOOLEAN:
					LOGICAL_POINTER(dest)[row_idx + dest_offset] = NA_LOGICAL;
					break;
				case parquet::format::Type::INT32:
					INTEGER_POINTER(dest)[row_idx + dest_offset] = NA_INTEGER;
					break;
				case parquet::format::Type::INT64:
				case parquet::format::Type::DOUBLE:
				case parquet::format::Type::FLOAT:
				case parquet::format::Type::INT96:
					NUMERIC_POINTER(dest)[row_idx + dest_offset] = NA_REAL;
					break; /*
					 case parquet::format::Type::FIXED_LEN_BYTE_ARRAY: { // oof, TODO duplication above
					 auto& s_ele = f.columns[col_idx]->schema_element;
					 if (!s_ele->__isset.converted_type) {
					 throw runtime_error("Missing FLBA type");
					 }
					 switch (s_ele->converted_type) {
					 case parquet::format::ConvertedType::DECIMAL:
					 NUMERIC_POINTER(dest)[row_idx + dest_offset] = NA_REAL;
					 break;
					 default:
					 UNPROTECT(1); // retlist
					 auto it =
					 parquet::format::_ConvertedType_VALUES_TO_NAMES.find(
					 s_ele->converted_type);
					 Rf_error("miniparquet_read: Unknown FLBA type %s",
					 it->second);
					 }
					 break;
					 }*/
				case parquet::format::Type::BYTE_ARRAY:
					SET_STRING_ELT(dest, row_idx + dest_offset, NA_STRING);
					break;

				default: {
					UNPROTECT(1); // retlist
					auto it = parquet::format::_Type_VALUES_TO_NAMES.find(
							col->f->columns[col->col_idx]->type);
					Rf_error("miniparquet_read: Unknown column type %s",
							it->second); // unlikely
				}
				}
			} else {

				switch (col->f->columns[col->col_idx]->type) {
				case parquet::format::Type::BOOLEAN:
					LOGICAL_POINTER(dest)[row_idx + dest_offset] =
							((bool*) result_col.data.ptr)[row_idx];
					break;
				case parquet::format::Type::INT32:
					INTEGER_POINTER(dest)[row_idx + dest_offset] =
							((int32_t*) result_col.data.ptr)[row_idx];
					break;
				case parquet::format::Type::INT64:
					NUMERIC_POINTER(dest)[row_idx + dest_offset] =
							(double) ((int64_t*) result_col.data.ptr)[row_idx];
					break;
				case parquet::format::Type::DOUBLE:
					NUMERIC_POINTER(dest)[row_idx + dest_offset] =
							((double*) result_col.data.ptr)[row_idx];
					break;
				case parquet::format::Type::FLOAT:
					NUMERIC_POINTER(dest)[row_idx + dest_offset] =
							(double) ((float*) result_col.data.ptr)[row_idx];
					break;
				case parquet::format::Type::INT96:
					NUMERIC_POINTER(dest)[row_idx + dest_offset] =
							impala_timestamp_to_nanoseconds(
									((Int96*) result_col.data.ptr)[row_idx])
									/ 1000000000;
					break;
					/*
					 case parquet::format::Type::FIXED_LEN_BYTE_ARRAY: { // oof, TODO clusterfuck
					 auto& s_ele = f.columns[col_idx]->schema_element;
					 if (!s_ele->__isset.converted_type) {
					 throw runtime_error("Missing FLBA type");
					 }
					 switch (s_ele->converted_type) {
					 case parquet::format::ConvertedType::DECIMAL:

					 {

					 // this is a giant clusterfuck
					 auto type_len = s_ele->type_length;
					 auto bytes = ((char**) col.data.ptr)[row_idx];
					 int64_t val = 0;
					 for (auto i = 0; i < type_len; i++) {
					 val = val << ((type_len - i) * 8) | (uint8_t) bytes[i];
					 }

					 NUMERIC_POINTER(dest)[row_idx + dest_offset] = val
					 / pow(10.0, s_ele->scale);

					 }

					 break;
					 default:
					 UNPROTECT(1); // retlist
					 auto it =
					 parquet::format::_ConvertedType_VALUES_TO_NAMES.find(
					 s_ele->converted_type);
					 Rf_error("miniparquet_read: Unknown FLBA type %s",
					 it->second);
					 }
					 break;
					 }
					 */
				case parquet::format::Type::BYTE_ARRAY:
					SET_STRING_ELT(dest, row_idx + dest_offset,
							mkCharCE(((char**) result_col.data.ptr)[row_idx],
									CE_UTF8));
					break;

				default: {
					auto it = parquet::format::_Type_VALUES_TO_NAMES.find(
							col->f->columns[col->col_idx]->type);
					UNPROTECT(1); // retlist
					Rf_error("miniparquet_read: Unknown column type %s",
							it->second); // unlikely
				}
				}
			}

		}

		dest_offset += result_col.nrows;
	}
	R_set_altrep_data2(s, dest);
	UNPROTECT(1); // dest;
	return (void*) parquet_dataptr_or_null(s); // same code path for everyone
}

static SEXP parquet_string_elt(SEXP s, R_xlen_t index) {
	auto col = (parquet_altrep_col*) R_ExternalPtrAddr(R_altrep_data1(s));
	if (index > col->f->nrow) {
		Rf_error("parquet_string_elt index out of bounds : %lld > %lld", index,
				col->f->nrow);
	}

	// find out which row group the index is in
	// check if row group was already materialized
	// return materialized value

	SEXP* strings = (SEXP*) parquet_dataptr(s, (Rboolean) false);
	return strings[index];
}

// R native routine registration
#define CALLDEF(name, n)                                                                                               \
	{ #name, (DL_FUNC)&name, n }
static const R_CallMethodDef R_CallDef[] = { CALLDEF(miniparquet_read, 1),

{ NULL, NULL, 0 } };

void R_init_miniparquet(DllInfo *dll) {
	R_registerRoutines(dll, NULL, R_CallDef, NULL, NULL);
	R_useDynamicSymbols(dll, FALSE);

	parquet_logical = R_make_altlogical_class("parquet_logical", "miniparquet",
			dll);
	parquet_integer = R_make_altinteger_class("parquet_integer", "miniparquet",
			dll);
	parquet_numeric = R_make_altreal_class("parquet_numeric", "miniparquet",
			dll);
	parquet_string = R_make_altstring_class("parquet_string", "miniparquet",
			dll);

	// altrep
	R_set_altrep_Length_method(parquet_logical, parquet_length);
	R_set_altrep_Inspect_method(parquet_logical, parquet_inspect);

	R_set_altrep_Length_method(parquet_integer, parquet_length);
	R_set_altrep_Inspect_method(parquet_integer, parquet_inspect);

	R_set_altrep_Length_method(parquet_numeric, parquet_length);
	R_set_altrep_Inspect_method(parquet_numeric, parquet_inspect);

	R_set_altrep_Length_method(parquet_string, parquet_length);
	R_set_altrep_Inspect_method(parquet_string, parquet_inspect);

	// altvec
	R_set_altvec_Dataptr_method(parquet_logical, parquet_dataptr);
	R_set_altvec_Dataptr_or_null_method(parquet_logical,
			parquet_dataptr_or_null);

	R_set_altvec_Dataptr_method(parquet_integer, parquet_dataptr);
	R_set_altvec_Dataptr_or_null_method(parquet_integer,
			parquet_dataptr_or_null);

	R_set_altvec_Dataptr_method(parquet_numeric, parquet_dataptr);
	R_set_altvec_Dataptr_or_null_method(parquet_numeric,
			parquet_dataptr_or_null);

	R_set_altvec_Dataptr_method(parquet_string, parquet_dataptr);
	R_set_altvec_Dataptr_or_null_method(parquet_string,
			parquet_dataptr_or_null);

	// altstring
	R_set_altstring_Elt_method(parquet_string, parquet_string_elt);
}
}
