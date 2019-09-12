#include <Rdefines.h>
// motherfucker
#undef error

using namespace std;


extern "C" {


SEXP miniparquet_read(SEXP extptr) {
	return NULL;
}


// R native routine registration
#define CALLDEF(name, n)                                                                                               \
	{ #name, (DL_FUNC)&name, n }
static const R_CallMethodDef R_CallDef[] = {CALLDEF(miniparquet_read, 1),

                                            {NULL, NULL, 0}};

void R_init_miniparquet(DllInfo *dll) {
	R_registerRoutines(dll, NULL, R_CallDef, NULL, NULL);
	R_useDynamicSymbols(dll, FALSE);
}
}
