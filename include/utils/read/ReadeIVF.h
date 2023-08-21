#ifndef READSPLITTEDINDEX_H_
#define READSPLITTEDINDEX_H_

#include <vector>
#include <iostream>

#include <faiss/IndexIVFPQ.h>
#include <faiss/IndexIVFPQR.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/index_io.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/IndexFlat.h>
#include <faiss/impl/io.h>

#define READ1(x)  READANDCHECK(&(x), 1)

#define READANDCHECK(ptr, n) {                              \
        size_t ret = (*f)(ptr, sizeof(*(ptr)), n);          \
        FAISS_THROW_IF_NOT_MSG(ret == (n), "read error");   \
    }

#define READVECTOR(vec) {                       \
        long size;                            \
        READANDCHECK (&size, 1);                \
        FAISS_THROW_IF_NOT (size >= 0 && size < (1L << 40));  \
        (vec).resize (size);                    \
        READANDCHECK ((vec).data (), size);     \
    }

using namespace std;

faiss::Index *read_index_especial(const char *fname, vector<long> centroids, vector<int> parts, float r, int io_flags);

#endif /* READSPLITTEDINDEX_H_ */
