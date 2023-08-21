//
// Created by gnandrade on 20/04/2020.
//

#ifndef PQNNS_MULTI_STREAM_INDEXIO_H
#define PQNNS_MULTI_STREAM_INDEXIO_H

#include <sys/stat.h>
#include <unistd.h>
#include <string>
#include <fstream>
#include <iostream>
#include <sys/mman.h>

#include <faiss/index_io.h>
#include <faiss/impl/FaissAssert.h>
#include "faiss/IndexFlat.h"
#include "faiss/IndexIVFPQ.h"

using namespace std;

bool indexFileExists(const std::string& name);
unsigned char* bvecs_read(const char *fname, int* d_out, long* n_out);
unsigned char* bvecs_read(const char *fname, int dim, long offset);
float* fvecs_read (const char *fname, int *d_out, long *n_out);
float* fvecs_read (const char *fname, int *d_out, long *n_out, long amount, long offset);
float* to_float_array(unsigned char* vector, int ne, int d);
float* getNextBatchFloatVector(unsigned char* vector, long batch_size, int d, long offset_b);

#endif //PQNNS_MULTI_STREAM_INDEXIO_H
