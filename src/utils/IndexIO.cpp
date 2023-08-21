//
// Created by gnandrade on 20/04/2020.
//

#include "utils/IndexIO.h"

bool indexFileExists(const std::string& name) {
    fstream fileStream;
    fileStream.open(name);
    if (fileStream.fail()) {
        return false;
    }
    return true;
}

unsigned char* bvecs_read(const char *fname, int* d_out, long* n_out) {
    FILE *f = fopen(fname, "rb");

    if (!f) {
        fprintf(stderr, "could not open %s\n", fname);
        perror("");
        abort();
    }

    struct stat st;
    fstat(fileno(f), &st);
    auto filesize = st.st_size;

    unsigned char* dataset = (unsigned char*) mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, fileno(f), 0);
    int* intptr = (int*) dataset;
    *d_out = intptr[0];
    *n_out = filesize / (4 + *d_out);

    fclose(f);

    return dataset;
}

unsigned char* bvecs_read(const char *fname, int dim, long offset) {
    FILE *f = fopen(fname, "rb");

    if (!f) {
        fprintf(stderr, "could not open %s\n", fname);
        perror("");
        abort();
    }

    struct stat st;
    fstat(fileno(f), &st);

    unsigned char* dataset = (unsigned char*) mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, fileno(f),  0);
    dataset += (offset*4) + (offset*(dim));

    fclose(f);
    return dataset;
}

float* fvecs_read (const char *fname, int *d_out, long *n_out) {
    FILE *f = fopen(fname, "r");
    if(!f) {
        fprintf(stderr, "could not open %s\n", fname);
        perror("");
        abort();
    }
    int d;

    assert(fread(&d, 1, sizeof(int), f) == sizeof(int) || !"could not read d");
    assert((d > 0 && d < 1000000) || !"unreasonable dimension");
    fseek(f, 0, SEEK_SET);
    struct stat st;
    fstat(fileno(f), &st);
    size_t sz = st.st_size;
    assert(sz % ((d + 1) * 4) == 0 || !"weird file size");
    size_t n = sz / ((d + 1) * 4);

    *d_out = (int) d; *n_out = (int) n;
    float *x = new float[n * (d + 1)];
    size_t nr = fread(x, sizeof(float), n * (d + 1), f);
    assert(nr == n * (d + 1) || !"could not read whole file");

    // shift array to remove row headers
    for(size_t i = 0; i < n; i++)
        memmove(x + i * d, x + 1 + i * (d + 1), d * sizeof(*x));

    fclose(f);
    return x;
}

float* fvecs_read (const char *fname, int *d_out, long *n_out, long amount, long offset) {
    FILE *f = fopen(fname, "r");
    if(!f) {
        fprintf(stderr, "could not open %s\n", fname);
        perror("");
        abort();
    }
    int d;

    assert(fread(&d, 1, sizeof(int), f) == sizeof(int) || !"could not read d");
    assert((d > 0 && d < 1000000) || !"unreasonable dimension");
    fseek(f, offset * ((d + 1) * 4), SEEK_SET); // HERE
    struct stat st;
    fstat(fileno(f), &st);
    size_t sz = st.st_size;
    assert(sz % ((d + 1) * 4) == 0 || !"weird file size");
    size_t n = amount; // sz / ((d + 1) * 4);

    *d_out = (int) d; *n_out = (int) n;
    float *x = new float[n * (d + 1)];
    size_t nr = fread(x, sizeof(float), n * (d + 1), f);
    assert(nr == n * (d + 1) || !"could not read whole file");

    // shift array to remove row headers
    for(size_t i = 0; i < n; i++)
        memmove(x + i * d, x + 1 + i * (d + 1), d * sizeof(*x));

    //int i = 0;
    //cout << "[" << i << "]: " << x[i*d] << " " << x[i*d + 1] << " ... " << x[i*d + (d-2)] << " " << x[i*d + (d-1)] << endl;
    //i = amount - 1;
    //cout << "[" << i << "]: " << x[i*d] << " " << x[i*d + 1] << " ... " << x[i*d + (d-2)] << " " << x[i*d + (d-1)] << endl;

    fclose(f);
    return x;
}

float* to_float_array(unsigned char* vector, int ne, int d) {
    float* res = new float[ne * d];

    for (int i = 0; i < ne; i++) {
        vector += 4;

        for (int cd = 0; cd < d; cd++) {
            res[i * d + cd] = *vector;
            vector++;
        }
    }

    return res;
}

float* getNextBatchFloatVector(unsigned char* vector, long batch_size, int d, long offset_b) {
    float* res = new float[batch_size * d];

    vector += (offset_b*4) + (offset_b*d) ;
    for (long i = 0; i < batch_size; i++) {
        vector += 4;
        for (int cd = 0; cd < d; cd++) {
            res[i * d + cd] = *vector;
            vector++;
        }
    }

    return res;
}