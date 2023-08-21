//
// Created by gnandrade on 19/03/2020.
//

#include "utils/Data.h"

Data::Data(const char * filePath, int& d)
{
    if (strcmp(filePath, "random") == 0) {
        // We will use random vectors as base
        this->nb_size = -1;
        this->dim = d;
        this->bvecs = false;
    } else {
        auto str_len = strlen(filePath);
        string extension(filePath + str_len - 5, filePath + str_len);

        if (strcmp(extension.data(), "bvecs") == 0) {
            mmapedFile = bvecs_read(filePath, &d, &this->nb_size);
            this->bvecs = true;
        } else if (strcmp(extension.data(), "fvecs") == 0) {
            nb = fvecs_read(filePath, &d, &this->nb_size);
            this->bvecs = false;
        }

        this->nb_itr = 0;
        this->dim = d;
    }
}

Data::Data(const char * filePath, int& d, long amount, long offset)
{
    if (strcmp(filePath, "random") == 0) {
        // We will use random vectors as base
        this->nb_size = -1;
        this->dim = d;
        this->bvecs = false;
    } else {
        auto str_len = strlen(filePath);
        string extension(filePath + str_len - 5, filePath + str_len);

        if (strcmp(extension.data(), "bvecs") == 0) {
            mmapedFile = bvecs_read(filePath, d, offset);
            this->nb_size = amount;
            this->bvecs = true;
        } else if (strcmp(extension.data(), "fvecs") == 0) {
            nb = fvecs_read(filePath, &d, &this->nb_size, amount, offset);
            this->nb_size = amount;
            this->bvecs = false;
        }

        this->nb_itr = 0;
        this->dim = d;
    }
}

Data::~Data()
{
    if (this->nb_size > 0) {
        if (this->bvecs) {
            size_t fz = (4 + dim) * this->nb_size;
            munmap(mmapedFile, fz);
        }
    }
}

float* Data::getNextBatch(long n) {
    if (this->nb_size == -1) {
        return packingRandomVectors(n, this->dim);
    }

    float* nextVec;
    if (this->bvecs) {
        nextVec = getNextBatchFloatVector(this->mmapedFile, n, dim, this->nb_itr);
    } else {
        nextVec = getNextVector(n);
    }

    this->nb_itr += n;
    if (this->nb_itr >= this->nb_size) {
        this->nb_itr = 0;
    }

    return nextVec;
}

float* Data::getNextVector(long n) {

    if (this->nb_size == -1) {
        return packingRandomVectors(n, this->dim);
    }

    if (n == nb_size) {
        return nb;
    }

    auto nextVec = new float[n * this->dim];
    long nb_itr_offset_b = nb_itr * this->dim;
    long nb_itr_offset_e = nb_itr_offset_b + (n * this->dim);
    copy (nb + nb_itr_offset_b, nb + nb_itr_offset_e, nextVec);

    return nextVec;
}

// Packing Random Vectors
float* Data::packingRandomVectors(long n, int d)
{
    auto xb = new float[d * n];
    for(long i = 0; i < n; i++) {
        for(int j = 0; j < d; j++) {
            xb[d * i + j] = (float) drand48();
        }
    }
    return xb;
}

void Data::printMatrix(float* mat, int n, int d)
{
    for(int i = 0; i < n; i++) {
        for(int j = 0; j < d; j++)
            printf("%5f ", mat[i * d + j]);
        printf("\n");
    }
}

void Data::printMatrix(long* mat, int n, int d)
{
    for(int i = 0; i < n; i++) {
        for(int j = 0; j < d; j++)
            printf("%ld ", mat[i * d + j]);
        printf("\n");
    }
}

vector<double> Data::Avg(vector<double> vec) {
    vector<double> result;

    // Avg
    double avg = 0.0;
    int count = 0;
    for (auto value : vec) {
        avg += value;
        count += 1;
    }
    avg /= (double) count;
    result.push_back(avg);

    // STDDEV
    double variance = 0.0;
    double std_dev = 0.0;
    for (auto time : vec) {
        variance += pow((time - avg),2);
        variance /= count;
        count++;
    }
    std_dev = sqrt(variance);
    result.push_back(std_dev);

    return result;
}

double Data::Throughput(double time, int n)
{
    return (double) n / time;
}

double Data::Throughput(double time)
{
    return 1.0 / time;
}

string Data::floatToString(float f) {
    std::ostringstream ss;
    ss.precision(9);
    ss << f;
    std::string s(ss.str());
    return s;
}

string Data::floatToString(double f) {
    std::ostringstream ss;
    ss.precision(9);
    ss << f;
    std::string s(ss.str());
    return s;
}

string Data::timeStampToString(double f) {
    std::ostringstream ss;
    ss.precision(13);
    ss << f;
    std::string s(ss.str());
    return s;
}

unsigned int Data::round_closest(unsigned int dividend, unsigned int divisor)
{
    return (dividend + (divisor / 2)) / divisor;
}

long Data::round_up(long x, long y)
{
    return (x + y - 1) / y;
}