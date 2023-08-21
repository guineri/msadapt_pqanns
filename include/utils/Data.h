//
// Created by gnandrade on 19/03/2020.
//

#ifndef PQNNS_WS_DATA_H
#define PQNNS_WS_DATA_H

#include <cstdlib>
#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <cmath>

#include "Constants.h"
#include "utils/Matrix.h"
#include "utils/Time.h"
#include "IndexIO.h"

using namespace std;

class Data {

private:
    float *nb;
    long nb_size;
    long nb_itr;
    int dim;
    unsigned char* mmapedFile;
    bool bvecs;

public:
    Data(const char * filePath, int& d);
    Data(const char * filePath, int& d, long amount, long initOffset);
    ~Data();
    float *getNextBatch(long n);
    float *getNextVector(long n);

    static float* packingRandomVectors(long n, int d);
    static void printMatrix(float* mat, int n, int d);
    static void printMatrix(long* mat, int n, int d);

    static vector<double> Avg(vector<double> vec);
    static double Throughput(double time, int n);
    static double Throughput(double time);

    static string floatToString(float f);
    static string floatToString(double f);
    static string timeStampToString(double f);
    static unsigned int round_closest(unsigned int dividend, unsigned int divisor);
    static long round_up(long x, long y);
};

#endif // PQNNS_WS_DATA_H
