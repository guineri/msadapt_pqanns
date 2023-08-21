//
// Created by gnandrade on 26/03/2020.
//

#ifndef DATA_TIME_H
#define DATA_TIME_H

#include <chrono>
#include <cstddef>
#include <iostream>
#include <sys/time.h>
#include <cmath>
#include <unistd.h>
#include <random>
#include <iomanip>

using namespace std::chrono;

class Time {
public:
    static double getCurrentTime();

    static float msSinceMidNight();

    static char *getCurrentTimeFormate();

    static double streamRateControlPoisson(double loadFactor, double execTime);

    static long streamRateControlEqual(double loadFactor, double execTime);
};


#endif // DATA_TIME_H
