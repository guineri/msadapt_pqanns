//
// Created by gnandrade on 26/03/2020.
//

#include "utils/Time.h"

double Time::streamRateControlPoisson(double loadFactor, double execTime)
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);

    double maxThroughput = 1.0 / execTime;
    double lambda = maxThroughput * loadFactor;
    double U = 1.0 - dis(gen);
    double L = -log(U);
    return (L / lambda) * 1000000.00;
}

long  Time::streamRateControlEqual(double loadFactor, double execTime) {
    double tbest = execTime;
    double maxThroughput = 1.0 / tbest;
    double incomingRate = maxThroughput * loadFactor;
    return (1.0/incomingRate) * 1000000;
}

double Time::getCurrentTime()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec * 1e-6;
}

std::chrono::system_clock::duration duration_since_midnight()
{
    auto now = std::chrono::system_clock::now();

    time_t tnow = std::chrono::system_clock::to_time_t(now);
    tm* date = std::localtime(&tnow);
    date->tm_hour = 0;
    date->tm_min = 0;
    date->tm_sec = 0;
    auto midnight = std::chrono::system_clock::from_time_t(std::mktime(date));

    return now - midnight;
}

float Time::msSinceMidNight()
{
    auto since_midnight = duration_since_midnight();
    auto milliseconds =
        std::chrono::duration_cast<std::chrono::microseconds>(since_midnight);
    return (float)milliseconds.count();
}

char* Time::getCurrentTimeFormate()
{
    time_t curr_time;
    tm* curr_tm;
    char* data = (char*)malloc(sizeof(char) * 20);

    time(&curr_time);
    curr_tm = localtime(&curr_time);

    strftime(data, 50, "%F %T", curr_tm);

    return data;
}