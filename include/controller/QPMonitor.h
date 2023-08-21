//
// Created by gnandrade on 22/04/2021.
//

#ifndef PQNNS_MULTI_STREAM_QPMONITOR_H
#define PQNNS_MULTI_STREAM_QPMONITOR_H

#include <unordered_map>
#include <string>
#include <iostream>
#include <utility>
#include <vector>
#include <algorithm>
#include <fstream>

using namespace std;

typedef unordered_map<int, vector<pair<double, long>>> MetricMap;

class QPMonitor {
public:
    enum Metric
    { 
        __begin,
        /* Query Metrics */
        QAR,    // Arrival Rate 
        QST,    // System Throughput
        QQS,    // Queue Size
        QQT,    // Queue Time       // Per thread
        QUW,    // UpdatedWait      // Per thread
        QET,    // Execution Time   // Per thread
        QRT,    // Response Time    // Per thread
        /* Bucket Metrics */
        BOR,    // Bucket outdated ration
        BOM,    // Bucket outdated max
        /* Insertion Metrics */
        IAR,    // Arrival Rate
        IST,    // System Throughput
        IQS,    // Queue Size
        IQT,    // Queue Time       // Per thread
        IET,    // Execution Time   // Per thread
        IRT,    // Response Time    // Per thread
        /* Static Metrics */
        T,
        TUB,
        TLB,
        CONF,
        __end,
    };

    void    setup();
    void    final(Metric metric);
    void    configure(int query, int insertion);
    void    print();
    void    add(Metric metric, double value);
    void    add(Metric metric, double value, int tid);
    void    set(Metric metric, double value);
    void    resetAll();
    void    reset(Metric metric);
    double  getCurrent(Metric metric);
    double  getOld(Metric metric);
    void    log(const char* file);
    int     trend(Metric metric);
    void    setTrend(Metric metric, int trend);

private:
    double  get(Metric metric);
    double computeMetric(Metric metric, MetricMap metric_map);
    void resize(Metric metric, int size);
    bool isQueryMetric(Metric metric);
    bool isGlobalMetric(Metric metric);
    bool isThroughputMetric(Metric metric);
    bool isSignalMetric(Metric metric);
    bool isStaticMetric(Metric metric);

    const vector<Metric> _per_thread_metrics = {    
        Metric::QST, Metric::QQT, Metric::QUW, Metric::QET, Metric::QRT,
        Metric::IST, Metric::IQT, Metric::IET, Metric::IRT};

    const vector<Metric> _global_metrics = {
        Metric::QAR, Metric::QQS, Metric::BOR, Metric::BOM,
        Metric::IAR, Metric::IQS};

    const vector<Metric> _query_metrics = {
        Metric::QAR, Metric::QST, Metric::QQS,
        Metric::QQT, Metric::QUW, Metric::QET, Metric::QRT};

    const vector<Metric> _insertion_metrics = {
        Metric::IQT, Metric::IET, Metric::IRT, 
        Metric::IAR, Metric::IST, Metric::IQS};

    const vector<Metric> _throughput_metrics = {
        Metric::QAR, Metric::IAR, Metric::QST, Metric::IST};

    const vector<Metric> _signal_metrics = {
        Metric::QAR, Metric::IAR, Metric::QQS,
        Metric::BOR};

    const vector<Metric> _static_metrics = {
        Metric::T, Metric::TLB, Metric::TUB, Metric::CONF
    };

    unordered_map<Metric, int> _metrics_trend;
    MetricMap _metrics;
    MetricMap _metrics_before_reset;
};

#endif //PQNNS_MULTI_STREAM_QPMONITOR_H