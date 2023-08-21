//
// Created by gnandrade on 22/04/2021.
//

#include "controller/QPMonitor.h"

void QPMonitor::setup()
{
    pair<double, long> zeros = make_pair(0.0, 0);
    for (int m = QPMonitor::Metric::__begin + 1; m != QPMonitor::Metric::__end; m++)
    {
        QPMonitor::Metric metric = static_cast<QPMonitor::Metric>(m);
        resize(metric, 1);
        for (int s = 0; s < _metrics[metric].size(); s++) {
            _metrics[metric][s] = make_pair(0.0, 0);
            _metrics_before_reset[metric][s] = make_pair(0.0, 0);
        }
        if (isSignalMetric(metric)) {
            _metrics_trend[metric] = 0;
        }
    } 
}

void QPMonitor::configure(int query, int insertion)
{
    pair<double, long> zeros = make_pair(0.0, 0);
    for (int m = QPMonitor::Metric::__begin + 1; m != QPMonitor::Metric::__end; m++)
    {
        QPMonitor::Metric metric = static_cast<QPMonitor::Metric>(m);
        if (!isGlobalMetric(metric)) {
            if (isQueryMetric(metric)) {
                resize(metric, query);
            } else {
                resize(metric, insertion);
            }
            reset(metric);
        }
    }   
}

void QPMonitor::resize(Metric metric, int size)
{
    _metrics[metric].clear();
    _metrics[metric].resize(size);
    _metrics_before_reset[metric].clear();
    _metrics_before_reset[metric].resize(size);
}

bool QPMonitor::isQueryMetric(Metric metric)
{
    if(find(_query_metrics.begin(), _query_metrics.end(), metric) != _query_metrics.end()) {
        return true;
    }
    return false;
}

bool QPMonitor::isGlobalMetric(Metric metric)
{
    if(find(_global_metrics.begin(), _global_metrics.end(), metric) != _global_metrics.end()) {
        return true;
    }
    return false;
}

bool QPMonitor::isThroughputMetric(Metric metric)
{
    if(find(_throughput_metrics.begin(), _throughput_metrics.end(), metric) != _throughput_metrics.end()) {
        return true;
    }
    return false;
}

bool QPMonitor::isSignalMetric(Metric metric)
{
    if(find(_signal_metrics.begin(), _signal_metrics.end(), metric) != _signal_metrics.end()) {
        return true;
    }
    return false;
}

bool QPMonitor::isStaticMetric(Metric metric)
{
    if(find(_static_metrics.begin(), _static_metrics.end(), metric) != _static_metrics.end()) {
        return true;
    }
    return false;
}

void QPMonitor::print()
{
    for (int m = QPMonitor::Metric::__begin + 1; m != QPMonitor::Metric::__end; m++)
    {
        QPMonitor::Metric metric = static_cast<QPMonitor::Metric>(m);
        cout << get(metric) << "\t";
    }
    cout << endl;
}

double QPMonitor::computeMetric(Metric metric, MetricMap metric_map)
{
    double value = 0.0;
    double ac = 0.0;
    double qt = 0;
    bool cancel = false;
    for (int s = 0; s < metric_map[metric].size(); s++)
    {
        if (metric_map[metric][s].second == 0.0) {
            cancel = true;
            break;
        }
        ac += metric_map[metric][s].first;
        qt += metric_map[metric][s].second;
    }

    if (cancel) {
        return 0.0;
    }

    value = (ac / qt);

    if (isThroughputMetric(metric)) 
    {
        value = 1.0 / value;
    }
    return value;
}

double QPMonitor::get(Metric metric)
{
    if (isStaticMetric(metric)) {
        return _metrics[metric][0].first;
    }

    return computeMetric(metric, _metrics);
}

double QPMonitor::getCurrent(Metric metric)
{
    if (isStaticMetric(metric)) {
        return _metrics[metric][0].first;
    }

    double value = computeMetric(metric, _metrics);
    if (value == 0.0) {
        value = computeMetric(metric, _metrics_before_reset);
    }
    return value;
}

double QPMonitor::getOld(Metric metric)
{
    return computeMetric(metric, _metrics_before_reset);
}

void QPMonitor::set(Metric metric, double value)
{
    if (isStaticMetric(metric)) {
        _metrics[metric][0].first = value;
    }
}

void QPMonitor::add(Metric metric, double value)
{
    int tid = 0;
    _metrics[metric][0].first    += value;
    _metrics[metric][0].second   += 1;
}

void QPMonitor::add(Metric metric, double value, int tid)
{
    _metrics[metric][tid].first    += value;
    _metrics[metric][tid].second   += 1;
}

void QPMonitor::reset(Metric metric)
{
    for (int s = 0; s < _metrics[metric].size(); s++) {
        if(_metrics[metric][s].second != 0) {
            _metrics_before_reset[metric][s] = _metrics[metric][s];
            _metrics[metric][s] = make_pair(0.0, 0);
        }
    }
    if (isSignalMetric(metric)) {
        _metrics_trend[metric] = 0;
    }
}

void QPMonitor::resetAll()
{
    for (int m = QPMonitor::Metric::__begin + 1; m != QPMonitor::Metric::__end; m++)
    {
        QPMonitor::Metric metric = static_cast<QPMonitor::Metric>(m);
        reset(metric);
    }
}

void QPMonitor::final(Metric metric)
{
    for (int s = 0; s < _metrics[metric].size(); s++) {
        _metrics[metric][s] = make_pair(0.0, 0);
        _metrics_before_reset[metric][s] = make_pair(0.0, 0);
    }
}

void QPMonitor::log(const char* file)
{
    // Saving log
    std::ofstream outfile;
    outfile.open(file, std::ios_base::app);
    for (int m = QPMonitor::Metric::__begin + 1; m != QPMonitor::Metric::__end; m++)
    {
        QPMonitor::Metric metric = static_cast<QPMonitor::Metric>(m);
        outfile << getCurrent(metric) << "\t";
        if (isSignalMetric(metric)) {
            outfile << trend(metric) << "\t";
        }
    }
    outfile << endl;
    outfile.close();
}

void QPMonitor::setTrend(Metric metric, int trend)
{
    _metrics_trend[metric] = trend;
}

int QPMonitor::trend(Metric metric)
{
    return _metrics_trend[metric];
}