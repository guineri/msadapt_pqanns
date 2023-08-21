//
// Created by gnandrade on 25/04/2021.
//

#ifndef PQNNS_MULTI_STREAM_SCSIGNALS_H
#define PQNNS_MULTI_STREAM_SCSIGNALS_H

#include <iostream>
#include "controller/QPMonitor.h"

using namespace std;

class SCSignals {
public:
    int     QARtrending();
    int     IARtrending();
    int     QQStrending();
    int     BORtrending();
    void    setup(QPMonitor *monitor);

private:
    QPMonitor *monitor;
    double QARbefore;
    double QQSbefore;
    double IARbefore;
    double BORbefore;

    double QAROffset = 0.50; // 50%
    double IAROffset = 0.50; // 50%
    double QQSOffset = 1.0; // One task
    double BOROffset = 1.0; // 0.5s

};

#endif //PQNNS_MULTI_STREAM_SCSIGNALS_H