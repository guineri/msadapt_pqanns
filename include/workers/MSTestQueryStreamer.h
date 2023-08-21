//
// Created by gnandrade on 17/03/2021.
//

#ifndef PQNNS_MULTI_STREAM_MSTESTQUERYSTREAMER_H
#define PQNNS_MULTI_STREAM_MSTESTQUERYSTREAMER_H

#include <cstdio>
#include <iostream>
#include <chrono>
#include "Worker.h"

using namespace std;

class MSTestQueryStreamer {
public:
    void static run(Worker *w);
};


#endif //PQNNS_MULTI_STREAM_MSTESTQUERYSTREAMER_H
