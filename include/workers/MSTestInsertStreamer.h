//
// Created by gnandrade on 20/03/2021.
//

#ifndef PQNNS_MULTI_STREAM_MSTESTINSERTSTREAMER_H
#define PQNNS_MULTI_STREAM_MSTESTINSERTSTREAMER_H

#include <cstdio>
#include <iostream>
#include <chrono>
#include "Worker.h"

using namespace std;

class MSTestInsertStreamer {
public:
    void static run(Worker *w);
};


#endif //PQNNS_MULTI_STREAM_MSTESTINSERTSTREAMER_H
