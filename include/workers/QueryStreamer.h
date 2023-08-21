//
// Created by gnandrade on 03/04/2020.
//

#ifndef PQNNS_WS_QUERYSTREAMER_H
#define PQNNS_WS_QUERYSTREAMER_H

#include "Worker.h"
#include "utils/Data.h"
#include "reader/Reader.h"

class QueryStreamer : public Worker {
public:
    QueryStreamer();
    void run() override;
    void datasetDistribution() override;

private:
    int roundRobinController;
    Data *dataset;

    int nextCoordinatorRR();
};

#endif // PQNNS_WS_QUERYSTREAMER_H
