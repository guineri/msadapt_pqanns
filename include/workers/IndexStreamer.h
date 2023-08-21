//
// Created by gnandrade on 03/04/2020.
//

#ifndef PQNNS_WS_INDEXSTREAMER_H
#define PQNNS_WS_INDEXSTREAMER_H

#include "Worker.h"
#include "utils/Data.h"
#include "reader/Reader.h"

class IndexStreamer : public Worker {
public:
    IndexStreamer();
    void run() override;
    void datasetDistribution() override;

private:
    int roundRobinController;

    int nextQueryPrcessorRR();
};

#endif // PQNNS_WS_INDEXSTREAMER_H
