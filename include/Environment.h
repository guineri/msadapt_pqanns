//
// Created by gnandrade on 03/04/2020.
//

#ifndef PQNNS_MULTI_STREAM_ENVIRONMENT_H
#define PQNNS_MULTI_STREAM_ENVIRONMENT_H

#include "comm/MpiCommunicator.h"
#include "utils/Logger.h"
#include "utils/Config.h"
#include "utils/Data.h"
#include "workers/Coordinator.h"
#include "workers/IndexStreamer.h"
#include "workers/QueryProcessor.h"
#include "workers/QueryStreamer.h"

class Environment {
public:
    Environment(Config conf) {
        this->conf = conf;
    }

    void config();
    void startUp(int argc, char* argv[]);
    void terminate();

private:
    Logger* logger = Logger::getInstance();
    vector<int> coordinatorsId;
    vector<int> queryProcessorId;
    vector<int> queryStreamerId;
    vector<int> indexStreamerId;

    int nCoordinatorWorkers{};
    int nQueryStreamerWorkers{};
    int nQueryProcessorWorkers{};
    int nIndexStreamerWorkers{};

    vector<Worker*> workers;
    Worker* getWorker(int index);
    Config conf{};

    static void saveMetric(Worker *w, const vector<string>& metrics, const char * type);
};

#endif // PQNNS_MULTI_STREAM_ENVIRONMENT_H
