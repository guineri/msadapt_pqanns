//
// Created by gnandrade on 03/04/2020.
//

#ifndef PQNNS_WS_COORDINATOR_H
#define PQNNS_WS_COORDINATOR_H

#include <algorithm>
#include <iostream>
#include <utility>
#include <vector>
#include <list>
#include <thread>
#include <mutex>

#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFPQ.h>

#include "Constants.h"
#include "Worker.h"
#include "comm/MpiCommunicator.h"
#include "utils/Data.h"
#include "reader/Reader.h"

using namespace std;

typedef vector<float> QPResponses;

class Coordinator : public Worker {
public:
    Coordinator();
    void run() override ;
    void train() override ;
    void datasetDistribution() override ;

    // Replication Metric
    int sendToSizeAvg;
    int processedQuery;

    double runTime;
        double recTotalTime;
            double mpiRecZippedResultWithId;
            double recMsgTotalTime;
            long recMsgTotalCount;
                double mergeTime;
                double recvTime;
        double procTotalTime;
        double streamRecTotalTime;

    double queryPerSec;

private:
    int rrRegionIt = 0;
    int queryProcessorIt = 0;
    int queryProcessorIndexBegin = 0;

    // Threads
    // Global Consumer/Producer List
    list<Buffer> streamBufferList;
    mutex threadControl;

    // Producer
    void StreamReceiver();

    // Consumer
    void Processor();
        void processIndex(const Buffer& task);
            int nextQueryProcessor();
        void processQuery(const Buffer& task);
            int nextRegion();

    // Response Receiver Thread
    // QueryResponseID, <Response Count waiting, MergeBuffer>
    map<long, pair<int, QPResponses>> responseControl;

    // In the sent package we can have vector from many sources
    map<long, vector<int> > whoStreamerNeedsResponse;

    mutex responseReceiveThreadControl;
    void ResponseReceiver();
    void Merge(QPResponses responses, long* answers);
    void sendResultBack(long *answers, long responseId);
    void saveResult(long *answers, long responseId);
    void cleanResponseIdData(long responseId);

    // Implemented Strategies
    void BroadCastProcessQuery(Buffer buffer);
    void RoundRobinProcessIndex(Buffer task);

    void IVFADCTrain();
    void CoarseCentroidTrainStep();
    void ClusterTrainStep();

    void IVFADCProcessQuery(const Buffer& task);
        void IVFADCProcessQueryByRegion(Buffer task);
        set<long> RegionSelectionHeuristic(set<long> nearestCentroidsRegions);
        set<long> RegionSelectionHeuristicSimpler(set<long> nearestCentroidsRegions);

    void IVFADCProcessIndex(const Buffer& task);
        void IVFADCProcessIndexByRegion(Buffer);
};

#endif // PQNNS_WS_COORDINATOR_H
