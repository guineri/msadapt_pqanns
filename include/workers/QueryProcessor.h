//
// Created by gnandrade on 03/04/2020.
//

#ifndef PQNNS_WS_QUERYPROCESSOR_H
#define PQNNS_WS_QUERYPROCESSOR_H

#include "Worker.h"
#include "reader/Reader.h"
#include "workers/MSTestQueryStreamer.h"
#include "workers/MSTestInsertStreamer.h"
#include "controller/StreamController.h"
#include "temporal/TemporalBuffer.h"
#include "controller/QPMonitor.h"

#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFPQ.h>
#include <list>
#include <thread>
#include <mutex>
#include <boost/thread/barrier.hpp>

class QueryProcessor : public Worker {
public:
    QueryProcessor();
    void datasetDistribution() override;
    void run() override;

    vector<float> queryStreamRate;
    float queryStreamChangeInterval;

    vector<float> insertStreamRate;
    float insertStreamChangeInterval;

    mutex querySummaryProfileMtx;
    long processedQuery;
    double queryProcessExecTime;
    double queryUpdatedCheckTime;
    double queryExecTime;
    double queryResponseTime;
    double queryQueueTime;
    int queryOuterFinal;

    mutex insertSummaryProfileMtx;
    long processedInsert;
    long processedInsertByQuery;
    double insertProcessExecTime;
    double insertExecTime;
    double insertResponseTime;
    double insertQueueTime;
    int insertOuterFinal;

    void profileTimeSummaryClear();
    void profileTimeSummary(int currentConfigInsertOuter, int currentConfigQueryOuter);
    void startup();

//private:
    // Packing
    Buffer queryBufferPack = Buffer(MSG_TAG_QUERY, -1); // We dont need packId here
    Buffer indexBufferPack = Buffer(MSG_TAG_INDEX, -1); // We dont need packId here

    // Threads
    TemporalBuffer temporalInsertionsBuffer;

    mutex queriesListControl;
    list<Buffer> queriesToProcessBufferList;

    // Stream Controller
    bool begin;
    boost::barrier *wait_all;
    QPMonitor monitor;
    bool queryStreamerCanStart;
    bool insertStreamerCanStart;

    std::atomic<bool> updatedCheckStart;
    std::atomic<bool> streamControllerStop;
    vector<pair<int, int>> queryOuterThreadsConfig;
    long insertOuterThreadsConfig;

    // Producer Thread
    void Receiver();

    // Consumer Thread
    void InsertProcessorThread(int iOuterId);
        void AddToIndex(Buffer task);

    // Consumer Thread
    void QueryProcessorThread(int qOuterId);
        void UpdatedBucketCheck(Buffer task);
            void InsertPendingThread(const vector<long>& bucketsId);
        void Search(Buffer task);

    void ReceiveNB();

    // Strategies Implementation
    void IVFADCSearch(Buffer task);
    void IVFADCAddIndex(Buffer task);

    // Packing
    bool queryPacking(vector<float> data, long id, vector<long> nCentIdx, vector<float> nCentDist, int from);
    bool indexPacking(vector<float> data, long id, vector<long> nCentIdx, vector<float> nCentDist, int from);

    std::atomic<bool> queryProcessorCanFinalize;
    std::atomic<bool> insertProcessorCanFinalize;

};

#endif // PQNNS_WS_QUERYPROCESSOR_H
