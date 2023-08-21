//
// Created by gnandrade on 14/04/2021.
//

#ifndef PQNNS_MULTI_STREAM_TEMPORALBUFFER_H
#define PQNNS_MULTI_STREAM_TEMPORALBUFFER_H

#include "Worker.h"
#include <mutex>
#include <condition_variable>
#include <unordered_map>
#include <list>
#include <vector>

class TemporalBuffer {
public:
    TemporalBuffer() {};
    void setup(Worker *w);
    Buffer getTask();
    list<Buffer> getAllFromBucket(long centroid);
    void addTask(Buffer task, long centroid);
    bool hasPendingTask(long centroid);
    bool bucketIsUpdated(long centroid);
    void waitUntilBucketIsUpdated(long centroid);
    void finalize();
    bool getHasAccumulatedData();
    long getSize();
    double getOutdatedRatio();
    double getOutdatedMax();
    void resetUpdatedBuffer();
    float getFlexibility();

private:
    Worker *w;
    Config config;
    vector<mutex * > insertionsTasksPerBucketMtx;
    // HERE! 
    //vector<condition_variable * > insertionsTasksPerBucketCnd;
    unordered_map<long,list<Buffer>> insertionsTasksPerBucket;
    mutex insertionsToProcessBufferListMtx;
    list<long> insertionsToProcessBufferList;
    bool hasAccumulatedData;
    void setAccumulatedData();
    float flexibilization;
    double outdatedMax;
};

#endif //PQNNS_MULTI_STREAM_TEMPORALBUFFER_H
