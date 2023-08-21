//
// Created by gnandrade on 14/04/2021.
//

#include "temporal/TemporalBuffer.h"
#include "workers/QueryProcessor.h"

void TemporalBuffer::setup(Worker *w)
{
    this->w = w;
    this->hasAccumulatedData = false;
    this->flexibilization = w->config.BUCKETOUTDATEDFLEX;
    this->insertionsTasksPerBucketMtx.clear();
    this->insertionsToProcessBufferList.clear();
    
    this->config = w->config;
    for (int i = 0; i < config.NCENTROIDS; i++) {
        this->insertionsTasksPerBucketMtx.push_back(new mutex());
        // HERE! 
        //this->insertionsTasksPerBucketCnd.push_back(new condition_variable());
        this->insertionsTasksPerBucket[i].clear();
    }
}

void TemporalBuffer::setAccumulatedData()
{
    if (this->insertionsToProcessBufferList.size() > 2) {
        this->hasAccumulatedData = true;
    } else {
        if (this->insertionsToProcessBufferList.size() <= 1) {
            this->hasAccumulatedData = false;
        }
    }
}

bool TemporalBuffer::getHasAccumulatedData() {
    return this->hasAccumulatedData;
}

long TemporalBuffer::getSize() {
    return this->insertionsToProcessBufferList.size();
}

float TemporalBuffer::getFlexibility()
{
    return this->flexibilization;
}

double TemporalBuffer::getOutdatedRatio()
{
    double notEmpty = 0.0;
    double outdatedTotal = 0.0;
    double max = 0.0;
    for (int i = 0; i < this->config.NCENTROIDS; i++) {
        if (!this->insertionsTasksPerBucket.at(i).empty()){
            double nextQueueTime = this->insertionsTasksPerBucket.at(i).front().getWhenTaskWasQueued();
            double bucketOutdated = Time::getCurrentTime() - nextQueueTime;
            if (bucketOutdated <= this->outdatedMax + 1.0) {
                outdatedTotal += bucketOutdated;
                notEmpty += 1.0;
                if (bucketOutdated > max) {
                    max = bucketOutdated;
                }
            }
            
        }
    }

    if (max != 0.0) {
        this->outdatedMax = max;
    }
    
    if (notEmpty != 0.0) {
        return outdatedTotal / notEmpty;
    }
    return 0.0;
}

double TemporalBuffer::getOutdatedMax() {
    return this->outdatedMax;
}

void TemporalBuffer::addTask(Buffer task, long centroid)
{
    {
        std::lock_guard<std::mutex> lock(this->insertionsToProcessBufferListMtx);
        {
            std::lock_guard<std::mutex> lock(*this->insertionsTasksPerBucketMtx[centroid]);
            task.setQueuedTime();
            this->insertionsTasksPerBucket.at(centroid).emplace_back(move(task));
            setAccumulatedData();
        }
        this->insertionsToProcessBufferList.push_back(centroid);
    }
}

Buffer TemporalBuffer::getTask()
{
    Buffer task;
    {
        std::lock_guard<std::mutex> lock(this->insertionsToProcessBufferListMtx);

        if (!this->insertionsToProcessBufferList.empty()) {
            long centId = this->insertionsToProcessBufferList.front();
            if (centId != MSG_TAG_STREAM_FINALIZE) {
                {
                    std::lock_guard<std::mutex> lock(*this->insertionsTasksPerBucketMtx[centId]);

                    if (!this->insertionsTasksPerBucket.at(centId).empty()) {
                        task = this->insertionsTasksPerBucket.at(centId).front();
                        this->insertionsTasksPerBucket.at(centId).pop_front();
                        task.setDequeuedTime();

                        /* Condition Variale */
                        //if (this->insertionsTasksPerBucket.at(centId).empty() ||
                        //    Time::getCurrentTime() - this->insertionsTasksPerBucket.at(centId).front().getWhenTaskWasQueued() <= this->flexibilization) {
                            // Bucket updated release lock
                            // HERE! 
                        //    insertionsTasksPerBucketCnd[centId]->notify_all();
                        //}

                        this->insertionsToProcessBufferList.pop_front();
                        setAccumulatedData();
                    }
                }
            } else {
                task = Buffer(MSG_TAG_STREAM_FINALIZE);
            }
        }
    }
    return task;
}

list<Buffer> TemporalBuffer::getAllFromBucket(long centroid)
{
    list<Buffer> toInsert;
    {
        std::lock_guard<std::mutex> lock(this->insertionsToProcessBufferListMtx);

        {
            std::lock_guard<std::mutex> lock(*this->insertionsTasksPerBucketMtx[centroid]);

            if (!this->insertionsTasksPerBucket[centroid].empty()) {
                toInsert = move(this->insertionsTasksPerBucket[centroid]);
                this->insertionsTasksPerBucket[centroid].clear();
                this->insertionsToProcessBufferList.remove(centroid);
                setAccumulatedData();
            }

        }
    }
    return toInsert;
}

bool TemporalBuffer::bucketIsUpdated(long centroid)
{
    if (!this->insertionsTasksPerBucket.at(centroid).empty()) {
        double nextQueueTime = this->insertionsTasksPerBucket.at(centroid).front().getWhenTaskWasQueued();
        if (Time::getCurrentTime() - nextQueueTime > this->flexibilization) {
            return false;
        }
    }
    return true;
}

void TemporalBuffer::waitUntilBucketIsUpdated(long centroid)
{
    /* Condition Variale */
    // HERE! 
    //std::unique_lock<std::mutex> lock(*this->insertionsTasksPerBucketMtx[centroid]);
    //insertionsTasksPerBucketCnd[centroid]->wait(lock, std::bind(&TemporalBuffer::bucketIsUpdated, this, centroid));

    /* Pooling */
    auto *qp = dynamic_cast<QueryProcessor *>(this->w);
    while(!qp->streamControllerStop && !bucketIsUpdated(centroid)) {}
}

bool TemporalBuffer::hasPendingTask(long centroid)
{
    /* With lock guard
    bool updated = false;
    {
        std::lock_guard<std::mutex> lock(*this->insertionsTasksPerBucketMtx[centroid]);
        updated = bucketIsUpdated(centroid);
    }
    return !updated;*/

    /* Free for all! */
    return !bucketIsUpdated(centroid);
}

void TemporalBuffer::finalize()
{
    {
        std::lock_guard<std::mutex> lock(this->insertionsToProcessBufferListMtx);
        this->insertionsToProcessBufferList.push_back(MSG_TAG_STREAM_FINALIZE);
    }
}