#include <utility>

//
// Created by gnandrade on 15/04/2020.
//

#ifndef PQNNS_MULTI_STREAM_BUFFER_H
#define PQNNS_MULTI_STREAM_BUFFER_H

class Buffer {
public:
    // Use to control receiver loop
    Buffer() {
        this->null = true;
        this->innerThreads = make_pair(1, 0);
    };

    // Used to save Finalize Message
    Buffer(int t) {
        this->tag = t;
        this->null = false;
        this->innerThreads = make_pair(1, 0);
    };

    // Used in Coordinator
    Buffer(int tag_f, long pId) {
        this->tag = tag_f;
        this->null = false;
        this->packCount = 0;
        this->packId = pId;
        this->dim = -1;
        this->innerThreads = make_pair(1, 0);
    }

    // Used in Coordinator to pack arriving stream
    Buffer(vector<float> data, long id, int tag, int cFrom, int d) {
        this->values = std::move(data);
        this->idx.push_back(id);
        this->packId = id;
        this->tag = tag;
        this->packFrom.push_back(cFrom);
        this->dim = d;
        this->null = false;
        this->packCount = values.size() / d;
        this->innerThreads = make_pair(1, 0);
    };

    // Used in QueryProcessors to packages from Coordinator
    Buffer(vector<float> data, long id, int count, int tag, int cFrom, int dim) {
        this->values = std::move(data);
        this->packId = id;
        this->tag = tag;
        this->coordFrom = cFrom;
        this->dim = dim;
        this->null = false;
        this->packCount = values.size() / dim;
        this->innerThreads = make_pair(1, 0);
    };

    // Used in QueryProcessors to packages from Coordinator
    Buffer(vector<float> data, vector<long> i, long id, int tag, int cFrom, int dim) {
        this->values = std::move(data);
        this->idx = std::move(i);
        this->packId = id;
        this->tag = tag;
        this->coordFrom = cFrom;
        this->dim = dim;
        this->null = false;
        this->packCount = idx.size();
        this->innerThreads = make_pair(1, 0);
    };

    // With id
    void pack(vector<float> val, long id, vector<long> nCentI, vector<float> nCentD, int from_f, int d) {
        if (this->dim == -1) {
            this->dim = d;
        }
        this->values.insert(this->values.end(), val.begin(), val.end());
        this->nCentIdx.insert(this->nCentIdx.end(), nCentI.begin(), nCentI.end());
        this->nCentDist.insert(this->nCentDist.end(), nCentD.begin(), nCentD.end());
        this->idx.push_back(id);
        this->packFrom.push_back(from_f);
        this->packCount = this->values.size() / d;
    }

    void setNCentIdx(vector<long> idx) {
        this->nCentIdx = std::move(idx);
    }

    void setNCentDist(vector<float> dist) {
        this->nCentDist = std::move(dist);
    }

    long* getNCentIdx() {
        return this->nCentIdx.data();
    }

    float* getNCentDist() {
        return this->nCentDist.data();
    }

    bool isNull() {
        return this->null;
    }

    int getTag() {
        return this->tag;
    }

    int getCoordFrom() {
        return this->coordFrom;
    }

    long getPackId() {
        return this->packId;
    }

    int getPackSize() {
        return this->packCount;
    }

    vector<int> getPackFrom() {
        return this->packFrom;
    }

    // Pack ToSend with id:
    // |--PACKID--|--VALUES--|--nCentIdx--|--nCentDist--|
    vector<float> getPackDataToSend() {
        this->values.insert(this->values.begin(), this->packId);
        this->values.insert(this->values.end(), this->nCentIdx.begin(),
                this->nCentIdx.end());
        this->values.insert(this->values.end(), this->nCentDist.begin(),
                            this->nCentDist.end());
        return this->values;
    }

    int getPackDataBufferSize()
    {
        // |--PACKID--|--VALUES--|--nCentIdx--|--nCentDist--|
        return 1 + this->values.size() + this->nCentIdx.size() + this->nCentDist.size();
    }

    bool hasVecIdx() {
        return !this->idx.empty();
    }

    long* getVecsIdx() {
        return this->idx.data();
    }

    float* getPackToProcess() {
        return this->values.data();
    }

    void printPack() {
        Data::printMatrix(this->values.data(), getPackSize(), 3);
    }

    void setQueuedTime() {
        this->queuedTime = Time::getCurrentTime();
    }

    void setDequeuedTime() {
        this->dequeuedTime = Time::getCurrentTime();
    }

    void setFinalizedProcess() {
        this->finalizedProcess = Time::getCurrentTime();
    }

    double getResponseTime() {
        return this->finalizedProcess - this->queuedTime;
    }

    double getQueueTime() {
        return this->dequeuedTime - this->queuedTime;
    }

    double getWhenTaskWasQueued() {
        return this->queuedTime;
    }

    void setNInnerThreads( pair<int, int> inner) {
        this->innerThreads = inner;
    }

    pair<int, int> getNInnerThreads() {
        return this->innerThreads;
    }


private:
    long packId;
    int packCount;
    int tag;
    int dim;
    int coordFrom;
    bool null;
    double queuedTime;
    double dequeuedTime;
    double finalizedProcess;

    pair<int, int> innerThreads;

    vector<int> packFrom;

    vector<long> idx;
    vector<float> values;
    vector<long> nCentIdx;
    vector<float> nCentDist;
};

#endif //PQNNS_MULTI_STREAM_BUFFER_H
