//
// Created by gnandrade on 03/04/2020.
//

#include "workers/IndexStreamer.h"

IndexStreamer::IndexStreamer()
{
    this->setSource(INDEX_STREAMER);
    this->roundRobinController = 0;
}

void IndexStreamer::datasetDistribution()
{
    Reader::ReadTrainIndexFromFile(*this);
    Reader reader(*this);
    reader.read();
}

void IndexStreamer::run()
{
    // 1 - Generate Random query vectors
    int n = this->config.INDEX_PACKAGE;
    Data *dataset = new Data(this->config.BASE_FILE_PATH.data(), this->config.DIM);

    int streamed = 0;
    float* indexVec = dataset->getNextBatch(n);
    long lastIdx = this->config.NB + 1;
    double loadFactor = this->config.LOAD_FACTOR_INSERT;

    long* indexVecIdx = new long[n];

    double startStreamTime = Time::getCurrentTime();
    while(Time::getCurrentTime() - startStreamTime < this->config.STREAM_TIME) {
        // 1.2 Get idx
        // TODO get next index global when have more than one index streamer
        for (int i = 0; i < n; i++) {
            indexVecIdx[i] = lastIdx + i;
        }
        lastIdx += n;

        // 2 - Send Index data to Closests Query Processors
        // Get closest centroids
        vector<long> nCentIdx;
        vector<float> nCentDist;
        vector<float> data;
        nCentIdx.resize(n);
        nCentDist.resize(n);
        data.resize(n * this->config.DIM);

        copy(indexVec, indexVec + (n * this->config.DIM), data.begin());

        // Create Task 
        Buffer *task = new Buffer(data, indexVecIdx[0], MSG_TAG_INDEX, -2, this->config.DIM);

        // Search closest centroids if Strategy is not DES
        this->coarse_centroids->search(n, &data[0], 1, &nCentDist[0], &nCentIdx[0]);
        task->setNCentIdx(nCentIdx);
        task->setNCentDist(nCentDist);

        int nearestCentroid = nCentIdx[0];

        // Get QP will receive Index
        int queryProcessorID = 0;
        if (this->config.DB_DISTRIBUTION_STRATEGY == DES_STRATEGY) {
            // Get Round Robin
            queryProcessorID = this->nextQueryPrcessorRR();
        } else {
            int targetRegion = this->getRegionByCentroid(nearestCentroid);
            queryProcessorID = this->queryProcessorsId.at(targetRegion);
        }
        
        // Send Data
        MPICommunicator::sendBufferToProcess(*task, queryProcessorID, this->PROCESS_COMM);

        // LoadFactor StreamRate Control
        long loadFactorInterval = Time::streamRateControlPoisson(loadFactor, this->config.INSERTION_EXEC_TIME);
        std::this_thread::sleep_for (std::chrono::microseconds (loadFactorInterval));

        // 3 - Get more vectors to stream.
        indexVec = dataset->getNextBatch(n);
        streamed += 1;
    }

    // Terminate (Send finalize message to all coordinators)
    MPICommunicator::sendStreamFinalizeMessage(this->queryProcessorsId, this->PROCESS_COMM);
    
    delete [] indexVec;
    delete [] indexVecIdx;
}

int IndexStreamer::nextQueryPrcessorRR()
{
    int nextQP = this->queryProcessorsId.at(roundRobinController);

    roundRobinController += 1;
    if (roundRobinController >= this->queryProcessorsId.size()) {
        roundRobinController = 0;
    }

    return nextQP;
}
