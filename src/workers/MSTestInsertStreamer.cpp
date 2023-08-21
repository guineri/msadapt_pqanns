//
// Created by gnandrade on 20/03/2021.
//

#include "workers/MSTestInsertStreamer.h"
#include "workers/QueryProcessor.h"

void MSTestInsertStreamer::run(Worker *w)
{
    int n = w->config.INDEX_PACKAGE;
    //Cast worker to QueryProcessor
    QueryProcessor *qp = dynamic_cast<QueryProcessor *>(w);

    // 1 - Generate Random query vectors
    Data *dataset = new Data(w->config.BASE_FILE_PATH.data(), w->config.DIM);

    int streamed = 0;
    float* prVec = dataset->getNextBatch(n);
    long lastIdx = w->config.NB + 1; // the last id inserted was the last datasetID
    qp->indexBufferPack = Buffer(MSG_TAG_INDEX, lastIdx);

    double oldTime = 0.0;
    double streamInterval = 0.0;
    int currentStreamRate = 0;
    double loadFactor = qp->insertStreamRate[currentStreamRate];

    qp->wait_all->count_down_and_wait(); //AQUI!!
    
    double startStreamTime = Time::getCurrentTime();
    while(Time::getCurrentTime() - startStreamTime < qp->config.STREAM_TIME) {
        // Get closest centroids
        vector<long> nCentIdx;
        vector<float> nCentDist;
        vector<float> data;
        nCentIdx.resize(n);
        nCentDist.resize(n);
        data.resize(n * w->config.DIM);

        copy(prVec, prVec + (n * w->config.DIM), data.begin());

        w->temporalIndex.nearestBuckets(n, &data[0], &nCentIdx[0], &nCentDist[0], 1);

        // LoadFactor StreamRate Control
        long loadFactorInterval = Time::streamRateControlPoisson(loadFactor, qp->config.INSERTION_EXEC_TIME);
        std::this_thread::sleep_for (std::chrono::microseconds (loadFactorInterval));

        double timeNew = Time::getCurrentTime();
        qp->indexPacking(data, lastIdx, nCentIdx, nCentDist, -1);
        qp->temporalInsertionsBuffer.addTask(qp->indexBufferPack, nCentIdx[0]);
        qp->indexBufferPack = Buffer(MSG_TAG_INDEX, lastIdx);

        lastIdx += n;
        // Clean index buffer pack

        if (oldTime != 0.0) {
            qp->monitor.add(QPMonitor::Metric::IAR, timeNew - oldTime);
            streamInterval += timeNew - oldTime;
        }
        oldTime = timeNew;

        if (streamInterval >= qp->insertStreamChangeInterval) {
            currentStreamRate += 1;
            if (currentStreamRate == qp->insertStreamRate.size()) {
                currentStreamRate = 0;
            }
            loadFactor = qp->insertStreamRate[currentStreamRate];
            streamInterval = 0.0;
        }

        prVec = dataset->getNextBatch(n);
        streamed += 1;

        if (streamed == w->config.NB) {
            dataset = new Data(w->config.BASE_FILE_PATH.data(), w->config.DIM);
            streamed = 0;
        }
    }

    qp->temporalInsertionsBuffer.finalize();
    qp->monitor.final(QPMonitor::Metric::IAR);
}