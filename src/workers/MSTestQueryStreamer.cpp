//
// Created by gnandrade on 17/03/2021.
//

#include "workers/MSTestQueryStreamer.h"
#include "workers/QueryProcessor.h"

void MSTestQueryStreamer::run(Worker *w)
{
    //Cast worker to QueryProcessor
    auto *qp = dynamic_cast<QueryProcessor *>(w);

    // 1 - Generate Random query vectors
    Data *dataset = new Data(w->config.QUERY_FILE_PATH.data(), w->config.DIM);

    int streamed = 0;
    float* prVec = dataset->getNextBatch(w->config.STREAM_PACKAGE);
    long* queryVecIdx = new long[w->config.STREAM_PACKAGE];
    int lastIdx = 0; // the last id inserted
    qp->queryBufferPack = Buffer(MSG_TAG_QUERY, lastIdx);

    double oldTime = 0.0;
    double streamInterval = 0.0;
    int currentStreamRate = 0;
    double loadFactor = qp->queryStreamRate[currentStreamRate];

    qp->wait_all->count_down_and_wait(); //AQUI!!

    double startStreamTime = Time::getCurrentTime();
    while(Time::getCurrentTime() - startStreamTime < qp->config.STREAM_TIME) {

        lastIdx += w->config.STREAM_PACKAGE;
        for (int i = 0; i < w->config.STREAM_PACKAGE; i++) {
            queryVecIdx[i] = lastIdx + i;
        }

        // Get w closests centroids
        int n = 1;
        vector<long> nCentIdx;
        vector<float> nCentDist;
        nCentIdx.resize(n * w->config.NPROB);
        nCentDist.resize(n * w->config.NPROB);

        vector<float> data;
        data.resize(n * w->config.DIM);
        copy(prVec, prVec + (n * w->config.DIM), data.begin());

        qp->temporalIndex.nearestBuckets(n, &data[0], &nCentIdx[0], &nCentDist[0], w->config.NPROB);

        // Pack and save to buffer
        bool full = qp->queryPacking(data, queryVecIdx[0], nCentIdx, nCentDist, -1);

        // LoadFactor StreamRate Controll
        long loadFactorInterval = Time::streamRateControlPoisson(loadFactor, qp->config.QUERY_EXEC_TIME);
        std::this_thread::sleep_for (std::chrono::microseconds (loadFactorInterval));

        // Dispatch Task
        if (full) {
            double timeNew = Time::getCurrentTime();
            //qp->queriesListControl.lock();
            qp->queryBufferPack.setQueuedTime();
            qp->queriesToProcessBufferList.emplace_back(move(qp->queryBufferPack));

            // Clean query buffer pack
            qp->queryBufferPack = Buffer(MSG_TAG_QUERY, queryVecIdx[0]);
            //qp->queriesListControl.unlock();

            if (oldTime != 0.0) {
                qp->monitor.add(QPMonitor::Metric::QAR, timeNew - oldTime);
                streamInterval += timeNew - oldTime;
            }
            oldTime = timeNew;

            if (streamInterval >= qp->queryStreamChangeInterval) {
                currentStreamRate += 1;
                if (currentStreamRate == qp->queryStreamRate.size()) {
                    currentStreamRate = 0;
                }
                loadFactor = qp->queryStreamRate[currentStreamRate];
                streamInterval = 0.0;
            }
        }

        //qp->queryPacking()
        prVec = dataset->getNextBatch(w->config.STREAM_PACKAGE);
        streamed += 1;

        if (streamed == w->config.QUERY_STREAM_SIZE) {
            dataset = new Data(w->config.QUERY_FILE_PATH.data(), w->config.DIM);
            streamed = 0;
        }
    }

    qp->queriesListControl.lock();
    qp->queriesToProcessBufferList.emplace_back(MSG_TAG_STREAM_FINALIZE);
    qp->queriesListControl.unlock();
    qp->monitor.final(QPMonitor::Metric::QAR);
}