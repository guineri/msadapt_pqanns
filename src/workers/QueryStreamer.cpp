//
// Created by gnandrade on 03/04/2020.
//

#include "workers/QueryStreamer.h"

QueryStreamer::QueryStreamer()
{
    this->setSource(QUERY_STREAMER);
    this->roundRobinController = 0;
}

void QueryStreamer::datasetDistribution()
{
    if (!config.READ_FROM_IVF_FILE) {
        // This streamer will also read nb
        if (find(config.READERS.begin(), config.READERS.end(), this->id) != config.READERS.end()) {
            Reader::ReadTrainIndexFromFile(*this);
            // Now, this coordinator will operator as a reader!
            Reader reader(*this);
            reader.read();
        }
    }
}

// TODO create a receive thread
void QueryStreamer::run()
{
    // 1 - Generate Random query vectors
    this->dataset = new Data(config.QUERY_FILE_PATH.data(), config.DIM);

    int streamed = 0;
    float* prVec = this->dataset->getNextBatch(this->config.STREAM_PACKAGE);
    long* queryVecIdx = new long[this->config.STREAM_PACKAGE];
    int lastIdx = 0; // the last id inserted

    double loadFactor = this->config.LOAD_FACTOR_QUERY;

    double startStreamTime = Time::getCurrentTime();
    //while(Time::getCurrentTime() - startStreamTime < this->config.STREAM_TIME) {
    while(streamed < this->config.QUERY_STREAM_SIZE) {
        // 1.2 Get idx
        // TODO get next index global when have more than one index streamer
        for (int i = 0; i < config.STREAM_PACKAGE; i++) {
            queryVecIdx[i] = lastIdx + i;
        }
        lastIdx += config.STREAM_PACKAGE;

        // LoadFactor StreamRate Controll
        // long loadFactorInterval = Time::streamRateControlPoisson(loadFactor, this->config.QUERY_EXEC_TIME);
        //std::this_thread::sleep_for (std::chrono::microseconds (loadFactorInterval));

        //cout << "QS SENDED [" << streamed << "]: " << prVec[0] << " " << prVec[1] << " ... " << prVec[(config.DIM-2)] << " " << prVec[(config.DIM-1)] << endl;
        // 2 - Send query data to Coordinator
        int nextTargetCoordinator = nextCoordinatorRR();
        this->MPIComm.sendQueryStream(prVec, queryVecIdx, nextTargetCoordinator, this->STREAM_COMM);

        // 3 - Wait Coordinator response
        //long* knnVec = new long[this->config.QUERY_RESULT_BUFFER_SIZE];
        //this->MPIComm.receiveQueryResult(knnVec, nextTargetCoordinator, this->PROCESS_COMM);
        //Data::printMatrix(knnVec, this->config.STREAM_PACKAGE, this->config.K);

        // 4 - Get more vectors to stream.
        prVec = this->dataset->getNextBatch(this->config.STREAM_PACKAGE);
        streamed += 1;

        //if (streamed == this->config.QUERY_STREAM_SIZE) {
        //    this->dataset = new Data(this->config.QUERY_FILE_PATH.data(), this->config.DIM);
        //    streamed = 0;
        //}
    }

    // Terminate (Send finalize message to all coordinators)
    MPICommunicator::sendStreamFinalizeMessage(this->coordinatorsId, this->STREAM_COMM);

    delete [] queryVecIdx;
    this->dataset->~Data();
}

int QueryStreamer::nextCoordinatorRR()
{
    int nextCoordinator = this->coordinatorsId.at(roundRobinController);

    roundRobinController += 1;
    if (roundRobinController >= this->coordinatorsId.size()) {
        roundRobinController = 0;
    }

    return nextCoordinator;
}

