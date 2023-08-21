//
// Created by gnandrade on 03/04/2020.
//

#include "workers/Coordinator.h"

#include <utility>
#include <stdlib.h>

Coordinator::Coordinator()
{
    this->setSource(COORDINATOR);
    this->sendToSizeAvg = 0;
    this->mergeTime = 0.0;
    this->recvTime = 0.0;
    this->recMsgTotalCount = 0;
    this->recMsgTotalTime = 0.0;
    this->mpiRecZippedResultWithId = 0.0;
}

void Coordinator::train()
{
    // Just coordinator 0 will be train query processors and only with IVFADC index
    if (this->id == 0) {
        this->IVFADCTrain();

        if (config.GENERATENEW) {
            Reader reader(*this);

            if (config.CHECKPOINT >= 0) {
                reader.createNewCheckPoint();
            } else {
                reader.createNew();
            }
            exit(EXIT_SUCCESS);
        }
    }
}

void Coordinator::datasetDistribution()
{
    // Coordinator 0 already have train data. (Got it in train step)
    if (this->id != 0) {
        //Just coordinators and query processors need it
        Reader::ReadTrainIndexFromFile(*this);
    }

    Reader reader(*this);

    if (!config.READ_FROM_IVF_FILE) {
        // This query processor will also read nb
        if (find(config.READERS.begin(), config.READERS.end(), this->id) != config.READERS.end()) {
            // Now, this coordinator will operator as a reader!
            reader.read();
        }
    } else {
        // Coordinator will just create where each centroids will be
        reader.read();
    }
}

void Coordinator::run()
{
    double initTime = Time::getCurrentTime();
    std::thread streamReceiver (&Coordinator::StreamReceiver, this);    // Producer
    std::thread processor (&Coordinator::Processor, this);  // Consumer
    std::thread responseReceiver (&Coordinator::ResponseReceiver, this);  // Extern Consumer

    streamReceiver.join();
    processor.join();
    responseReceiver.join();
    this->runTime = Time::getCurrentTime() - initTime;
}

// This thread is responsible just to receive vectors from index/query streamers and
// put it in a global buffer structure. It works like a producer.
void Coordinator::StreamReceiver()
{
    double initTime = Time::getCurrentTime();
    int receivedQuery = 0;
    int receivedIndex = 0;
    int receivedFinalize = 0;
    bool receiverCanFinalize = false;

    while (!receiverCanFinalize) {
        // 1 - Receive new stream data (a index stream or a query stream,)
        // Index and Query streams need to be with IDXs
        vector<float> data;
        vector<long> idx;
        MPI_Status status = this->MPIComm.receiveStream(data, idx, this->STREAM_COMM);

        switch (status.MPI_TAG) {

            case MSG_TAG_QUERY : {
                this->threadControl.lock();
                this->streamBufferList.emplace_back(data, idx[0], MSG_TAG_QUERY, status.MPI_SOURCE, config.DIM);
                this->threadControl.unlock();
                receivedQuery += 1;
                break;
            };

            case MSG_TAG_STREAM_FINALIZE : {
                receivedFinalize += 1;

                // If all streamers ended, this thread no longer will receive data
                // so, it will be finished.
                // Also, it will indicate to consumer thread that tasks over
                if (receivedFinalize >= this->queryStreamersId.size()) {
                    receiverCanFinalize = true;

                    // Indicate to consumer thread that tasks over
                    this->threadControl.lock();
                    this->streamBufferList.emplace_back(MSG_TAG_STREAM_FINALIZE);
                    this->threadControl.unlock();
                }
            }
            default:
                break;
        }
    }

    this->streamRecTotalTime = Time::getCurrentTime() - initTime;
}

// This thread is responsible to get task from global list and process it
void Coordinator::Processor()
{
    double initTime = Time::getCurrentTime();
    this->processedQuery = 0;
    int processedIndex = 0;
    bool processorCanFinalize = false;

    while (!processorCanFinalize) {
        Buffer task;
        // Getting next task from global list
        // TODO COND/WAIT? Sleep?
        this->threadControl.lock();
        if (!this->streamBufferList.empty()) {
            task = move(this->streamBufferList.front());
            this->streamBufferList.pop_front();
        }
        this->threadControl.unlock();

        if (!task.isNull()) {
            switch (task.getTag()) {
                case MSG_TAG_QUERY: {
                    // Process Query
                    this->processQuery(task);

                    processedQuery += 1;
                    break;
                }

                case MSG_TAG_INDEX: {
                    // 1.2.1 - Send Index vectors to Query processors in a Round robin way inside a defined window!
                    this->processIndex(task);
                    processedIndex += 1;
                    break;
                }

                case MSG_TAG_STREAM_FINALIZE: {
                    //Consumer received a finalize tasks.
                    //There's no more task (index/query) to process.
                    processorCanFinalize = true;
                    break;
                }
                default:
                    break;
            }
        }
    }
    // There's no more stream
    // There's no more task in global list
    // We do not need more processors, so...
    // Send a finalize message to all query processors
    for (auto workerId : this->queryProcessorsId) {
        MPICommunicator::sendStreamFinalizeMessage(workerId, this->PROCESS_COMM);
    }

    this->procTotalTime = Time::getCurrentTime() - initTime;
}

// This thread will be responsible to receive only query responses
// Buffer and merge when necessary
// Query processors will broadcast finalize messages
void Coordinator::ResponseReceiver()
{
    double initTime = Time::getCurrentTime();
    int receiveResponseCount = 0;
    int receivedFinalize = 0;
    bool responseReceiverCanFinalize = false;

    while(!responseReceiverCanFinalize) {
        long responseId;
        int pack_size;
        // |id|count|idx|distances|
        double initMpiRec = Time::getCurrentTime();
        auto responseReceived = new float[2 * (config.QUERY_PACKAGE * config.K) + 2]; // TODO plus id and count!
        MPI_Status status = this->MPIComm.receiveZippedResultWithId(responseReceived, responseId, pack_size, this->PROCESS_COMM);
        this->mpiRecZippedResultWithId += Time::getCurrentTime() - initMpiRec;

        switch (status.MPI_TAG)
        {
            case MSG_TAG_QUERY_RESPONSE:
            {
                double iniRecMsgTime = Time::getCurrentTime();

                double initRecv = Time::getCurrentTime();
                this->responseControl[responseId].second.insert(
                        this->responseControl[responseId].second.end(),
                        responseReceived,
                        responseReceived + (2* config.K));

                // Count that received message
                this->responseControl[responseId].first -= 1;
                this->recvTime += Time::getCurrentTime() - initRecv;

                // If Received all responses
                // Its time to merge and send back
                if (this->responseControl[responseId].first == 0) {

                    long* answers = new long[config.K];
                    this->Merge(this->responseControl[responseId].second, answers);

                    printf("aqui");
                    if (config.SAVE_RESULT) {
                        this->saveResult(answers, responseId);
                        // TODO this->sendResultBack(respId, responseId);
                    }

                    // When send back result, we can remove id from global map
                    //this->cleanResponseIdData(responseId);

                    // Clean data
                    this->responseControl[responseId].second.clear();
                    this->responseControl[responseId].second.shrink_to_fit();
                    delete [] answers;
                }

                delete [] responseReceived;

                this->recMsgTotalCount += 1;
                this->recMsgTotalTime += Time::getCurrentTime() - iniRecMsgTime;
                break;
            }

            case MSG_TAG_RESPONSE_FINALIZE:
            {
                // In this case we need to receive a final confirmation of all query processors (from query an insert threads)
                receivedFinalize += 1;
                if (receivedFinalize == (this->queryProcessorsId.size())) {
                    responseReceiverCanFinalize = true;
                }

                break;
            }
        }

        receiveResponseCount += 1;
    }

    this->recTotalTime = Time::getCurrentTime() - initTime;
}

void Coordinator::sendResultBack(long* mergedMsg, long responseId)
{
    for (int i = 0; i < this->whoStreamerNeedsResponse[responseId].size(); i++) {
        //Data::printMatrix(mergedMsg + i*config.K, config.STREAM_PACKAGE, config.K);
        int streamer = this->whoStreamerNeedsResponse[responseId].at(i);
        this->MPIComm.sendQueryResult(mergedMsg + i*config.K, streamer, this->PROCESS_COMM);
    }
}

void Coordinator::saveResult(long* answers, long responseId)
{
    std::ofstream outfile;
    outfile.open(getResultFileName("dist_index"), std::ios_base::app);
    outfile << responseId << " ";
    for (int i = 0; i < config.K; i++) {
        outfile << answers[i] << " ";
    }
    outfile << endl;
    outfile.close();
}

void Coordinator::cleanResponseIdData(long responseId)
{
    // When send back result, we can remove id from global map
    this->responseReceiveThreadControl.lock();
    this->responseControl.erase(responseId);
    this->whoStreamerNeedsResponse.erase(responseId);
    this->responseReceiveThreadControl.unlock();
}

void Coordinator::processIndex(const Buffer& task)
{
   this->IVFADCProcessIndex(task);
}

void Coordinator::processQuery(const Buffer& task)
{
    this->IVFADCProcessQuery(task);
}
void Coordinator::Merge(QPResponses responses, long* answers)
{
    int n_qp = responses.size() / (config.K * 2);
    double initTime = Time::getCurrentTime();
    int counter[n_qp];
    for (int i = 0; i < n_qp; i++) counter[i] = 0;

    for (int topi = 0; topi < config.K; topi++) {
        float bestDist = std::numeric_limits<float>::max();
        long bestId = -1;
        int fromShard = -1;

        for (int shard = 0; shard < n_qp; shard++) {
            if (counter[shard] == config.K) continue;

            if (responses[(shard * (config.K * 2)) + config.K + counter[shard]] < bestDist) {
                bestDist = responses[(shard * (config.K * 2)) + config.K + counter[shard]];
                bestId = responses[(shard * (config.K * 2)) + counter[shard]];
                fromShard = shard;
            }
        }

        answers[topi] = bestId;
        counter[fromShard]++;
    }
    this->mergeTime += Time::getCurrentTime() - initTime;
}

int Coordinator::nextQueryProcessor()
{
    int workerId = this->queryProcessorsId.at(this->queryProcessorIt);
    this->queryProcessorIt += 1;

    if (this->queryProcessorIt >= (this->queryProcessorIndexBegin + INDEX_WINDOW_SIZE)) {
        // If is the end of window
        // Back to the window beginning
        this->queryProcessorIt = this->queryProcessorIndexBegin;
    } else {
        if (this->queryProcessorIt >= this->queryProcessorsId.size()) { // If is the end of vector ids
            // Also back to the window beginning
            this->queryProcessorIt = this->queryProcessorIndexBegin;
        }
    }

    return workerId;
}

int Coordinator::nextRegion()
{
    int workerId = this->rrRegionIt;
    this->rrRegionIt += 1;

    if (this->rrRegionIt >= (this->queryProcessorsId.size())) {
        this->rrRegionIt = 0;
    }

    return workerId;
}