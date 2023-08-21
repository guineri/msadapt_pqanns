//
// Created by gnandrade on 03/04/2020.
//

#include <utils/Data.h>

#include <utility>
#include "workers/QueryProcessor.h"

QueryProcessor::QueryProcessor()
{
    this->setSource(QUERY_PROCESSOR);
    this->startup();
}

void QueryProcessor::startup()
{
    this->queryProcessorCanFinalize = false;
    this->insertProcessorCanFinalize = false;
    this->queryStreamerCanStart = false;
    this->insertStreamerCanStart = false;
    this->profileTimeSummaryClear();
    this->monitor.setup();
}

void QueryProcessor::profileTimeSummaryClear()
{
    // Query Sumary
    this->processedQuery = 0;
    this->queryResponseTime = 0.0;
    this->queryQueueTime = 0.0;
    this->queryExecTime = 0.0;
    this->queryUpdatedCheckTime = 0.0;
    this->queryProcessExecTime = 0.0;
    // Insert Sumary
    this->processedInsert = 0;
    this->insertResponseTime = 0.0;
    this->insertQueueTime = 0.0;
    this->insertExecTime = 0.0;
    this->insertProcessExecTime = 0.0;
    this->processedInsertByQuery = 0;
}

void QueryProcessor::profileTimeSummary(int currentConfigInsertOuter, int currentConfigQueryOuter)
{
    std::stringstream buffer;
    double currentConfigQueryInner = 0;
    if (currentConfigQueryOuter > 0) {
        if (currentConfigQueryOuter == this->config.MAXCPUCORES - this->config.DEDICATEDCORES) {
            currentConfigQueryInner = 1;
        } else {
            currentConfigQueryInner =
                    (double) ((this->config.MAXCPUCORES - this->config.DEDICATEDCORES) - currentConfigInsertOuter) / (double) currentConfigQueryOuter;
        }
        buffer.clear();
    }
    buffer << "[" << currentConfigQueryOuter << "][" << currentConfigQueryInner << "][" << currentConfigInsertOuter << "]";

    // Query Summary After Configuration
    // Q
    // <configuration>
    // <processedQuery>
    // <queryProcessExecTime>
    // <queryUpdatedCheckTime>
    // <queryExecTime>
    // <queryResponseTime>
    // <queryQueueTime>
    if (this->config.MSTEST) {
        printf("Q\t%s\t%ld\t%.9f\t%.9f\t%.9f\t%.9f\t%.9f\n",
            buffer.str().c_str(),
            this->processedQuery,
            this->queryProcessExecTime / (double) currentConfigQueryOuter,
            this->queryUpdatedCheckTime / (double)  this->processedQuery,
            this->queryExecTime / (double)  this->processedQuery,
            this->queryResponseTime / (double)  this->processedQuery,
            this->queryQueueTime / (double)  this->processedQuery);
    }

    this->queryOuterFinal = currentConfigQueryOuter;
    this->queryProcessExecTime /= (double) currentConfigQueryOuter;
    this->queryUpdatedCheckTime /= (double)  this->processedQuery;
    this->queryExecTime /= (double)  this->processedQuery;
    this->queryResponseTime /= (double)  this->processedQuery;
    this->queryQueueTime /= (double)  this->processedQuery;

    // Insert Summary After Configuration
    // I
    // <configuration>
    // <processedInsert>
    // <insertProcessExecTime>
    // <insertExecTime>
    // <insertResponseTime>
    // <insertQueueTime>
    if (this->config.MSTEST) {
        printf("I\t%s\t%ld\t%ld\t%.9f\t%.9f\t%.9f\t%.9f\n",
            buffer.str().c_str(),
            this->processedInsert,
            this->processedInsertByQuery,
            this->insertProcessExecTime / (double) currentConfigInsertOuter,
            this->insertExecTime / (double)  this->processedInsert,
            this->insertResponseTime / (double)  this->processedInsert,
            this->insertQueueTime / (double)  this->processedInsert);
    }

    this->insertOuterFinal = currentConfigInsertOuter;
    this->insertProcessExecTime /= (double) currentConfigInsertOuter;
    this->insertExecTime /= (double)  this->processedInsert;
    this->insertResponseTime /= (double)  this->processedInsert;
    this->insertQueueTime /= (double)  this->processedInsert;
}

void QueryProcessor::ReceiveNB()
{
    this->amountQPReceived = 0;
    int finalizeCount = 0;
    bool receiveNBCanFinalize = false;

    while (!receiveNBCanFinalize) {
        float *xb_subset = nullptr;
        long *receivedIdx = nullptr;
        long subsetSize = 0;
        MPI_Status status = this->MPIComm.receiveNBSlice(&receivedIdx, &xb_subset, subsetSize, this->PROCESS_COMM);

        switch (status.MPI_TAG) {
            case MSG_TAG_TRAIN:
            {
                this->addNBSliceControl.lock();
                this->index->add_with_ids(subsetSize, xb_subset, receivedIdx);
                this->amountQPReceived += subsetSize;
                this->addNBSliceControl.unlock();
                break;
            }

            case MSG_TAG_TRAIN_FINALIZE:
            {
                finalizeCount += 1;
                if (finalizeCount == config.READERS.size()) {
                    receiveNBCanFinalize = true;
                }
                break;
            }

        }

        delete[] xb_subset;
        delete[] receivedIdx;
    }

    cout << "Index Amount[" << this->id << "]: " << this->amountQPReceived << endl;
}

void QueryProcessor::datasetDistribution()
{
    if (!config.READ_FROM_IVF_FILE) {
        Reader::ReadTrainIndexFromFile(*this);
        if (find(config.READERS.begin(), config.READERS.end(), this->id) != config.READERS.end()) {
            // This query processor will also work as a reader, in this case
            // So, we need a thread to read and send. And another to receive and add index.
            Reader reader(*this);
            std::thread nbReader(&Reader::read, reader);
            std::thread nbReceiver(&QueryProcessor::ReceiveNB, this);

            nbReceiver.join();
            nbReader.join();
        } else {
            // Just receive and add index
            this->ReceiveNB();
        }
    } else {
        // QPs will read IVF from file
        this->temporalIndex.setup(this->config);
        Reader reader(*this);
        reader.read();

    }
}

void QueryProcessor::run()
{
    this->config.DEDICATEDCORES = 0;
    this->startup();
    this->temporalInsertionsBuffer.setup(this);
    this->queriesToProcessBufferList.clear();

    //CORE 0 : Main Thread
    if (this->config.MSTEST) {
        this->begin = true;
    } else {
        this->begin = false;
    }
    
    this->config.DEDICATEDCORES += 1;
    int currentCore = 1;
    cpu_set_t cpuset;

    //CORE 1 : Stream Controller
    this->config.DEDICATEDCORES += 1;

    if (this->config.OUTERQUERYTHREADS > 0 && this->config.MSTEST) {
        //CORE 2 : Query Stream 
        this->config.DEDICATEDCORES += 1;
    }
    if (this->config.OUTERINSERTTHREADS >= 0 && this->config.MSTEST) {
        //CORE 3 : Insertion Streamer
        this->config.DEDICATEDCORES += 1;
    }


    // Receiver Thread if not MSTEST
    if (!this->config.MSTEST) {
        this->config.DEDICATEDCORES += 1;

        std::thread receiver (&QueryProcessor::Receiver, this);
        //CPU_ZERO(&cpuset);
        //CPU_SET(currentCore, &cpuset);
        //pthread_setaffinity_np(receiver.native_handle(),
        //                        sizeof(cpu_set_t), &cpuset);
        receiver.detach();
        currentCore += 1;
    }

    // Stream Controller
    StreamController streamController{};
    streamController.configure(this);

    void (StreamController::*streamControllerFunc)();
    //CORE 1 : Started StreamController
    switch (this->config.STREAM_CONTROL_STRATEGY)
    {
        case 0:
        {
            int outer_insertion = this->config.OUTERINSERTTHREADS;
            if (outer_insertion == -1) {
                outer_insertion = 0;
            }
            int outer_query = this->config.OUTERQUERYTHREADS;
            if (outer_query == -1) {
                outer_query = 0;
            }
            this->wait_all = new boost::barrier((this->config.DEDICATEDCORES - 1) + outer_insertion + outer_query);
            streamController.setupNewConfiguration(outer_query, outer_insertion);
            streamControllerFunc = &StreamController::run;
            break;
        }
        case 1:
            this->wait_all = new boost::barrier((this->config.DEDICATEDCORES - 1) + this->insertOuterThreadsConfig + this->queryOuterThreadsConfig.size());
            //streamControllerFunc = &StreamController::msAdapt;
            streamControllerFunc = &StreamController::msAdaptLevel2;
            break;
        default:
            streamControllerFunc = &StreamController::run;
            break;
    }
    std::thread streamControllerThr (streamControllerFunc, streamController);

    //CPU_ZERO(&cpuset);
    //CPU_SET(currentCore, &cpuset);
    //pthread_setaffinity_np(streamControllerThr.native_handle(),
    //                        sizeof(cpu_set_t), &cpuset);
    streamControllerThr.detach();
    currentCore += 1;

    // Query Streamer
    if (this->config.OUTERQUERYTHREADS > 0 && this->config.MSTEST) {
        std::thread queryStreamer (&MSTestQueryStreamer::run, this);
       //CPU_ZERO(&cpuset);
       //CPU_SET(currentCore, &cpuset);
       //pthread_setaffinity_np(queryStreamer.native_handle(),
       //                         sizeof(cpu_set_t), &cpuset);
        queryStreamer.detach();
        //AQUI!!queryStreamer.join();
        currentCore += 1;
    }

    // Insertion Streamer
    if (this->config.OUTERINSERTTHREADS >= 0 && this->config.MSTEST) {
        std::thread insertStreamer (&MSTestInsertStreamer::run, this);
        //CPU_ZERO(&cpuset);
        //CPU_SET(currentCore, &cpuset);
        //pthread_setaffinity_np(insertStreamer.native_handle(),
        //                        sizeof(cpu_set_t), &cpuset);
        insertStreamer.detach();
        //AQUI!!insertStreamer.join();
        currentCore += 1;
    }

    bool streamNotFinalized = true;
    int currentConfigInsertOuter;
    int currentConfigQueryOuter;
    while (streamNotFinalized) {
        this->streamControllerStop = false;

        //==== Get Current Config To profile
        currentConfigInsertOuter = this->insertOuterThreadsConfig;
        currentConfigQueryOuter = this->queryOuterThreadsConfig.size();
        // =====

        // Dispatch Insert outer threads
        vector<std::thread> iOuterThreads;
        for (int iOuterId = 0; iOuterId < this->insertOuterThreadsConfig; iOuterId++) {
            iOuterThreads.emplace_back(&QueryProcessor::InsertProcessorThread, this, iOuterId);
            //cpu_set_t cpuset;
            //CPU_ZERO(&cpuset);
            //CPU_SET(this->config.DEDICATEDCORES + iOuterId, &cpuset);
            //pthread_setaffinity_np(iOuterThreads.back().native_handle(), sizeof(cpu_set_t), &cpuset);
        }

        // Dispatch Query outer threads
        vector<std::thread> qOuterThreads;
        int qOuterId = 0;
        for (auto qOuter : this->queryOuterThreadsConfig) {
            qOuterThreads.emplace_back(&QueryProcessor::QueryProcessorThread, this, qOuterId);
            //cpu_set_t cpuset;
            //CPU_ZERO(&cpuset);
            //CPU_SET(qOuter.second, &cpuset);
            //pthread_setaffinity_np(qOuterThreads.back().native_handle(), sizeof(cpu_set_t), &cpuset);
            qOuterId += 1;
        }

        //Wait and Join
        for (auto& tqo : qOuterThreads) {
            tqo.join();
        }

        //Wait and Join
        for (auto& tio : iOuterThreads) {
            tio.join();
        }

        // Check final Iteration: When stream controller not stopped and threads ended
        if (!streamControllerStop) {
            if (!this->config.MSTEST) {
                MPICommunicator::sendProcessFinalizeMessage(this->coordinatorsId, this->PROCESS_COMM);
            }
            streamNotFinalized = false;
            this->queryProcessorCanFinalize = true;
            this->insertProcessorCanFinalize = true;
        }
        else {
            this->begin = false;
        //  this->profileTimeSummary(currentConfigInsertOuter, currentConfigQueryOuter);
        //  this->profileTimeSummaryClear();
        }
    }
    this->profileTimeSummary(currentConfigInsertOuter, currentConfigQueryOuter);
    this->profileTimeSummaryClear();
    this->temporalIndex.clearStreamIndex();
}

// This thread will be responsible just to receive query or index
// from coordinators and add in a global task list
// Same Coordinator StreamReceiver thread
void QueryProcessor::Receiver()
{
    double oldTimeQ = 0.0;
    double oldTimeI = 0.0;
    int receivedQuery = 0;
    int receivedIndex = 0;
    int receivedFinalize = 0;
    bool receiverCanFinalize = false;

    while (!receiverCanFinalize) {
        long id;
        vector<float> data;
        vector<long> nCentIdx;
        vector<float> nCentDist;
        MPI_Status status = this->MPIComm.receiveBufferToProcess(data, id, nCentIdx, nCentDist, this->PROCESS_COMM);

        switch (status.MPI_TAG) {
            // 2.1 - If was a Query Message
            case MSG_TAG_QUERY:
            {
                // Here we pack incoming queries
                // Our parameter QUERY_PACKAGE indicates how many queries will be package.
                // When pack is completely full, we add this buffer in global list to be processed by consumer thread.
                bool full = this->queryPacking(data, id, nCentIdx, nCentDist, status.MPI_SOURCE);

                if (full) {
                    double timeNew = Time::getCurrentTime();
                    this->queryBufferPack.setQueuedTime();
                    this->queriesToProcessBufferList.emplace_back(move(this->queryBufferPack));
                
                    if (oldTimeQ != 0.0) {
                        this->monitor.add(QPMonitor::Metric::QAR, timeNew - oldTimeQ);
                    }
                    oldTimeQ = timeNew;
                    
                    // Clean query buffer pack CHECK
                    receivedQuery += 1;
                    this->queryBufferPack = Buffer(MSG_TAG_QUERY, receivedQuery);
                }                
                break;
            }

            // 2.2 - If Was a Index Message
            case MSG_TAG_INDEX:
            {
                // Here we pack incoming indexes
                // Our parameter INDEX_PACKAGE indicates how many index will be package.
                // When pack is completely full, we add this buffer in global list to be processed by consumer thread.
                bool full = this->indexPacking(data, id, nCentIdx, nCentDist, status.MPI_SOURCE);
                this->temporalInsertionsBuffer.addTask(this->indexBufferPack, nCentIdx[0]);

                if (full) {
                    double timeNew = Time::getCurrentTime();
                    if (oldTimeI != 0.0) {
                        this->monitor.add(QPMonitor::Metric::IAR, timeNew - oldTimeI);
                    }
                    oldTimeI = timeNew;

                    // Clean index buffer pack
                    receivedIndex += 1;
                    this->indexBufferPack = Buffer(MSG_TAG_INDEX, receivedIndex);
                }
                break;
            }

            case MSG_TAG_STREAM_FINALIZE:
            {
                receivedFinalize += 1;

                // If all coordinators ended, and InsertionStreamers this thread no longer will receive data
                // so, it will be finished.
                // Also, it will indicate to consumer thread that tasks over
                // TODO Check finalize impact
                if (receivedFinalize >= (this->coordinatorsId.size() + this->indexStreamersId.size())) {
                    receiverCanFinalize = true;

                    // Finalize Insertion Process if have
                    if (this->indexStreamersId.size() > 0) {
                        this->temporalInsertionsBuffer.finalize();
                        this->monitor.final(QPMonitor::Metric::IAR);
                    }

                    // Finalize Query Process
                    this->queriesListControl.lock();
                    this->queriesToProcessBufferList.emplace_back(MSG_TAG_STREAM_FINALIZE);
                    this->queriesListControl.unlock();
                    this->monitor.final(QPMonitor::Metric::QAR);
                }

                break;
            }

            default:
                break;
        }
    }
}

bool QueryProcessor::queryPacking(vector<float> data, long id, vector<long> nCentIdx, vector<float> nCentDist, int from)
{
    this->queryBufferPack.pack(std::move(data), id, std::move(nCentIdx), std::move(nCentDist), from, config.DIM);
    return this->queryBufferPack.getPackSize() == this->config.QUERY_PACKAGE;
}


bool QueryProcessor::indexPacking(vector<float> data, long id, vector<long> nCentIdx, vector<float> nCentDist, int from)
{
    this->indexBufferPack.pack(std::move(data), id, std::move(nCentIdx), std::move(nCentDist), from, config.DIM);
    return this->indexBufferPack.getPackSize() == this->config.INDEX_PACKAGE;
}

// Insert Processor thread is responsible to get a insert task from insert list and run.
// just add to local index
void QueryProcessor::InsertProcessorThread(int iOuterId)
{
    if (this->begin) { //AQUI!!
       this->wait_all->count_down_and_wait(); //AQUI!!
    } //AQUI!!

    double localThroughput;
    double localRate = 0.0;
    double timeNew;
    double oldTime = 0.0;
    double tSingleExec = 0.0;
    double tRT = 0.0;
    double tQT = 0.0;
    long tt = 0;
    double initTime = Time::getCurrentTime();
    bool processorCanFinalize = false;

    this->insertStreamerCanStart = true;

    while (!processorCanFinalize) {

        if (this->streamControllerStop) {
            // Force stop by stream controller
            //cout << "insert thread stopped!!" << endl;
            break;
        }

        Buffer task;
        task = this->temporalInsertionsBuffer.getTask();

        if (!task.isNull()) {
            switch (task.getTag()) {
                case MSG_TAG_INDEX: {
                    // 2.2.1 Add to local index
                    double a = Time::getCurrentTime();
                    this->AddToIndex(task);
                    double b = Time::getCurrentTime() - a;
                    task.setFinalizedProcess();

                    tSingleExec += b;
                    tRT += task.getResponseTime();
                    tQT += task.getQueueTime();
                    tt += 1;
                    
                    timeNew = Time::getCurrentTime();
                    if (oldTime != 0.0) {
                        this->monitor.add(QPMonitor::Metric::IST, timeNew - oldTime, iOuterId);
                    }
                    oldTime = timeNew;
                    this->monitor.add(QPMonitor::Metric::IET, b, iOuterId);
                    this->monitor.add(QPMonitor::Metric::IRT, task.getResponseTime(), iOuterId);
                    this->monitor.add(QPMonitor::Metric::IQT, task.getQueueTime(), iOuterId);

                    if (this->config.SAVE_RESULT) {
                        std::ofstream outfile;
                        int qot = this->queryOuterThreadsConfig.size();
                        int iot = this->insertOuterThreadsConfig;
                        outfile.open(this->msc_test_thread_out("I", iOuterId, qot, iot), std::ios_base::app);
                        outfile << "I\t"
                             << qot << "\t"
                             << iot << "\t"
                             << Data::timeStampToString(Time::getCurrentTime()) << "\t"
                             << task.getPackId() - 1000000 << "\t"
                             << Data::floatToString(b) << "\t"
                             << Data::floatToString(task.getResponseTime()) << "\t"
                             << Data::floatToString(task.getQueueTime()) << endl;
                        outfile.close();
                    }
                    break;
                }

                case MSG_TAG_STREAM_FINALIZE: {
                    //Consumer received a finalize tasks.
                    //There's no more task (index/query) to process.
                    this->monitor.final(QPMonitor::Metric::IST);
                    this->monitor.final(QPMonitor::Metric::IRT);
                    this->monitor.final(QPMonitor::Metric::IET);
                    this->monitor.final(QPMonitor::Metric::IQT);
                    processorCanFinalize = true;
                    break;
                }
                default:
                    break;
            }
        }
    }
    //Summary
    this->insertSummaryProfileMtx.lock();
    this->insertProcessExecTime += Time::getCurrentTime() - initTime;
    this->insertResponseTime += tRT;
    this->insertQueueTime += tQT;
    this->insertExecTime += tSingleExec;
    this->processedInsert += tt;
    this->insertSummaryProfileMtx.unlock();
}

// QueryProcessor thread is responsible to get task from query global list and run.
// Search in local index and send result back
void QueryProcessor::QueryProcessorThread(int qOuterId)
{
    if (this->begin) { //AQUI!!
        this->wait_all->count_down_and_wait(); //AQUI!!
    } //AQUI!!

    double localThroughput;
    double localRate = 0.0;
    double timeNew;
    double oldTime = 0.0;
    double tUpdatedCheck = 0.0;
    double tExec = 0.0;
    double tRT = 0.0;
    double tQT = 0.0;
    long tt = 0;
    double initTime = Time::getCurrentTime();
    bool processorCanFinalize = false;
    this->queryStreamerCanStart = true;

    while (!processorCanFinalize) {

        if (this->streamControllerStop) {
            // Force stop by stream controller
            break;
        }

        Buffer task;
        // Getting next task from global list
        // TODO COND/WAIT? Sleep?
        this->queriesListControl.lock();
        if (!this->queriesToProcessBufferList.empty()) {
            task = move(this->queriesToProcessBufferList.front());
            // not remove finalize task cus all outer threads needs to read to finalize
            if (task.getTag() != MSG_TAG_STREAM_FINALIZE) {
                this->queriesToProcessBufferList.pop_front();
                task.setDequeuedTime();
                task.setNInnerThreads(this->queryOuterThreadsConfig[qOuterId]);
            }
        }
        this->queriesListControl.unlock();

        if (!task.isNull()) {
            switch (task.getTag()) {
                case MSG_TAG_QUERY: {

                    //First Lets check that has pending insertions
                    double a1 = Time::getCurrentTime();
                    /* Strategy Lazy Insertions */
                    if (this->updatedCheckStart) {
                        // Only check when has insertion accumulation
                        this->UpdatedBucketCheck(task);
                    } else {
                        /* Strategy Wait until updated */
                        for (int i = 0; i < this->config.NPROB; i++) {
                            long centroid = task.getNCentIdx()[i];
                            // Blockant Operation
                            this->temporalInsertionsBuffer.waitUntilBucketIsUpdated(centroid);
                        }
                    }
                    double b1 = Time::getCurrentTime() - a1;
                    tUpdatedCheck += b1;

                    // 2.1.1 - Process search in local index
                    double a = Time::getCurrentTime();
                    for (int i = 0; i < 10; i++) {
                        this->Search(task);
                    }
                    double b = Time::getCurrentTime() - a;
                    task.setFinalizedProcess();

                    tQT += task.getQueueTime();
                    tRT += task.getResponseTime();
                    tExec += b;
                    tt += 1;
                    
                    timeNew = Time::getCurrentTime();
                    if (oldTime != 0.0) {
                        this->monitor.add(QPMonitor::Metric::QST, timeNew - oldTime, qOuterId);
                    }
                    oldTime = timeNew;
                    this->monitor.add(QPMonitor::Metric::QET, b, qOuterId);
                    this->monitor.add(QPMonitor::Metric::QUW, b1, qOuterId);
                    this->monitor.add(QPMonitor::Metric::QRT, task.getResponseTime(), qOuterId);
                    this->monitor.add(QPMonitor::Metric::QQT, task.getQueueTime(), qOuterId);

                    if (this->config.SAVE_RESULT) {
                        std::ofstream outfile;
                        int qot = this->queryOuterThreadsConfig.size();
                        int iot = this->insertOuterThreadsConfig;
                        outfile.open(this->msc_test_thread_out("Q", qOuterId, qot, iot), std::ios_base::app);
                        outfile << "Q\t"
                             << qot << "\t"
                             << iot << "\t"
                             << Data::timeStampToString(Time::getCurrentTime()) << "\t"
                             << task.getPackId() << "\t"
                             << Data::floatToString(b1) << "\t"
                             << Data::floatToString(b) << "\t"
                             << Data::floatToString(task.getResponseTime()) << "\t"
                             << Data::floatToString(task.getQueueTime()) << endl;
                        outfile.close();
                    }
                    break;
                }

                case MSG_TAG_STREAM_FINALIZE: {
                    //Consumer received a finalize tasks.
                    //There's no more task (index/query) to process.
                    this->monitor.final(QPMonitor::Metric::QST);
                    this->monitor.final(QPMonitor::Metric::QET);
                    this->monitor.final(QPMonitor::Metric::QUW);
                    this->monitor.final(QPMonitor::Metric::QRT);
                    this->monitor.final(QPMonitor::Metric::QQT);
                    processorCanFinalize = true;
                    break;
                }
                default:
                    break;
            }
        }

    }
    //Summary
    this->querySummaryProfileMtx.lock();
    this->queryProcessExecTime += Time::getCurrentTime() - initTime;
    this->queryResponseTime += tRT;
    this->queryQueueTime += tQT;
    this->queryExecTime += tExec;
    this->queryUpdatedCheckTime += tUpdatedCheck;
    this->processedQuery += tt;
    this->querySummaryProfileMtx.unlock();
}

void QueryProcessor::InsertPendingThread(const vector<long>& bucketsId)
{
    for (long centId : bucketsId) {
        list<Buffer> toInsert;
        toInsert = this->temporalInsertionsBuffer.getAllFromBucket(centId);

        if (!toInsert.empty()) {
            double tRT = 0.0;
            double tQT = 0.0;
            double tExec = 0.0;
            // TODO CHECK BEST STRATEGY!!!!!!
            //vector<float> data;
            //vector<long> idx;
            //vector<long> centIdx;
            for (Buffer t : toInsert) {
                t.setDequeuedTime();
                //data.insert(data.end(), t.getPackToProcess(),  t.getPackToProcess() + this->config.DIM);
                //idx.push_back(t.getPackId());
                //centIdx.push_back(centId);
                double a = Time::getCurrentTime();
                this->temporalIndex.insert(t.getPackSize(), t.getPackToProcess(), t.getNCentIdx(), t.getVecsIdx());
                double b = Time::getCurrentTime() - a;
                t.setFinalizedProcess();

                tRT += t.getResponseTime();
                tQT += t.getQueueTime();
                tExec += b;
            }
            //this->temporalIndex.insert(toInsert.size(), data.data(), centIdx.data(), idx.data());
            this->insertSummaryProfileMtx.lock();
            this->insertResponseTime += tRT;
            this->insertQueueTime += tQT;
            this->insertExecTime += tExec;
            this->processedInsert += toInsert.size();
            this->processedInsertByQuery += toInsert.size();
            this->insertSummaryProfileMtx.unlock();
        }
    }
}

void QueryProcessor::UpdatedBucketCheck(Buffer task)
{
    // Firs, check if needs update
    vector<long> toUpdate;
    for (int i = 0; i < this->config.NPROB; i++) {
        toUpdate.push_back(task.getNCentIdx()[i]);
    }

    if (toUpdate.empty()) {
        return;
    }

    int innerThreads = task.getNInnerThreads().first;
    int coreOffset = task.getNInnerThreads().second;
    if (toUpdate.size() < innerThreads) {
        innerThreads = toUpdate.size();
    }

    vector<std::thread> innerInsertThreads;
    int totalBucketsDistributed = 0;
    int bucketsN = toUpdate.size();
    vector<long> bucketsId;
    for (int t = 0; t < innerThreads; t++) {
        int toInsertBucketsN = std::ceil((double) (bucketsN - totalBucketsDistributed) / (double) (innerThreads - t));

        if (t == (innerThreads -1)) {
            toInsertBucketsN = bucketsN - totalBucketsDistributed;
        }

        int startOffset = totalBucketsDistributed;

        bucketsId.insert(bucketsId.end(),
                         toUpdate.data() + startOffset,
                         toUpdate.data() + startOffset + toInsertBucketsN);

        totalBucketsDistributed += toInsertBucketsN;

        //Dispatch some Threads
        if (t != (innerThreads - 1)) {
            innerInsertThreads.emplace_back(&::QueryProcessor::InsertPendingThread, this, bucketsId);
            //cpu_set_t cpuset;
            //CPU_ZERO(&cpuset);
            //CPU_SET(t + coreOffset + 1, &cpuset);
            //pthread_setaffinity_np(innerInsertThreads.back().native_handle(), sizeof(cpu_set_t), &cpuset);
        } else {
            break;
        }

        bucketsId.clear();
        bucketsId.shrink_to_fit();
    }

    this->InsertPendingThread(bucketsId);

    // Join
    for (auto& t : innerInsertThreads) {
        t.join();
    }

    bucketsId.clear();
    bucketsId.shrink_to_fit();
    toUpdate.clear();
    toUpdate.shrink_to_fit();
}

void QueryProcessor::Search(Buffer task)
{
    if (config.DB_DISTRIBUTION_STRATEGY > 0) {
        // Will receive a batch of nb queries
        this->IVFADCSearch(std::move(task));
        return;
    }
}

void QueryProcessor::AddToIndex(Buffer task)
{
    if (config.DB_DISTRIBUTION_STRATEGY > 0) {
        // Will receive a batch of nb queries
        this->IVFADCAddIndex(std::move(task));
        return;
    }
}