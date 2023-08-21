//
// Created by gnandrade on 03/04/2020.
//

#include "Environment.h"

void Environment::config()
{
    logger->log(DEBUG, ENVIRONMENT, -1, "Configuring...");

    this->nCoordinatorWorkers = this->conf.N_COORDINATORS;
    this->nQueryStreamerWorkers = this->conf.N_QUERY_STREAMER;
    this->nIndexStreamerWorkers = this->conf.N_INDEX_STREAMER;
    this->nQueryProcessorWorkers = this->conf.N_QUERY_PROCESSOR;

    int globalId = 0;

    for (int i = 0; i < nCoordinatorWorkers; i++) {
        this->coordinatorsId.push_back(globalId++);
        this->workers.push_back(new Coordinator());
    }

    for (int i = 0; i < nQueryStreamerWorkers; i++) {
        this->queryStreamerId.push_back(globalId++);
        this->workers.push_back(new QueryStreamer());
    }

    for (int i = 0; i < nIndexStreamerWorkers; i++) {
        this->indexStreamerId.push_back(globalId++);
        this->workers.push_back(new IndexStreamer());
    }

    for (int i = 0; i < nQueryProcessorWorkers; i++) {
        this->queryProcessorId.push_back(globalId++);
        this->workers.push_back(new QueryProcessor());
    }

    for (int coord = 0; coord < workers.size(); coord++) {
        this->workers.at(coord)->queryProcessorsId = queryProcessorId;
        this->workers.at(coord)->queryStreamersId = queryStreamerId;
        this->workers.at(coord)->indexStreamersId = indexStreamerId;
        this->workers.at(coord)->coordinatorsId = coordinatorsId;
    }
}

Worker* Environment::getWorker(int index)
{
    return this->workers.at(index);
}


void Environment::startUp(int argc, char* argv[])
{
    int nWorkers = MPICommunicator::init_multi_thread(argc, argv);
    int globalWorkerId = MPICommunicator::getGlobalWorkerId();

    MPI_Comm STREAM_COMM = MPICommunicator::duplicateCommGroup();
    MPI_Comm PROCESS_GROUP = MPICommunicator::duplicateCommGroup();

    MPICommunicator comm;
    comm.setConfig(this->conf);

    // Get worker instance to run
    Worker* w = getWorker(globalWorkerId);
    w->setId(globalWorkerId);
    w->setStreamComm(STREAM_COMM);
    w->setProcessComm(PROCESS_GROUP);
    w->setComm(comm);
    w->setConfig(this->conf);

    // Train initial Index
    MPICommunicator::waitAllWorkers();
    //if (globalWorkerId == 0) {
    //    cout << "Train started ..." << endl;
    //}
    w->train();

    // Distribute initial database
    MPICommunicator::waitAllWorkers();
    //if (globalWorkerId == 0) {
    //    cout << "Distribution started ..." << endl;
    //}
    w->datasetDistribution();

    // Execute query/index stream and query search
    MPICommunicator::waitAllWorkers();
    //if (globalWorkerId == 0) {
    //    cout << "Run started ..." << endl;
   // }

   //vector<float> qlf{0.2, 0.4, 0.6, 0.8, 1.0};
   //vector<float> ilf{0.2, 0.4, 0.6, 0.8, 1.0};

   //for (int q = 0; q < qlf.size(); q++) {
   // for (int i = 0; i < ilf.size(); i++) {
   //     if (qlf[q] + ilf[i] <= 1.21) {
   //         cout << "[" << qlf[q] << "][" << ilf[i] << "]" << endl;
    //        w->config.LOAD_FACTOR_QUERY = qlf[q];
    //        w->config.LOAD_FACTOR_INSERT = ilf[i];
            w->run();
            MPICommunicator::waitAllWorkers();
    //    }
    //}
   //}

    // Report!!
    if (!w->config.MSTEST && find(w->coordinatorsId.begin(), w->coordinatorsId.end(), globalWorkerId) != w->coordinatorsId.end()) {
        vector<string> metric;
        Coordinator *c = dynamic_cast<Coordinator *>(getWorker(globalWorkerId));
        metric.push_back(Data::floatToString((float)c->id));
        metric.push_back(Data::floatToString((float)conf.DB_DISTRIBUTION_STRATEGY));
        metric.push_back(Data::floatToString((float)c->processedQuery));
        double sendToS = (double) c->sendToSizeAvg / (double) c->processedQuery;
        metric.push_back(Data::floatToString(sendToS));

        metric.push_back(Data::floatToString(c->runTime));
        metric.push_back(Data::floatToString(c->procTotalTime));
        metric.push_back(Data::floatToString(c->streamRecTotalTime));
        metric.push_back(Data::floatToString(c->recTotalTime));
        metric.push_back(Data::floatToString(c->mpiRecZippedResultWithId));
        metric.push_back(Data::floatToString((float)c->recMsgTotalCount));
        metric.push_back(Data::floatToString(c->recMsgTotalTime));
        metric.push_back(Data::floatToString(c->recvTime));
        metric.push_back(Data::floatToString(c->mergeTime));

        saveMetric(c, metric, "coord");
        //cout << "Coord: " << c->id << endl;
        //cout << "Strategy: " << conf.DB_DISTRIBUTION_STRATEGY << endl;
        //cout << "C=" << conf.N_COORDINATORS << " QP=" << conf.N_QUERY_PROCESSOR << endl;
        //cout << "Query Amount: " << c->processedQuery << endl;
        //cout << "Query Per Sec (QPS): " << conf.N_COORDINATORS * c->queryPerSec << endl;
        //cout << "Send Per Query (SPQ): " <<  sendToS << endl;
        //cout << "Run time:" << c->runTime << endl;
        //cout << "Receive Thread time: " << c->recTotalTime << endl;
        //cout << "Merge Total time: " << c->mergeTime << endl;
        //cout << "Recv Total Time: " << c->recvTime << endl;
    }

    if (find(w->queryProcessorsId.begin(), w->queryProcessorsId.end(), globalWorkerId) != w->queryProcessorsId.end()) {
        auto *qp = dynamic_cast<QueryProcessor *>(getWorker(globalWorkerId));
        vector<string> metric;
        metric.push_back(Data::floatToString((float)qp->id));
        metric.push_back(Data::floatToString((float)qp->processedQuery));
        metric.push_back(Data::floatToString((float)qp->queryOuterFinal));
        metric.push_back(Data::floatToString((float)qp->queryProcessExecTime));
        metric.push_back(Data::floatToString((float)qp->queryUpdatedCheckTime));
        metric.push_back(Data::floatToString((float)qp->queryExecTime));
        metric.push_back(Data::floatToString((float)qp->queryResponseTime));
        metric.push_back(Data::floatToString((float)qp->queryQueueTime));
        metric.push_back(Data::floatToString((float)qp->processedInsert));
        metric.push_back(Data::floatToString((float)qp->insertOuterFinal));
        metric.push_back(Data::floatToString((float)qp->processedInsertByQuery));
        metric.push_back(Data::floatToString((float)qp->insertProcessExecTime));
        metric.push_back(Data::floatToString((float)qp->insertExecTime));
        metric.push_back(Data::floatToString((float)qp->insertResponseTime));
        metric.push_back(Data::floatToString((float)qp->insertQueueTime));
        saveMetric(qp, metric, "qps");
    }

    if (globalWorkerId == 0) {
        // TODO resolve this!
        //cout << "Finished !!!" << endl;
        //cout << "Coordinators report: " << w->getResultTimeFileName("coord") << endl;
        //cout << "QueryProcessors report: " << w->getResultTimeFileName("qps") << endl;

        //if (w->config.SAVE_RESULT) {
        //    cout << "KNN Idx per Query: " << w->getResultFileName("dist_index") << endl;
        //}
    }
}

void Environment::terminate()
{
    MPICommunicator::waitAllWorkers();
    MPICommunicator::terminate();
}

void Environment::saveMetric(Worker *w, const vector<string>& metrics, const char * type)
{
    std::ofstream outfile;
    outfile.open(w->getResultTimeFileName(type), std::ios_base::app);
    for (const auto& m : metrics) {
        outfile << m << " ";
     }
    outfile << endl;
    outfile.close();
}
