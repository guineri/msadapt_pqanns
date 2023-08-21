//
// Created by gnandrade on 03/04/2020.
//

#ifndef PQNNS_WS_WORKER_H
#define PQNNS_WS_WORKER_H

#include "comm/MpiCommunicator.h"
#include "utils/Logger.h"
#include "utils/Config.h"
#include "temporal/TemporalIndex.h"
#include "Buffer.h"

#include <mutex>
#include <iostream>
#include <fstream>
#include <cstdio>
#include <map>

using namespace std;

class Worker {
public:

    virtual void train() { }
    virtual void datasetDistribution() { }
    virtual void run() {  }

    virtual void setId(int workerId)
    {
        this->id = workerId;
    }

    virtual void setSource(Source type)
    {
        this->sourceType = type;
    }

    virtual void setStreamComm(MPI_Comm streamComm)
    {
        this->STREAM_COMM = streamComm;
    }

    virtual void setProcessComm(MPI_Comm processComm)
    {
        this->PROCESS_COMM = processComm;
    }

    virtual void setConfig(Config conf)
    {
        this->config = conf;
    }

    virtual void setComm(MPICommunicator mpiComm) {
        this->MPIComm = mpiComm;
    }

    Logger* logger = Logger::getInstance();
    int id{};
    Source sourceType{};
    map<string, double> totalTimePerRegion;
    map<string, int> nTimePerRegion;

    // Communicators groups
    // Stream communicator gets streamers and Coordinator
    // Used by streamers to send and consumer thread to receive
    MPI_Comm STREAM_COMM;

    // Processors communicator gets query processors and Coordinator
    // Used by producer thread to send data to query processors
    // Used by query processors to send result back to coordinator
    // Used by coordinator to send back result to query streamers
    MPI_Comm PROCESS_COMM;

    //Configurations
    Config config;
    MPICommunicator MPIComm;

    // Its it
    vector<int> queryProcessorsId;
    vector<int> queryStreamersId;
    vector<int> indexStreamersId;
    vector<int> coordinatorsId;

    // IVFADC PQ
    mutex addNBSliceControl;
    long amountQPReceived;
    faiss::IndexFlatL2 *coarse_centroids;
    faiss::IndexIVFPQ *index;
    TemporalIndex temporalIndex; //Used in QPs

    long dataAmount;

    // Clustering Strategies data
    // Coarse Centroid ID / Region ID (QP ID)
    map<long, long> centroidsRegions;
    // Region ID and the list of nearby regions
    map<long, vector<long>> nearbyRegions;

    // Replication Pre processing
    void ReplicationPreProcessingStep();
    int replicationAmount;
    map<long, vector<long>> regionsHasMyData;
    map<long, vector<long>> regionsIHaveData;

    int getRegionByCentroid(int centroid);
    map<int, int> centroidsNotClusteredByRegion;
    int getRegionByNotClusteredCentroids(int centroidPosition);

    map<int, vector<int>> centroidsClusteredByRegion;
    int getRegionByClusteredCentroids(int centroidPosition);

    const char * ivf_index_file_name() {
        char* filename = new char[200];
        sprintf(filename, "%s/index_full_db[%s]_cent[%d]",
                config.OUT_QP_INDEX_FILE_PREFIX.c_str(),
                config.DATABASE,
                config.NCENTROIDS);
        return filename;
    }

    const char * ivf_index_file_name_batch(long batch) {
        char* filename = new char[200];
        sprintf(filename, "%s/index_full_db[%s]_cent[%d]_batch[%ld]_p[%f]",
                config.OUT_QP_INDEX_FILE_PREFIX.c_str(),
                config.DATABASE,
                config.NCENTROIDS,
                batch,
                config.NB_BATCH_PERC);
        return filename;
    }

    const char * rep_region_file_name() {
        char *filename = new char[200];
        sprintf(filename, "%s/rep_region_qp[%d]_cent[%d]_db[%s]",
                config.OUT_COORD0_CC_CLUSTER_FILE.c_str(),
                config.N_QUERY_PROCESSOR,
                config.NCENTROIDS,
                config.DATABASE);
        return filename;
    }

    const char * cc_cluster_file_name() {
        char* filename = new char[200];
        sprintf(filename, "%s/cc_cluster_qp[%d]_cent[%d]_db[%s]",
                config.OUT_COORD0_CC_CLUSTER_FILE.c_str(),
                config.N_QUERY_PROCESSOR,
                config.NCENTROIDS,
                config.DATABASE);
        return filename;
    }

    string getResultFileName(const char* type) {
        char* filename = new char[200];
        sprintf(filename, "%s/result_coord[%d]_st[%d]_db[%s]_qp[%d]_cood[%d]_cent[%d]_[%f].%s",
                config.OUT_RESULT_PATH.c_str(),
                this->id,
                config.DB_DISTRIBUTION_STRATEGY,
                config.DATABASE,
                config.N_QUERY_PROCESSOR,
                config.N_COORDINATORS,
                config.NCENTROIDS,
                config.REPLICATION_RATE,
                type);
        return filename;
    }

    string getResultTimeFileName(const char* type) {
        char* filename = new char[200];
        sprintf(filename, "%s/result_st[%d]_db[%s]_qp[%d]_cood[%d]_cent[%d]_sale[%f]_sc[%d]_conf[%d_%d].%s",
                config.OUT_RESULT_PATH.c_str(),
                config.DB_DISTRIBUTION_STRATEGY,
                config.DATABASE,
                config.N_QUERY_PROCESSOR,
                config.N_COORDINATORS,
                config.NCENTROIDS,
                config.SCALE,
                config.STREAM_CONTROL_STRATEGY,
                config.OUTERQUERYTHREADS,
                config.OUTERINSERTTHREADS,
                type);
        return filename;
    }

    string msc_test_thread_out(const char* type, int tid, int qot, int iot) {
        char* filename = new char[200];
        sprintf(filename, "%s/%s_%d_lfq[%lf]_lfi[%lf]_qot[%d]_iot[%d]_sc[%d]_st[%ld].%s",
                config.OUT_MS_TEST_FOLDER.c_str(),
                type,
                tid,
                config.LOAD_FACTOR_QUERY,
                config.LOAD_FACTOR_INSERT,
                qot,
                iot,
                config.STREAM_CONTROL_STRATEGY,
                config.STREAM_TIME,
                "ms_test_out");
        return filename;
    }

    string msc_test_stream_controler_out(const char* type) {
        char* filename = new char[200];
        sprintf(filename, "%s/stream_controler_%s.%s",
                config.OUT_MS_TEST_FOLDER.c_str(),
                type,
                "msc_out");
        return filename;
    }
};

#endif // PQNNS_WS_WORKER_H
