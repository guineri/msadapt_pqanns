//
// Created by gnandrade on 11/04/2020.
//

#ifndef PQNNS_MULTI_STREAM_CONFIG_H
#define PQNNS_MULTI_STREAM_CONFIG_H
#define BOOST_BIND_GLOBAL_PLACEHOLDERS

#include <iostream>
#include <cstring>
#include <cstdio>
#include <vector>
#include <string>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "utils/Data.h"

namespace pt = boost::property_tree;

using namespace std;

class Config
{
private:
    void configure_dataset_input();

public:
    void loadConfig(int argc, char* argv[]);

    long NB;     // Train base size
    long NT;
    int DIM;    // Vector Dimension
    int K;      // K Nearest-Neighbors
    int NCENTROIDS; //centroids

    int QUERY_STREAM_SIZE; // Number of stream vectors
    int INDEX_STREAM_SIZE; // Number of index vectors

    int CHECKPOINT;
    
    int STREAM_PACKAGE; // Vectors per message to stream
    int QUERY_PACKAGE;  // Vectors per message to search
    int INDEX_PACKAGE;  // Vectors per message to add

    long NB_READER_SIZE;
    long NB_READER_BATCH_SIZE;
    float NB_BATCH_PERC;
    vector<int> READERS;

    // Some const values
    int STREAM_BUFFER_SIZE;

    int N_COORDINATORS;
    int N_QUERY_STREAMER;
    int N_QUERY_PROCESSOR;
    int N_INDEX_STREAMER;

    // Load Factor
    double LOAD_FACTOR_QUERY;
    double LOAD_FACTOR_INSERT;
    double MAX_THROUGHPUT_RATE;
    double REPLICATION_RATE;

    // DB distribution strategy
    int DB_DISTRIBUTION_STRATEGY;

    // PROB
    int NPROB;
    int M;
    int N_BITS_PER_IDX;

    // File Name
    const char * DATABASE;
    string HOME_DIR_PATH;
    string SCRATCH_DIR_PATH;
    string OUT_COORD0_INDEX_FILE;
    string OUT_COORD0_COARSE_INDEX_FILE;
    string OUT_COORD0_CC_CLUSTER_FILE;
    string OUT_MS_TEST_FOLDER;
    string OUT_QP_INDEX_FILE_PREFIX;
    bool READ_FROM_IVF_FILE;

    string TRAIN_FILE_PATH;
    string BASE_FILE_PATH;
    string QUERY_FILE_PATH;

    string OUT_RESULT_PATH;
    bool SAVE_RESULT;

    float SCALE;

    bool GENERATENEW;

    int TEMPORALSLICES;

    float BUCKETOUTDATEDFLEX;

    float TEMPORALUPDATEINTERVAL;

    int MAXCPUCORES;

    int DEDICATEDCORES;

    bool MSTEST;

    int OUTERQUERYTHREADS;

    int OUTERINSERTTHREADS;

    double INSERTION_EXEC_TIME;
    double QUERY_EXEC_TIME;

    int STREAM_CONTROL_STRATEGY;
    long STREAM_TIME;
};
#endif //PQNNS_MULTI_STREAM_CONFIG_H
