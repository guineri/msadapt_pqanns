//
// Created by gnandrade on 11/04/2020.
//

#include "utils/Config.h"

void Config::loadConfig(int argc, char **argv) {
    // Parameters:
    if (argc != 53) {
        printf("Usage:\n");
        printf("mpirun -n <workers> pqnns-multi-stream\n\t"
               "-home <home_dir_path>\n\t"
               "-scratch <scratch_dir_path>\n\t"
               "-c <coordinators>\n\t"
               "-qp <query processors>\n\t"
               "-qs <query streamer>\n\t"
               "-is <insertion streamer>\n\t"
               "-re <readers processors>\n\t"
               "-lfq <load factor>\n\t"
               "-lfi <load factor>\n\t"
               "-strategy <strategy: 0..4>\n\t"
               "-dataset <dataset: random, sift1b>\n\t"
               "-rep_rate <replication rate>\n\t"
               "-read_ivf <true/false>\n\t"
               "-save <true/false>\n\t"
               "-scale <scale>\n\t"
               "-create_index <true/false>\n\t"
               "-ms_test <true/false>\n\t"
               "-outer_query\n\t"
               "-outer_insert\n\t"
               "-max-threads\n\t"
               "-stream-strategy\n\t"
               "-stream-time\n\t"
               "-temporal-slices\n\t"
               "-bucket-outdated-flex\n\t"
               "-temporal-update-interval\n\t"
               "-checkpoint\n\t");
        exit(0);
    }

    // Path args
    this->HOME_DIR_PATH                 = argv[2];
    this->SCRATCH_DIR_PATH              = argv[4];
    this->N_COORDINATORS                = atoi(argv[6]);
    this->N_QUERY_PROCESSOR             = atoi(argv[8]);
    this->N_QUERY_STREAMER              = atoi(argv[10]);
    this->N_INDEX_STREAMER              = atoi(argv[12]);
    int readers_n                       = atoi(argv[14]);
    this->LOAD_FACTOR_QUERY             = atof(argv[16]);
    this->LOAD_FACTOR_INSERT            = atof(argv[18]);
    this->DB_DISTRIBUTION_STRATEGY      = atoi(argv[20]);
    this->DATABASE                      = argv[22];
    this->REPLICATION_RATE              = atof(argv[24]);
    this->READ_FROM_IVF_FILE            = strcmp(argv[26], "true") == 0;
    this->SAVE_RESULT                   = strcmp(argv[28], "true") == 0;
    this->SCALE                         = atof(argv[30]);
    this->GENERATENEW                   = strcmp(argv[32], "true") == 0;
    this->MSTEST                        = strcmp(argv[34], "true") == 0;
    this->OUTERQUERYTHREADS             = atoi(argv[36]);
    this->OUTERINSERTTHREADS            = atoi(argv[38]);
    this->MAXCPUCORES                   = atoi(argv[40]);
    this->STREAM_CONTROL_STRATEGY       = atoi(argv[42]);
    this->STREAM_TIME                   = atoi(argv[44]);
    this->TEMPORALSLICES                = atoi(argv[46]);
    this->BUCKETOUTDATEDFLEX            = atof(argv[48]);
    this->TEMPORALUPDATEINTERVAL        = atof(argv[50]);
    this->CHECKPOINT                    = atof(argv[52]);
    this->DEDICATEDCORES                = 0;

    // Default
    this->N_BITS_PER_IDX = 8;

    // Setting up readers
    for (int i = 0; i < readers_n; i++) {
        this->READERS.push_back(i);
    }

    configure_dataset_input();

    this->NB_READER_SIZE = ceil ( (float) NB /  (float) READERS.size());
    this->NB_READER_BATCH_SIZE = ceil(((float)NB_READER_SIZE * NB_BATCH_PERC));

    // Packages size
    // We fixed stream package always 1
    // So each streamer (index/query) will stream 1 vector per msg
    this->STREAM_PACKAGE = 1;
    this->QUERY_PACKAGE = 1;    // yet not focus
    this->INDEX_PACKAGE = 1;    // yet not focus

    // Buffer sizes
    this->STREAM_BUFFER_SIZE = this->STREAM_PACKAGE * this->DIM;

    // TODO Set stream rate
    double T = 0.8;    // Execution time to one query
    double N = 1;       // Number of max parallelism (number of query processors)
    this->MAX_THROUGHPUT_RATE = N / T;

    // Result file
    this->OUT_RESULT_PATH = SCRATCH_DIR_PATH + "/out/result";

    if (this->N_QUERY_PROCESSOR <= 0 && !this->GENERATENEW) {
        printf("Wrong parameters semantic:\n");
        printf("ERROR: We need at least one QueryProcessor!\n");
        printf("Current: Coordinators = %d / QueryProcessors %d\n", this->N_COORDINATORS, this->N_QUERY_PROCESSOR);
        exit(0);
    }

    if (this->MSTEST)  {
        // printf("Multi Stream Test Mode\n");
        this->N_COORDINATORS                = 0;
        this->N_QUERY_PROCESSOR             = 1;
        this->N_QUERY_STREAMER              = 0;
        this->N_INDEX_STREAMER              = 0;
    }

    if (!this->GENERATENEW && !this->READ_FROM_IVF_FILE && this->READERS.empty()) {
        printf("In Distribute mode at least one node must be READER!!\n");
        exit(0);
    }
}

void Config::configure_dataset_input()
{
    ifstream jsonFile("data/datasets.json");

    boost::property_tree::ptree pt;
    boost::property_tree::read_json(jsonFile, pt);
    bool found = false;

    for (auto & datasets: pt.get_child("datasets")) {
        string dataset_name = datasets.second.get_child("name").get_value<string>();
        if (dataset_name == this->DATABASE) {
            found = true;
            this->K = datasets.second.get_child("k").get_value<int>();
            this->DIM = datasets.second.get_child("dim").get_value<int>();
            this->NCENTROIDS = datasets.second.get_child("centroids").get_value<int>();
            int dataset_subqnt = datasets.second.get_child("subquantizers").get_value<int>();
            this->M = this->DIM / dataset_subqnt;
            this->NPROB = datasets.second.get_child("w").get_value<int>();

            // File paths
            auto dataset_nb = datasets.second.get_child("base");
            string nb_path = dataset_nb.get_child("path").get_value<string>();
            this->BASE_FILE_PATH = SCRATCH_DIR_PATH + nb_path;
            this->NB = dataset_nb.get_child("size").get_value<long>();
            this->INSERTION_EXEC_TIME = dataset_nb.get_child("exec_time").get_value<double>();

            auto dataset_train = datasets.second.get_child("train");
            string train_path = dataset_train.get_child("path").get_value<string>();
            this->TRAIN_FILE_PATH = SCRATCH_DIR_PATH + train_path;
            this->NT = dataset_train.get_child("size").get_value<long>();

            auto dataset_query = datasets.second.get_child("query");
            string query_path = dataset_query.get_child("path").get_value<string>();
            this->QUERY_FILE_PATH = SCRATCH_DIR_PATH + query_path;
            this->QUERY_STREAM_SIZE = dataset_query.get_child("size").get_value<long>();
            this->QUERY_EXEC_TIME = dataset_query.get_child("exec_time").get_value<double>();

            this->NB_BATCH_PERC = datasets.second.get_child("base_batch_percent").get_value<float>();

            // Output Files Path
            this->OUT_COORD0_INDEX_FILE =
                    SCRATCH_DIR_PATH + "/out/index/train/coord0_ivfadc_index_" + (this->DATABASE) + "_" + to_string(this->NCENTROIDS) + ".faiss";
            this->OUT_COORD0_COARSE_INDEX_FILE =
                    SCRATCH_DIR_PATH + "/out/index/train/coord0_coarse_centroids_index_" + (this->DATABASE) + "_" + to_string(this->NCENTROIDS) + ".faiss";
            this->OUT_COORD0_CC_CLUSTER_FILE = SCRATCH_DIR_PATH + "/out/cluster";
            this->OUT_QP_INDEX_FILE_PREFIX = SCRATCH_DIR_PATH + "/out/index/qp";
            this->OUT_MS_TEST_FOLDER = SCRATCH_DIR_PATH + "/out/result/msc_test/";
            break;
        }
    }

    if (!found) {
        printf("Input database not know: %s\n", this->DATABASE);
        exit(0);
    }
}