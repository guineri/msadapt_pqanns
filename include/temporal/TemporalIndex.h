//
// Created by gnandrade on 18/03/2021.
//

#ifndef PQNNS_MULTI_STREAM_TEMPORALINDEX_H
#define PQNNS_MULTI_STREAM_TEMPORALINDEX_H

#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFPQ.h>

#include "utils/Config.h"
#include "utils/IndexIO.h"
#include "utils/Logger.h"
#include "utils/read/ReadeIVF.h"

#include <utility>
#include <thread>
#include <mutex>
#include <shared_mutex>
#include <map>
#include <faiss/utils/RWLock.h>

class TemporalIndex {
public:
    TemporalIndex() {};
    void setup(Config config);
    void read(const char * ivf_index_file_name, const vector<long>& centroids, vector<int> slices);
    void nearestBuckets(int n, float* data, long* idx, float* dist, int k);
    void insert(long n, float* data, long* centIdx, long* idx);
    void search(int n,  const float *x, int k, long* cidx, float* cdist, float* distances, long* labels,
                pair<int, int> innerThreads);
    void temporalUpdate();
    void clearStreamIndex();
    void verifySizes();

private:
    Config config;
    int temporalSlice;
    long temporalThreshold = 10000; //TODO Parametrize
    faiss::IndexFlatL2 *coarse_centroids;
    std::vector<faiss::IndexIVFPQ *> temporalIndex;
    long centroidsSize;

    vector<RWLock *> readWriteLock;
    mutex streamIndexLock;
    shared_mutex _temporalUpdateLock;

    vector<long> temporalIndexIds;
    std::unordered_map<long, long> indexSizes;
    faiss::IndexIVFPQ * emptyIndex();
    void merge(vector<float> responses, float* distances, long* labels, int n_qp);
    void search_thread(int n, const float *x, int k,
                       std::pair<vector<long>, vector<float>> toSearch, vector<float> &innerThreadsResponses,
            mutex& innerThreadsMutex);
};


#endif //PQNNS_MULTI_STREAM_TEMPORALINDEX_H
