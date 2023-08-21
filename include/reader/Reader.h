//
// Created by gnandrade on 06/05/2020.
//

#ifndef PQNNS_MULTI_STREAM_READER_H
#define PQNNS_MULTI_STREAM_READER_H

#include "utils/Data.h"
#include "utils/read/ReadeIVF.h"
#include "Worker.h"
#include <vector>
#include <set>

class Reader {

public:
    explicit Reader(Worker& workerReader);

    static void ReadTrainIndexFromFile(Worker& worker);
    void ReadIVFIndexFromFile(vector<long> centroids, vector<int> slices);
    static void ReadClusterCentroidsDataFromFile(Worker& worker);

    // Generate new
    void createNew();
    void createNewCheckPoint();

    // Read Step!
    void read();

    // Read From file
    void ReadIVFCentroids();
    void ReadIVFEqually();

    // Strategy 1 (DES)
    void DataEqualSplit();
    void EqualSplitNBSend();

    // Strategy 2 (BES)
    void BucketEqualSplit();

    // Strategy 3 (SABES)
    void SpatialAwareBucketEqualSplit();

    // Used by strategy 2 and 3
    void CentroidSplitNBSend();

private:
    long amountToRead;
    long initNBId;
    Worker *w;
};


#endif //PQNNS_MULTI_STREAM_READER_H
