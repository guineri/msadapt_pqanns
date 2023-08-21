//
// Created by gnandrade on 03/04/2020.
//

#ifndef PQNNS_MULTI_STREAM_MPICOMMUNICATOR_H
#define PQNNS_MULTI_STREAM_MPICOMMUNICATOR_H

#include "mpi.h"
#include "utils/Config.h"
#include "utils/Data.h"
#include "Buffer.h"

#include "Constants.h"

#include <iostream>
#include <vector>

using namespace std;

class MPICommunicator {
public:
    void setConfig(Config conf) {
        this->config = conf;
    }

    int static init(int argc, char* argv[]);
    static int init_multi_thread(int argc, char **argv);

    MPI_Group static getWorldCommGroup();
    static MPI_Comm duplicateCommGroup();
    MPI_Comm static createNewCommGroup(vector<int> rankIds, int tag);
    int static getGlobalWorkerId();
    void static terminate();
    void static waitAllWorkers();
    static int getCommWorkerId(MPI_Comm localComm);

    // Generics
    static void send(long* data, long size, int dest, int tag, MPI_Comm comm);
    static void send(float* data, long size, int dest, int tag, MPI_Comm comm);
    static MPI_Status receive(long *data, int size, int source, int tag, MPI_Comm comm);
    static MPI_Status receive(float *data, int size, int source, int tag, MPI_Comm comm);

    // Stream Query/Index
    // sendQueryStream ==> receiveStream
    // sendIndexStream ==> receiveStream
    void sendQueryStream(float *data, long *idx, int dest, MPI_Comm comm);
    void sendIndexStream(float *data, long *idx, int dest, MPI_Comm comm);
    MPI_Status receiveStream(vector<float>& data, vector<long>& idx, MPI_Comm comm);

    // To process Query/Index
    // sendQueryPackToProcess ==> receiveToProcessWithId
    // sendIndexToProcess ==> receiveToProcessWithId
    static void sendBufferToProcess(Buffer data, int dest, MPI_Comm comm);
    static void sendBufferToProcess(const Buffer& data, const vector<int> &destIDs, MPI_Comm comm);
    MPI_Status receiveBufferToProcess(vector<float>& data, long& id, vector<long>& nCentIdx, vector<float>& nCentDist, MPI_Comm comm);

    // Query Result
    void sendBufferResult(long *I, float *D, Buffer task, MPI_Comm comm);
        void sendZippedResultWithId(long *I, float *D, long id, int count, int dest, MPI_Comm comm);
        MPI_Status receiveZippedResultWithId(float* result, long& id, int& count, MPI_Comm comm);
    void sendQueryResult(long* data, int dest, MPI_Comm comm);
    MPI_Status receiveQueryResult(long* data, int source, MPI_Comm comm);

    // Finalize
    static void sendStreamFinalizeMessage(int dest, MPI_Comm comm);
    static void sendStreamFinalizeMessage(const vector<int>& dest, MPI_Comm comm);
    static void sendProcessFinalizeMessage(int dest, MPI_Comm comm);
    static void sendProcessFinalizeMessage(const vector<int> &dest, MPI_Comm comm);
    static void sendTrainFinalizeMessage(int dest, MPI_Comm comm);
    static void sendTrainFinalizeMessage(const vector<int> &dest, MPI_Comm comm);

    // Train
    void sendNBSlice(long* idx, float* nb, long count, int dest, MPI_Comm comm);
    MPI_Status receiveNBSlice(long** idx, float** nb, long& count, MPI_Comm comm);

    // Handshake
    static void sendCoordinatorIdList(vector<int> coordinatorsId, const vector<int>& indexStreamersId, MPI_Comm comm);
    static void sendCoordinatorsAmount(int amount, const vector<int>& queryProcessorsId, MPI_Comm comm);
    static MPI_Status receiveCoordinatorIdList(vector<int> &coordinatorsId, MPI_Comm comm);
    static MPI_Status receiveCoordinatorsAmount(int *coordAmount, MPI_Comm comm);

private:
    Config config;
    void sendStreamWithId(float *data, long *idx, int dest, int tag, MPI_Comm comm);
};

#endif // PQNNS_MULTI_STREAM_MPICOMMUNICATOR_H
