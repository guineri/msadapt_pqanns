//
// Created by gnandrade on 03/04/2020.
//

#include "comm/MpiCommunicator.h"

int MPICommunicator::init(int argc, char* argv[])
{
    int nWorkers;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nWorkers);

    return nWorkers;
}

int MPICommunicator::init_multi_thread(int argc, char* argv[])
{
    int nWorkers;
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    MPI_Comm_size(MPI_COMM_WORLD, &nWorkers);

    return nWorkers;
}

MPI_Group MPICommunicator::getWorldCommGroup()
{
    MPI_Group world_group;
    MPI_Comm_group(MPI_COMM_WORLD, &world_group);

    return world_group;
}

MPI_Comm MPICommunicator::createNewCommGroup(vector<int> rankIds, int tag)
{
    MPI_Group world_group = getWorldCommGroup();
    MPI_Group new_group;
    MPI_Group_incl(world_group, rankIds.size(), rankIds.data(), &new_group);

    MPI_Comm new_comm;
    MPI_Comm_create_group(MPI_COMM_WORLD, new_group, tag, &new_comm);

    return new_comm;
}

MPI_Comm MPICommunicator::duplicateCommGroup()
{
    MPI_Comm new_comm;
    MPI_Comm_dup(MPI_COMM_WORLD, &new_comm);

    return new_comm;
}

int MPICommunicator::getGlobalWorkerId()
{
    int worldRank;
    MPI_Comm_rank(MPI_COMM_WORLD, &worldRank);

    return worldRank;
}

int MPICommunicator::getCommWorkerId(MPI_Comm localComm)
{
    int localRank;
    MPI_Comm_rank(localComm, &localRank);

    return localRank;
}

void MPICommunicator::terminate()
{
    MPI_Finalize();
}

void MPICommunicator::waitAllWorkers()

{
    MPI_Barrier(MPI_COMM_WORLD);
}

// RAW SEND (FLOAT AND LONG)
void MPICommunicator::send(float* data, long size, int dest, int tag, MPI_Comm comm)
{
    MPI_Send(data, size, MPI_FLOAT, dest, tag, comm);
}

void MPICommunicator::send(long* data, long size, int dest, int tag, MPI_Comm comm)
{
    MPI_Send(data, size, MPI_LONG, dest, tag, comm);
}

MPI_Status MPICommunicator::receive(float* data, int size, int source, int tag, MPI_Comm comm)
{
    MPI_Status status;
    MPI_Recv(data, size, MPI_FLOAT, source, tag, comm, &status);
    return status;
}

MPI_Status MPICommunicator::receive(long* data, int size, int source, int tag, MPI_Comm comm)
{
    MPI_Status status;
    MPI_Recv(data, size, MPI_LONG, source, tag, comm, &status);
    return status;
}

// ========================================== STREAM
void MPICommunicator::sendQueryStream(float* data, long* idx, int dest, MPI_Comm comm)
{
    int tag = MSG_TAG_QUERY;
    sendStreamWithId(data, idx, dest, tag, comm);
}

// Stream Index
void MPICommunicator::sendIndexStream(float* data, long* idx, int dest, MPI_Comm comm)
{
    int tag = MSG_TAG_INDEX;
    sendStreamWithId(data, idx, dest, tag, comm);
}

void MPICommunicator::sendStreamWithId(float* data, long* idx, int dest, int tag, MPI_Comm comm)
{
    // |data=stream_size*dim|idx=stream_size|
    int size = this->config.STREAM_BUFFER_SIZE + this->config.STREAM_PACKAGE;

    auto* zippedIndexMsg = new float[size];

    // Zip Rein just one buffer
    copy(data, data + this->config.STREAM_BUFFER_SIZE, zippedIndexMsg);
    copy(idx, idx + this->config.STREAM_PACKAGE, zippedIndexMsg + this->config.STREAM_BUFFER_SIZE);

    send(zippedIndexMsg, size, dest, tag, comm);
}

// Wrappers
// Receive Generic Stream
MPI_Status MPICommunicator::receiveStream(vector<float>& data, vector<long>& idx, MPI_Comm comm)
{
    // |data=stream_size*dim|idx=stream_size|
    int recSize = this->config.STREAM_BUFFER_SIZE + this->config.STREAM_PACKAGE; // count with ids!
    int source = MPI_ANY_SOURCE; // From any streamer
    int tag = MPI_ANY_TAG; // Any one!

    auto tmpRec = new float[recSize];
    MPI_Status status = receive(tmpRec, recSize, source, tag, comm);

    if (status.MPI_TAG != MSG_TAG_STREAM_FINALIZE) {
        // In this case we need to split vectors and idx
        // Get only ids!
        // |data=stream_size*dim|
        int size = this->config.STREAM_BUFFER_SIZE;
        data.resize(size);
        copy(tmpRec, tmpRec + this->config.STREAM_BUFFER_SIZE, data.begin());

        // |idx=stream_size|
        size = this->config.STREAM_PACKAGE;
        idx.resize(size);
        copy(tmpRec + this->config.STREAM_BUFFER_SIZE, tmpRec + recSize, idx.begin());
    }

    delete [] tmpRec;
    return status;
}

// =================================================== TO PROCESS
// Receive Index and Query to process
// Receive from: sendQueryPackToProcess and sendIndexToProcess/sendIndexStreamWithId
MPI_Status MPICommunicator::receiveBufferToProcess(vector<float>& data, long& id, vector<long>& nCentIdx, vector<float>& nCentDist, MPI_Comm comm)
{
    // |packId=1|data=stream_size*dim|nCentIdx=NPROB|nCentDist=NPROB|
    int recSize = 1 + this->config.STREAM_BUFFER_SIZE + (2 * this->config.NPROB);
    int source = MPI_ANY_SOURCE; // Receive Index or queries from all coordinators
    int tag = MPI_ANY_TAG; // Could be: MSG_TAG_QUERY or MSG_TAG_INDEX

    // Receive a generic buffer with max size: in this case will be index with idx
    float* tmpRec = new float[recSize];
    MPI_Status status = receive(tmpRec, recSize, source, tag, comm);

    // |packId=1|
    id = (long) tmpRec[0];

    // If it is a finalize message, just return.
    if (status.MPI_TAG == MSG_TAG_STREAM_FINALIZE) {
        id = -1;
        return status;
    }

    // Fill data
    // |data=stream_size*dim|idx=stream_size|
    int dataSize = this->config.STREAM_BUFFER_SIZE;
    data.resize(dataSize);
    copy(tmpRec + 1, tmpRec + 1 + dataSize, data.begin());

    // Get Nearest Centroids Idx
    int nCentroidsSize = this->config.NPROB;
    nCentIdx.resize(nCentroidsSize);
    copy(tmpRec + 1 + dataSize, tmpRec + 1 + dataSize + nCentroidsSize, nCentIdx.begin());

    // Get Nearest Centroids Dist
    nCentDist.resize(nCentroidsSize);
    copy(tmpRec + 1 + dataSize + nCentroidsSize, tmpRec + 1 + dataSize + (2 * nCentroidsSize), nCentDist.begin());

    delete [] tmpRec;
    return status;
}

// Query to process
void MPICommunicator::sendBufferToProcess(const Buffer& data, const vector<int>& destIDs, MPI_Comm comm)
{
    // Receiver: receiveToProcessWithId
    for (int workerId : destIDs) {
        MPICommunicator::sendBufferToProcess(data, workerId, comm);
    }
}

void MPICommunicator::sendBufferToProcess(Buffer data, int dest, MPI_Comm comm)
{
    int size = data.getPackDataBufferSize();
    int tag = data.getTag();

    send(data.getPackDataToSend().data(), size, dest, tag, comm);
}

// Query Result
void MPICommunicator::sendBufferResult(long* I, float* D, Buffer task, MPI_Comm comm)
{
    int packSize = task.getPackSize();

    for (int i = 0 ; i < packSize; i++) {
        int offset_b = i * config.K;
        int offset_e = offset_b + config.K;
        vector<long> sliceToSendI;
        vector<float> sliceToSendD;
        sliceToSendI.insert(sliceToSendI.end(), I + offset_b, I + offset_e);
        sliceToSendD.insert(sliceToSendD.end(), D + offset_b, D + offset_e);

        sendZippedResultWithId(sliceToSendI.data(), sliceToSendD.data(), task.getVecsIdx()[i], 1, task.getPackFrom().at(i), comm);
    }
}

void MPICommunicator::sendZippedResultWithId(long* I, float* D, long id, int count, int dest, MPI_Comm comm)
{
    // |id|count|idx|distances|
    int size = 2 * (count * config.K) + 2; //Plus ID and count
    int tag = MSG_TAG_QUERY_RESPONSE;

    auto* zippedResult = new float[size];

    zippedResult[0] = id;
    zippedResult[1] = count;

    // Zip Result in just one buffer
    copy(I, I + (count * config.K), zippedResult + 2);
    copy(D, D + (count * config.K), zippedResult + 2 + (count * config.K));

    // Send it
    send(zippedResult, size, dest, tag, comm);
}

MPI_Status MPICommunicator::receiveZippedResultWithId(float* result, long& id, int& count, MPI_Comm comm)
{
    // |id|count|idx|distances|
    int size = 2 * (config.QUERY_PACKAGE * config.K) + 2; //Plus ID and count!
    int tag = MPI_ANY_TAG; // could be a finalize msg to.
    int source = MPI_ANY_SOURCE;

    auto tmpRec = new float[size];
    MPI_Status status = receive(tmpRec, size, source, tag, comm);

    id = (long) tmpRec[0];
    count = (int) tmpRec[1];

    if (status.MPI_TAG == MSG_TAG_RESPONSE_FINALIZE) {
        id = -1;
        return status;
    }

    copy(tmpRec + 2, (tmpRec + 2) + (2 * count * config.K), result);

    delete [] tmpRec;
    return status;
}

// Query Result
void MPICommunicator::sendQueryResult(long* data, int dest, MPI_Comm comm)
{
    int size = this->config.STREAM_PACKAGE * this->config.K;
    int tag = MSG_TAG_QUERY_RESPONSE;

    send(data, size, dest, tag, comm);
}

MPI_Status MPICommunicator::receiveQueryResult(long* data, int source, MPI_Comm comm)
{
    int size = this->config.STREAM_PACKAGE * this->config.K;
    int tag = MSG_TAG_QUERY_RESPONSE;

    return receive(data, size, source, tag, comm);
}

// Split NB
void MPICommunicator::sendNBSlice(long* idx, float* nb, long count, int dest, MPI_Comm comm)
{
    // |count|idx|nb slice|
    //long size = 1 + (count) + (count * config.DIM); //Plus count
    long size = 1 + (config.NB_READER_BATCH_SIZE) + (config.NB_READER_BATCH_SIZE * config.DIM); //Plus count!
    int tag = MSG_TAG_TRAIN;

    auto* zippedResult = new float[size];

    zippedResult[0] = count;

    // Zip Result in just one buffer
    copy(idx, idx + (count), zippedResult + 1);
    copy(nb, nb + (count * config.DIM), zippedResult + 1 + (count));

    for (int i = 0; i < config.SCALE; i++) {
        // Send it
        send(zippedResult, size, dest, tag, comm);
    }

    delete [] zippedResult;
}

MPI_Status MPICommunicator::receiveNBSlice(long** idx, float** nb, long& count, MPI_Comm comm)
{
    // |count|idx|nb slice|
    long size = 1 + (config.NB_READER_BATCH_SIZE) + (config.NB_READER_BATCH_SIZE * config.DIM); //Plus count!
    int tag = MPI_ANY_TAG; // could be a finalize msg to.
    int source = MPI_ANY_SOURCE;

    auto tmpRec = new float[size];
    MPI_Status status = receive(tmpRec, size, source, tag, comm);

    count = (long) tmpRec[0];

    if (status.MPI_TAG == MSG_TAG_TRAIN_FINALIZE) {
        count = -1;
        return status;
    }

    *idx = new long[count];
    *nb = new float[count * config.DIM];

    copy(tmpRec + 1, (tmpRec + 1) + (count), *idx);
    copy((tmpRec + 1) + (count), (tmpRec) + 1 + (count) + (count * config.DIM), *nb);

    delete[] tmpRec;
    return status;
}

// Finalize Messages
void MPICommunicator::sendStreamFinalizeMessage(int dest, MPI_Comm comm)
{
    float trashData = 0.0;
    MPI_Send(&trashData, 1, MPI_FLOAT, dest, MSG_TAG_STREAM_FINALIZE, comm);
}

void MPICommunicator::sendStreamFinalizeMessage(const vector<int>& dest, MPI_Comm comm)
{
    for (auto workerId : dest) {
        sendStreamFinalizeMessage(workerId, comm);
    }
}

void MPICommunicator::sendProcessFinalizeMessage(int dest, MPI_Comm comm)
{
    float trashData = 0.0;
    MPI_Send(&trashData, 1, MPI_FLOAT, dest, MSG_TAG_RESPONSE_FINALIZE, comm);
}

void MPICommunicator::sendProcessFinalizeMessage(const vector<int>& dest, MPI_Comm comm)
{
    for (auto workerId : dest) {
        sendProcessFinalizeMessage(workerId, comm);
    }
}

void MPICommunicator::sendTrainFinalizeMessage(int dest, MPI_Comm comm)
{
    float trashData = 0.0;
    MPI_Send(&trashData, 1, MPI_FLOAT, dest, MSG_TAG_TRAIN_FINALIZE, comm);
}

void MPICommunicator::sendTrainFinalizeMessage(const vector<int>& dest, MPI_Comm comm)
{
    for (auto workerId : dest) {
        sendTrainFinalizeMessage(workerId, comm);
    }
}

// Handshake
void MPICommunicator::sendCoordinatorIdList(vector<int> coordinatorsId, const vector<int>& streamersId, MPI_Comm comm)
{
    int size = coordinatorsId.size();
    int tag = MSG_TAG_HANDSHAKE;

    for (auto streamer : streamersId) {
        MPI_Send(&coordinatorsId[0], size, MPI_INT, streamer, tag, comm);
    }
}

MPI_Status MPICommunicator::receiveCoordinatorIdList(vector<int> &coordinatorsId, MPI_Comm comm)
{
    int tag = MSG_TAG_HANDSHAKE;
    int size;

    // Probing incoming message to know buffer size!
    MPI_Status status;
    MPI_Probe(MPI_ANY_SOURCE, MSG_TAG_HANDSHAKE, comm, &status);
    MPI_Get_count(&status, MPI_INT, &size);

    // Receive list content
    coordinatorsId.resize(size);
    MPI_Recv(&coordinatorsId[0], size, MPI_INT, status.MPI_SOURCE, tag, comm, &status);
    return status;
}

void MPICommunicator::sendCoordinatorsAmount(int amount, const vector<int>& queryProcessorsId, MPI_Comm comm)
{
    int tag = MSG_TAG_HANDSHAKE;
    int size = 1;

    for (auto queryProcessor: queryProcessorsId) {
        MPI_Send(&amount, size, MPI_INT, queryProcessor, tag, comm);
    }
}

MPI_Status MPICommunicator::receiveCoordinatorsAmount(int* coordAmount, MPI_Comm comm)
{
    int tag = MSG_TAG_HANDSHAKE;
    int size = 1;
    int source = MPI_ANY_SOURCE;

    MPI_Status status;
    MPI_Recv(coordAmount, size, MPI_INT, source, tag, comm, &status);
    return status;
}
