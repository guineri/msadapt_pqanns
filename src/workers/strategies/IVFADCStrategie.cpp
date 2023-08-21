//
// Created by gnandrade on 19/04/2020.
//

#include "workers/Coordinator.h"
#include "workers/QueryProcessor.h"
#include <faiss/index_io.h>
#include <faiss/Clustering.h>

#include "omp.h"
#include "utils/IndexIO.h"
#include <bits/stdc++.h>

void Coordinator::IVFADCTrain()
{
    CoarseCentroidTrainStep();
    if (config.DB_DISTRIBUTION_STRATEGY == SABES_STRATEGY) {
        ClusterTrainStep();
    }
}

void Coordinator::CoarseCentroidTrainStep()
{
    if (indexFileExists(config.OUT_COORD0_INDEX_FILE)
        && indexFileExists(config.OUT_COORD0_COARSE_INDEX_FILE)) {
        // Yet trained just read from file in next step!
        Reader::ReadTrainIndexFromFile(*this);
        return;
    }

    // Coarse centroids started in DIM space
    this->coarse_centroids = new faiss::IndexFlatL2(this->config.DIM);

    // Global index to be trained
    this->index = new faiss::IndexIVFPQ(this->coarse_centroids, this->config.DIM, config.NCENTROIDS, config.M, config.N_BITS_PER_IDX);

    {   // training

        Data trainDataset(config.TRAIN_FILE_PATH.data(), config.DIM, config.NT, 0);
        float *trainVecs = trainDataset.getNextBatch(config.NT);

        // Train it!
        this->index->nprobe = config.NPROB;
        this->index->verbose = false;
        this->index->train (config.NT, trainVecs);

        trainDataset.~Data();
        delete [] trainVecs;
    }

    { // Saving
        write_index(this->index, config.OUT_COORD0_INDEX_FILE.data());
        write_index(this->coarse_centroids, config.OUT_COORD0_COARSE_INDEX_FILE.data());
    }
}

void Coordinator::ClusterTrainStep()
{
    if (!indexFileExists(cc_cluster_file_name()) || !indexFileExists(rep_region_file_name())) {
        // If Coarse Centroid not clustered yet, so cluster it in qp processors
        int nClusters = this->queryProcessorsId.size();
        faiss::IndexFlatL2 regions(this->config.DIM);
        faiss::Clustering cluss(this->config.DIM, nClusters);
        cluss.train(this->coarse_centroids->ntotal, this->coarse_centroids->xb.data(), regions);

        std::ofstream outfile_cluster_file;
        outfile_cluster_file.open(cc_cluster_file_name(), ios::trunc);
        // Assign query processor to coarse centroids
        for (int i = 0; i <  this->coarse_centroids->ntotal; i++) {
            int centroidOffset_b = i * this->config.DIM;
            int n = 1; int k = 1;
            long* queryProcessorId = new long[n];
            regions.assign(n, this->coarse_centroids->xb.data() + centroidOffset_b, queryProcessorId, k);
            this->centroidsRegions[i] = queryProcessorId[0];

            // Also save, it
            outfile_cluster_file << i << " " << queryProcessorId[0] << endl;
        }
        outfile_cluster_file.close();

        std::ofstream outfile_rep_region_file;
        outfile_rep_region_file.open(rep_region_file_name(), ios::trunc);
        // Get distance matrix from region centroids
        for (int r = 0; r < nClusters; r++) {
            int regionOffset_b = r * this->config.DIM;
            int n = 1; int k = nClusters;
            long* nearbyRegions = new long[n * k];
            regions.assign(n, regions.xb.data() + regionOffset_b, nearbyRegions, k);
            this->nearbyRegions[r].insert(this->nearbyRegions[r].begin(), nearbyRegions, nearbyRegions + k);

            // Also save, it
            for (auto nb : this->nearbyRegions[r]) {
                outfile_rep_region_file << nb << " ";
            }
            outfile_rep_region_file << endl;
        }
        outfile_rep_region_file.close();
    } else {
        // Just Read from file!
        Reader::ReadClusterCentroidsDataFromFile(*this);
    }
}

void Worker::ReplicationPreProcessingStep()
{
    this->replicationAmount = round(config.REPLICATION_RATE * (double)(this->queryProcessorsId.size() - 1));
    if (replicationAmount > 0) {
        for (int regionTarget = 0; regionTarget < this->nearbyRegions.size(); regionTarget++) {
            for (int nb = 0; nb < replicationAmount; nb++) {
                int nearbyReg = this->nearbyRegions[regionTarget].at(nb);
                this->regionsHasMyData[regionTarget].push_back(nearbyReg);
                this->regionsIHaveData[nearbyReg].push_back(regionTarget);
            }
        }
    }
}

// Forward step
void Coordinator::IVFADCProcessQuery(const Buffer& task)
{
    switch (config.DB_DISTRIBUTION_STRATEGY)
    {
        case DES_STRATEGY:
        {
            this->BroadCastProcessQuery(task);
            break;
        }

        case BES_STRATEGY:
        {
            this->IVFADCProcessQueryByRegion(task);
            break;
        }

        case SABES_STRATEGY:
        {
            this->IVFADCProcessQueryByRegion(task);
            break;
        } 

        default:
            break;
    }
}

// Forward step
void Coordinator::IVFADCProcessIndex(const Buffer& task)
{
    switch (config.DB_DISTRIBUTION_STRATEGY)
    {
        case DES_STRATEGY:
        {
            this->RoundRobinProcessIndex(task);
            break;
        }

        case BES_STRATEGY:
        {
            this->IVFADCProcessIndexByRegion(task);
            break;
        }

        case SABES_STRATEGY:
        {
            this->IVFADCProcessIndexByRegion(task);
            break;
        }
        default:
            break;
    }
}

// This strategy try to reduce communication
// We first find the W closest coarse centroid of input query
// Then we find the regions that each centroid belong to.
// Send this query to query processors responsible for the regions
void Coordinator::IVFADCProcessQueryByRegion(Buffer task)
{
    int n_threads = 1;
    int w = config.NPROB;
    // Find w nearest centroids for task.getPackToSend
    vector<long> nCentIdx;
    vector<float> nCentDist;
    nCentIdx.resize(w * task.getPackSize());
    nCentDist.resize(w * task.getPackSize());
    this->coarse_centroids->search_parallel(task.getPackSize(), task.getPackToProcess(), w,&nCentDist[0], &nCentIdx[0], n_threads);
    task.setNCentIdx(nCentIdx);
    task.setNCentDist(nCentDist);

    // Get the regions that keeps these centroids
    set<long> nearestCentroidsRegions;
    for (int i = 0; i < task.getPackSize(); i++) {
        for (int j = 0; j < w; j++) {
            int region = getRegionByCentroid(nCentIdx[ i * w + j ]);
            nearestCentroidsRegions.insert(region);
        }
    }

    set<long> regionsToSend = RegionSelectionHeuristicSimpler(nearestCentroidsRegions);

    // Get correspondent query process for each region
    vector<int> queryProcessorsToSend;
    std::set<long>::iterator regIt;
    for (regIt=regionsToSend.begin(); regIt!=regionsToSend.end(); ++regIt) {
        long regionId = *regIt;
        int queryProcessorId = this->queryProcessorsId.at(regionId);
        queryProcessorsToSend.push_back(queryProcessorId);
    }

    pair<int, QPResponses> newPair;
    if (queryProcessorsToSend.size() == 1) {
        //we just need one response
        newPair.first = UNIQUE_RESPONSE;
    } else {
        newPair.first = queryProcessorsToSend.size();
    }

    this->responseReceiveThreadControl.lock();
    this->responseControl[task.getPackId()] = newPair;
    // Save the streamer that will receive response
    this->whoStreamerNeedsResponse[task.getPackId()] = task.getPackFrom();
    this->responseReceiveThreadControl.unlock();

    // Send!
    this->sendToSizeAvg += queryProcessorsToSend.size();
    MPICommunicator::sendBufferToProcess(task, queryProcessorsToSend, this->PROCESS_COMM);

    // delete [] nearestCentroids;
}

void Coordinator::IVFADCProcessIndexByRegion(Buffer task)
{
    // We are considering that will be just one index vector
    // Its the same process of CentroidSplitNBSend
    // Get the nearest coarse centroid of nb vectors
    int n_threads = 1;
    vector<long> nCentIdx;
    vector<float> nCentDist;
    nCentIdx.resize(task.getPackSize());
    nCentDist.resize(task.getPackSize());
    // TODO Use ASSIGN
    this->coarse_centroids->search_parallel(task.getPackSize(), task.getPackToProcess(), 1, &nCentDist[0],
            &nCentIdx[0], n_threads);
    task.setNCentIdx(nCentIdx);
    task.setNCentDist(nCentDist);

    // Find the correspondent RegionID given the nearest centroid
    int nearestCentroid = nCentIdx[0];
    int targetRegion = this->getRegionByCentroid(nearestCentroid);
    int queryProcessorID = this->queryProcessorsId.at(targetRegion);

    // If our strategy will handle replications
    // We will also add this xb vector to another R QPs
    vector<long> replicationRegions;
    if (this->replicationAmount > 0) {
        // In this case we will get the replicationAmount nearests QPs
        for (auto reg : this->regionsIHaveData[targetRegion]) {
            int qpId = this->queryProcessorsId.at(reg);
            MPICommunicator::sendBufferToProcess(task, qpId, this->PROCESS_COMM);
        }
    } else {
        MPICommunicator::sendBufferToProcess(task, queryProcessorID, this->PROCESS_COMM);
    }


    nCentDist.clear();
    nCentDist.shrink_to_fit();
    nCentIdx.clear();
    nCentIdx.shrink_to_fit();
}

void Coordinator::RoundRobinProcessIndex(Buffer task)
{
    // 1.2.1 - Send Index vectors to Query processors in a Round robin way inside a defined window!
    // Find w nearest centroids for task.getPackToSend
    // auto  *nearestCentroids = new faiss::Index::idx_t [w * task.getPackSize()];
    int n_threads = 1;
    vector<long> nCentIdx;
    vector<float> nCentDist;
    nCentIdx.resize(task.getPackSize());
    nCentDist.resize(task.getPackSize());
    this->coarse_centroids->search_parallel(task.getPackSize(), task.getPackToProcess(), 1, &nCentDist[0], &nCentIdx[0], n_threads);
    task.setNCentIdx(nCentIdx);
    task.setNCentDist(nCentDist);

    MPICommunicator::sendBufferToProcess(std::move(task), nextQueryProcessor(), this->PROCESS_COMM);
}

void Coordinator::BroadCastProcessQuery(Buffer task)
{
    int n_threads = 1;
    int w = config.NPROB;
    // Find w nearest centroids for task.getPackToSend
    // auto  *nearestCentroids = new faiss::Index::idx_t [w * task.getPackSize()];
    vector<long> nCentIdx;
    vector<float> nCentDist;
    nCentIdx.resize(w * task.getPackSize());
    nCentDist.resize(w * task.getPackSize());
    this->coarse_centroids->search_parallel(task.getPackSize(), task.getPackToProcess(), w,&nCentDist[0], &nCentIdx[0], n_threads);
    task.setNCentIdx(nCentIdx);
    task.setNCentDist(nCentDist);

    pair<int, QPResponses> newPair;
    if (this->queryProcessorsId.size() == 1) {
        //we just need one response
        newPair.first = UNIQUE_RESPONSE;
    } else {
        newPair.first = this->queryProcessorsId.size();
    }

    this->responseReceiveThreadControl.lock();
    this->responseControl[task.getPackId()] = newPair;
    // Save the streamer that will receive response
    this->whoStreamerNeedsResponse[task.getPackId()] = task.getPackFrom();
    this->responseReceiveThreadControl.unlock();

    // Send!
    this->sendToSizeAvg += queryProcessorsId.size();
    //cout << "COORD SENDED [" << task.getPackId() << "]: " << task.getPackToProcess()[0] << " " << task.getPackToProcess()[1] << " ... " << task.getPackToProcess()[(config.DIM-2)] << " " << task.getPackToProcess()[(config.DIM-1)] << endl;
    MPICommunicator::sendBufferToProcess(task, this->queryProcessorsId, this->PROCESS_COMM);
}

// ========================= QUERY PROCESSOR
void QueryProcessor::IVFADCSearch(Buffer task)
{
    double initTime = Time::getCurrentTime();
    long *I = new long[task.getPackSize() * config.K];
    float *D = new float[task.getPackSize() * config.K];
    // nq = nb of queries => QUERY_PACKAGE
    // xq = package of queries => receivedVectors
    // k = number of nearest neighbours to search => K
    // D = output distances between search vectors and k vectors for each searched vector
    // I = output id of k vectors for each searched vector

    pair<int, int> innerThreads = task.getNInnerThreads();
    this->temporalIndex.search(task.getPackSize(), task.getPackToProcess(),
            this->config.K, task.getNCentIdx(), task.getNCentDist(), D, I, innerThreads);

    if (!this->config.MSTEST) {
        double initSendTime = Time::getCurrentTime();
        this->MPIComm.sendBufferResult(I, D, task, this->PROCESS_COMM);
    }

    delete [] I;
    delete [] D;
}

void QueryProcessor::IVFADCAddIndex(Buffer task)
{
    this->temporalIndex.insert(task.getPackSize(), task.getPackToProcess(), task.getNCentIdx(), task.getVecsIdx());
}

set<long> Coordinator::RegionSelectionHeuristicSimpler(set<long> nearestCentroidsRegions)
{
    if (this->replicationAmount == 0) {
        // We do not have nothing to filter =(
        return nearestCentroidsRegions;
    }

    if (this->replicationAmount == this->queryProcessorsId.size() -1) {
        // Fully replicated
        // we just need to choose one
        // Round Robin fashion
        set<long> sendTo;
        sendTo.insert(nextRegion());
        return sendTo;
    }

    set<long> sendTo;
    // In this case we will filter regionsToSend
    // Removing regions that have same data
    set<long>::iterator regIt;
    for (regIt=nearestCentroidsRegions.begin(); regIt!=nearestCentroidsRegions.end(); ++regIt) {
        long regionTarget = *regIt;

        if (sendTo.empty()) {
            sendTo.insert(regionTarget);
            continue;
        }

        auto inSendTo = sendTo.find(regionTarget);
        if (inSendTo != sendTo.end()) {
            // This region already in sendTo
            continue;
        }

        map<long, vector<long>> regionRepresents;
        // HMD = regionsHasMyData
        // HMD vector has for each region R, all other regions NR
        // -> that NR keeps R replicated dataset
        bool HMDpresent = false;
        for (auto h : this->regionsHasMyData[regionTarget]) {
            inSendTo = sendTo.find(h);
            if (inSendTo != sendTo.end()) {
                // In sandTo have a region that have same data of regionTarget
                HMDpresent = true;
                break;
            }
        }

        if (HMDpresent) {
            continue;
        }
        // In the end of this HUGE heuristic
        // If anyone in sendTo has regionTarget data
        // we add region target
        sendTo.insert(regionTarget);
    }

    return sendTo;
}

set<long> Coordinator::RegionSelectionHeuristic(set<long> nearestCentroidsRegions)
{
    if (this->replicationAmount == 0) {
        // We do not have nothing to filter =(
        return nearestCentroidsRegions;
    }

    if (this->replicationAmount == this->queryProcessorsId.size() -1) {
        // Fully replicated
        // we just need to choose one
        // Round Robin fashion
        set<long> sendTo;
        sendTo.insert(nextRegion());
        return sendTo;
    }

    set<long> sendTo;
    // In this case we will filter regionsToSend
    // Removing regions that have same data
    set<long>::iterator regIt;
    for (regIt=nearestCentroidsRegions.begin(); regIt!=nearestCentroidsRegions.end(); ++regIt) {
        long regionTarget = *regIt;

        if (sendTo.empty()) {
            sendTo.insert(regionTarget);
            continue;
        }

        auto inSendTo = sendTo.find(regionTarget);
        if (inSendTo != sendTo.end()) {
            // This region already in sendTo
            continue;
        }

        map<long, vector<long>> regionRepresents;
        // HMD = regionsHasMyData
        // HMD vector has for each region R, all other regions NR
        // -> that NR keeps R replicated dataset
        bool HMDpresent = false;
        for (auto h : this->regionsHasMyData[regionTarget]) {
            inSendTo = sendTo.find(h);
            if (inSendTo != sendTo.end()) {
                // In sandTo have a region that have same data of regionTarget
                HMDpresent = true;
                break;
            }
            // We also need to check if
            // A the region H (thats keeps replication data of targetRegion)
            // also keeps replicated data of some region I already in sendTo vector.
            // If yes, we can remove I from sendTo and put H.
            // So H keeps dara from regionTarget and I.
            // IHD = regionsIHaveData
            // IHD vector has for each region R, all other regions NR
            // -> that R Keeps NR replicated dataset
            bool IHDpresent = false;
            for (auto i : this->regionsIHaveData[h]) {
                inSendTo = sendTo.find(i);
                if (inSendTo != sendTo.end()) {
                    // Its really important to know if its possible to change i for h
                    // h needs to keep the same replication data of i
                    // in vector regionsRepresents we save all regions that some region is representing in sendTo
                    // So to change i for h. h also needs to keep i representation
                    bool IDHcanChange = true;
                    if (!regionRepresents[i].empty()) {
                        for (auto r : regionRepresents[i]) {
                            auto has = find(this->regionsHasMyData[r].begin(), this->regionsHasMyData[r].end(), h);
                            if (has == this->regionsHasMyData[r].end()) {
                                // Oh no!
                                // h do not represent r, that is representd by i
                                // So we cannot change i for h. Im sorry
                                IDHcanChange = false;
                                break;
                            }
                        }
                    }

                    if (IDHcanChange) {
                        // It means that all regions r represented by i also is represented by h
                        // or i represents any one,
                        // so we can change i for h.
                        IHDpresent = true;

                        // changing i for h
                        // Removing I
                        sendTo.erase(inSendTo);
                        // Addin H
                        sendTo.insert(h);
                        // Now h represents everyone that i represent
                        if (!regionRepresents[i].empty()) {
                            regionRepresents[h].insert(regionRepresents[h].begin(), regionRepresents[i].begin(), regionRepresents[i].end());
                            regionRepresents[i].clear();
                        }
                        // Also h will represent i and region target
                        regionRepresents[h].push_back(i);
                        regionRepresents[h].push_back(regionTarget);
                    }
                    break;
                }
            }
            if (IHDpresent) {
                HMDpresent = true;
                continue;
            }
        }
        if (HMDpresent) {
            continue;
        }
        // In the end of this HUGE heuristic
        // If anyone in sendTo has regionTarget data
        // we add region target
        sendTo.insert(regionTarget);
    }

    return sendTo;
}