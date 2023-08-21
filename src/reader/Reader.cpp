//
// Created by gnandrade on 06/05/2020.
//

#include "reader/Reader.h"

#include <utility>

Reader::Reader(Worker& workerReader)
{
    this->w = &workerReader;

    // We will split NB to distribParticipants
    long sliceNPerReader = workerReader.config.NB_READER_SIZE;
    long amountSplited = 0;
    for (int r = 0; r < workerReader.config.READERS.size() ; r++) {
        this->initNBId = (r * sliceNPerReader);
        this->amountToRead = sliceNPerReader;
        if (r == (workerReader.config.READERS.size() - 1)) {
            // Last Reader will receive remaining data
            this->amountToRead = workerReader.config.NB - amountSplited;
        }

        if (r == workerReader.id) {
            break;
        }
        amountSplited += sliceNPerReader;
    }
}

void Reader::read() {
    switch (w->config.DB_DISTRIBUTION_STRATEGY) {
        case DES_STRATEGY:
            DataEqualSplit();
            break;
        case BES_STRATEGY:
            BucketEqualSplit();
            break;
        case SABES_STRATEGY:
            SpatialAwareBucketEqualSplit();
            break;
        default:
            break;
    }

    if (!w->config.READ_FROM_IVF_FILE) {
        // This worker finalized to read his own nb slice!
        MPICommunicator::sendTrainFinalizeMessage(w->queryProcessorsId, w->PROCESS_COMM);
    }
}

void Reader::EqualSplitNBSend()
{
    // Initial vector base to be sent
    Data dataset(w->config.BASE_FILE_PATH.data(), w->config.DIM, this->amountToRead, this->initNBId);

    // TODO revise this ceil
    long local_batch_size = floor ((float) this->amountToRead * w->config.NB_BATCH_PERC);
    long local_batch_size_send = 0;

    // TODO revise this ceil
    long batch_n = ceil ( (float) this->amountToRead / (float) local_batch_size );
    long id_offset = this->initNBId;

    int processedBatch = 0;

    while(processedBatch < batch_n) {

        if ( processedBatch == (batch_n - 1) ) {
            local_batch_size = this->amountToRead - local_batch_size_send;
        }

        float *xb = dataset.getNextBatch(local_batch_size);
        processedBatch += 1;

        cout << "[" << w->id << "] Sending NB Batch: " << processedBatch << "/" << batch_n << " (" << local_batch_size << ") " << endl;

        // Ids
        vector<long> ids;
        for (long i = 0; i < local_batch_size; i++) {
            ids.push_back(i + id_offset);
        }

        int nQPWorkers = w->queryProcessorsId.size();
        long vecPerWorkers = floor ((float)local_batch_size / (float)nQPWorkers);
        long amountSent = 0;

        for (int work = 0; work < w->queryProcessorsId.size(); work++) {
            int workerId = w->queryProcessorsId.at(work);
            long amountToSend = vecPerWorkers;

            long base = (work * amountToSend * w->config.DIM);
            long idx_base = work * amountToSend;

            if (work == (w->queryProcessorsId.size() - 1)) {
                // Last Worker will receive remaining data
                amountToSend = local_batch_size - amountSent;
            }

            // Optimization: Locally we can add directly to index
            if (w->id == workerId) {
                for (int i = 0; i < w->config.SCALE; i++) {
                    w->addNBSliceControl.lock();
                    w->index->add_with_ids(amountToSend, xb + base, ids.data() + idx_base);
                    w->amountQPReceived += amountToSend;
                    w->addNBSliceControl.unlock();
                }
            } else {
                w->MPIComm.sendNBSlice(ids.data() + idx_base, xb + base, amountToSend, workerId, w->PROCESS_COMM);
            }

            amountSent += vecPerWorkers;
        }

        delete [] xb;
        id_offset += local_batch_size;
        local_batch_size_send += local_batch_size;
    }
    // Clean
    dataset.~Data();
}

void Reader:: CentroidSplitNBSend()
{
    // Initial vector base to be sent
    Data dataset(w->config.BASE_FILE_PATH.data(), w->config.DIM, this->amountToRead, this->initNBId);

    // TODO revise this ceil
    long local_batch_size = floor ((float) this->amountToRead * w->config.NB_BATCH_PERC);
    long local_batch_size_send = 0;

    // TODO revise this ceil
    long batch_n = ceil ( (float) this->amountToRead / (float) local_batch_size );
    long id_offset = this->initNBId;

    int processedBatch = 0;

    while(processedBatch < batch_n) {

        if ( processedBatch == (batch_n - 1) ) {
            local_batch_size = this->amountToRead - local_batch_size_send;
        }

        float *xb = dataset.getNextBatch(local_batch_size);
        processedBatch += 1;

        cout << "[" << w->id << "] Sending NB Batch: " << processedBatch << "/" << batch_n << " (" << local_batch_size << ") " << endl;

        // Get the nearest coarse centroid of nb vectors
        int k = 1;
        auto *nearestCentroidPerNBEntry = new faiss::Index::idx_t[k * local_batch_size];
        w->coarse_centroids->assign(local_batch_size, xb, nearestCentroidPerNBEntry, k);

        // Find the correspondent RegionID given the nearest centroid
        map<long, set<long>> xbToRegion;
        for (long i = 0; i < local_batch_size; i++) {
            int nearestCentroid = nearestCentroidPerNBEntry[i];
            int targetRegion = w->getRegionByCentroid(nearestCentroid);
            xbToRegion[targetRegion].insert(i);

            // If our strategy will handle replications
            // We will also add this xb vector to another R QPs
            if (w->replicationAmount > 0) {
                // In this case we will get the replicationAmount nearests QPs
                for (int rep = 0; rep < w->replicationAmount; rep++) {
                    long nearestRegionFromTarget = w->nearbyRegions[targetRegion].at(rep);
                    xbToRegion[nearestRegionFromTarget].insert(i);
                }
            }
        }

        // For each region
        // Pack xb vectors that will be send to same QueryProcessor!
        for (int region = 0; region < w->queryProcessorsId.size(); region++) {
            // pack all vectors to send to query processor
            long amountToSend = xbToRegion[region].size();
            vector<float> data;
            for (auto vecId : xbToRegion[region]) {
                long vecOffset_b = vecId * w->config.DIM;
                long vecOffset_e = vecOffset_b + w->config.DIM;
                data.insert(data.end(), xb + vecOffset_b, xb + vecOffset_e);
            }

            // First get QP responsible for this region
            int queryProcessorID = w->queryProcessorsId.at(region);

            //Send idx slice plus batch offset
            // TODO LOOK AT THIS!
            vector<long> ids;
            for (auto idd: xbToRegion[region]) {
                ids.push_back(idd + id_offset);
            }

            // Optimization: Locally we can add directly to index
            if (w->id == queryProcessorID) {
                for (int i = 0; i < w->config.SCALE; i++) {
                    w->addNBSliceControl.lock();
                    w->index->add_with_ids(amountToSend, data.data(), ids.data());
                    w->amountQPReceived += amountToSend;
                    w->addNBSliceControl.unlock();
                }
            } else {
                w->MPIComm.sendNBSlice(ids.data(), data.data(), amountToSend, queryProcessorID, w->PROCESS_COMM);
            }
        }

        delete[] xb;
        delete[] nearestCentroidPerNBEntry;
        id_offset += local_batch_size;
        local_batch_size_send += local_batch_size;
    }
    // Clean
    dataset.~Data();
}

// Strategy 1
//
// Equally distribute dataset among query Processors
void Reader::DataEqualSplit()
{
    // In this strategy we will not do replication
    w->replicationAmount = 0;

    // Send or Get Data
    if (!w->config.READ_FROM_IVF_FILE) {
        this->EqualSplitNBSend();
    } else {
        this->ReadIVFEqually();
    }
}

// Strategy 2
//
// Coarse_centroids will be equally spllited into QueryProcessors amount
// Each query processor will be responsible for one piece
// Each query processor will receive only vectors of centroids that its is responsible
// WARNING: WE DO NOT WORK WITH REPLICATION IN THIS CASE
void Reader::BucketEqualSplit() {

    // Split coarse_centroids among QP Regions
    // Region = specific machine that have some data
    // Each Region will be keeped by a Query Processor
    int nRegions = w->queryProcessorsId.size();
    int totalCentroids = w->config.NCENTROIDS;
    int centroidsPerRegion = Data::round_closest(totalCentroids, nRegions);

    int amountDistrib = 0;
    for (int region = 0; region < nRegions; region++) {
        amountDistrib += centroidsPerRegion;
        if (region == nRegions - 1) {
            // means that query processor qp will hold centroids
            // ]this->coarseCentroidsDistrib[qp-1], this->coarseCentroidsDistrib[qp]]
            w->centroidsNotClusteredByRegion[region] = totalCentroids - 1;
        } else {
            w->centroidsNotClusteredByRegion[region] = amountDistrib - 1;
        }
    }

    // In this strategy we will not do replication
    w->replicationAmount = 0;

    // Send Data
    if (!w->config.READ_FROM_IVF_FILE) {
        this->CentroidSplitNBSend();
    } else {
        this->ReadIVFCentroids();
    }
}

// Strategy 3
//
// Coarse_centroids will be clustered into QueryProcessors clusters
// this clustered centroids will be used in coordinators to search query processor
// Each query processor will be responsible for one cluster
// Each query processor will receive only vectors of centroids that its is responsible
// Or vector from nearests query processors in case of replication
void Reader::SpatialAwareBucketEqualSplit()
{
    // Read pre processed cluster and data
    // Coordinator 0 already did it in train step
    if (w->id != 0) {
        Reader::ReadClusterCentroidsDataFromFile(*w);
    }

    // Pre process replication.
    w->ReplicationPreProcessingStep();

    // Send Data
    if (!w->config.READ_FROM_IVF_FILE) {
        this->CentroidSplitNBSend();
    } else {
        this->ReadIVFCentroids();
    }
}

/*
 * ======================================================
 * =============== HELPER FUNCTIONS =====================
 * ======================================================
 */

int Worker::getRegionByCentroid(int centroid)
{
    if (config.DB_DISTRIBUTION_STRATEGY == SABES_STRATEGY) {
        return getRegionByClusteredCentroids(centroid);
    }

    return getRegionByNotClusteredCentroids(centroid);
}

int Worker::getRegionByNotClusteredCentroids(int centroid)
{
    // ]this->coarseCentroidsDistrib[w-1], this->coarseCentroidsDistrib[w]]
    int currentPosition = 0;
    for (int qp = 0; qp < this->queryProcessorsId.size(); qp++) {
        if (centroid >= currentPosition) {
            if(centroid <= this->centroidsNotClusteredByRegion[qp]) {
                return qp;
            }
        }
        currentPosition = this->centroidsNotClusteredByRegion[qp] + 1;
    }
    // Something wrong
    return -1;
}

int Worker::getRegionByClusteredCentroids(int centroid)
{
    return centroidsRegions[centroid];
}

void Reader::ReadClusterCentroidsDataFromFile(Worker& worker)
{
    if (indexFileExists(worker.cc_cluster_file_name()) &&
        indexFileExists(worker.rep_region_file_name())) {
        // Read centroidsRegions from file
        std::ifstream ifs (worker.cc_cluster_file_name(),  std::ifstream::in);
        std::string line;
        while (std::getline(ifs, line)) {
            std::string field;
            std::vector<std::string> separated_fields;
            std::istringstream iss_line(line);
            while (std::getline(iss_line, field, ' ')) {
                separated_fields.push_back(field);
            }
            worker.centroidsRegions[stol(separated_fields[0])]
                    = stol(separated_fields[1]);
        }
        ifs.close();

        // Read centroidsRegions from file
        std::ifstream ifs2 (worker.rep_region_file_name(),  std::ifstream::in);
        while (std::getline(ifs2, line)) {
            std::string field;
            std::vector<std::string> separated_fields;
            std::istringstream iss_line(line);
            while (std::getline(iss_line, field, ' ')) {
                separated_fields.push_back(field);
            }

            long regId = stol(separated_fields[0]);
            int nClusters = worker.queryProcessorsId.size();
            for (int c = 1; c < nClusters; c++) {
                worker.nearbyRegions[regId].push_back(stol(separated_fields[c]));
            }
        }
        ifs2.close();
    } else {
        worker.logger->log(DEBUG, QUERY_PROCESSOR, worker.id, "SOMETHING WENT REALLY WRONG");
    }
}

void Reader::ReadTrainIndexFromFile(Worker& worker)
{
    // First of all, we need to read file setup from file
    if (indexFileExists(worker.config.OUT_COORD0_COARSE_INDEX_FILE)) {
        // Read Coarse centroids from file!
        worker.coarse_centroids =
                dynamic_cast<faiss::IndexFlatL2 *>(faiss::read_index(worker.config.OUT_COORD0_COARSE_INDEX_FILE.data(), 0));
        worker.index =
                dynamic_cast<faiss::IndexIVFPQ *>(faiss::read_index(worker.config.OUT_COORD0_INDEX_FILE.data(), 0));
        worker.index->nprobe = worker.config.NPROB;
    } else {
        worker.logger->log(DEBUG, QUERY_PROCESSOR, worker.id, "SOMETHING WENT REALLY WRONG");
    }
}

void Reader::ReadIVFEqually()
{
    if (find(w->queryProcessorsId.begin(), w->queryProcessorsId.end(), w->id) == w->queryProcessorsId.end()) {
        return;
    }

    map<long, vector<long>> regionsCentroids;
    int partition = 0;
    int slice = 0;
    for (long qpId : w->queryProcessorsId) {
        // Load all centroids
        for (int i = 0; i < w->config.NCENTROIDS; i++) {
            regionsCentroids[qpId].push_back(i);
        }
        if (qpId == w->id) {
            partition = slice;
        }
        slice++;
    }

    vector<int> slices;
    slices.push_back(w->queryProcessorsId.size());
    slices.push_back(partition);

    Reader::ReadIVFIndexFromFile(regionsCentroids[w->id], slices);
}

void Reader::ReadIVFCentroids()
{
    if (find(w->queryProcessorsId.begin(), w->queryProcessorsId.end(), w->id) == w->queryProcessorsId.end()) {
        return;
    }

    map<long, vector<long>> regionsCentroids;
    for (int i = 0; i < w->config.NCENTROIDS; i++) {
        long reg = w->getRegionByCentroid(i);
        long qpId = w->queryProcessorsId.at(reg);
        regionsCentroids[qpId].push_back(i);
        if (w->replicationAmount > 0) {
            for (int rep = 0; rep < w->replicationAmount; rep++) {
                long regNN = w->nearbyRegions[reg].at(rep);
                long qpNNId = w->queryProcessorsId.at(regNN);
                regionsCentroids[qpNNId].push_back(i);
            }
        }
    }

    // We just slice data in DES strategy
    vector<int> slices;
    slices.push_back(1);
    slices.push_back(0);

    Reader::ReadIVFIndexFromFile(regionsCentroids[w->id], slices);
}

void Reader::ReadIVFIndexFromFile(vector<long> centroids, vector<int> slices)
{
    w->temporalIndex.read(w->ivf_index_file_name(), std::move(centroids), std::move(slices));
}

void Reader::createNew()
{
    // Initial vector base to be sent
    Data dataset(w->config.BASE_FILE_PATH.data(), w->config.DIM, w->config.NB, 0);

    this->amountToRead = w->config.NB;
    // TODO revise this ceil
    long local_batch_size = floor ((float) this->amountToRead * w->config.NB_BATCH_PERC);
    long local_batch_size_send = 0;

    // TODO revise this ceil
    long batch_n = ceil ( (float) this->amountToRead / (float) local_batch_size );

    int processedBatch = 0;

    while(processedBatch < batch_n) {

        if ( processedBatch == (batch_n - 1) ) {
            local_batch_size = this->amountToRead - local_batch_size_send;
        }

        float *xb = dataset.getNextBatch(local_batch_size);
        processedBatch += 1;

        cout << "[" << w->id << "] Adding Batch: " << processedBatch << "/" << batch_n << " (" << local_batch_size << ") " << endl;

        w->index->add(local_batch_size, xb);

        cout << "Added batch: " << processedBatch << endl;
        delete [] xb;
        local_batch_size_send += local_batch_size;
    }
    // Clean
    dataset.~Data();

    { // Saving
        write_index(w->index, w->ivf_index_file_name());
    }

}

void Reader::createNewCheckPoint()
{
    long initialBatch = w->config.CHECKPOINT;

    if (initialBatch > 0) {
        cout << "Reading initial index: " << w->ivf_index_file_name_batch(initialBatch) << endl;
        w->index =
                dynamic_cast<faiss::IndexIVFPQ *>(faiss::read_index(w->ivf_index_file_name_batch(initialBatch), 0));
        w->index->precompute_table();
        cout << "Read and starting from batch: " << initialBatch + 1 << endl;
    }

    long batch_size = floor ((float) w->config.NB * w->config.NB_BATCH_PERC);
    long batch_size_send = 0;
    long batch_n = long (ceil ( (float) w->config.NB / (float) batch_size ));

    long initOffset = initialBatch * batch_size;
    this->amountToRead = w->config.NB - (initOffset);

    // Initial vector base to be sent
    cout << "InitialOffset: " << initOffset << " Amount to read: " << batch_size << endl;
    Data dataset(w->config.BASE_FILE_PATH.data(), w->config.DIM, batch_size, initOffset);
    cout << "InitialOffset: " << initOffset << " Amount to read: " << batch_size << endl;
    //int size =0;
    //for (int i = 0; i < w->index->nlist; i++) {
    //    size += w->index->invlists->list_size(i);
    //}
    //cout << "Initial Index Size: " << size << endl;

    int processedBatch = initialBatch;

    while(processedBatch < batch_n) {

        if ( processedBatch == (batch_n - 1) ) {
            batch_size_send = this->amountToRead - batch_size_send;
        }

        float *xb = dataset.getNextBatch(batch_size);
        processedBatch += 1;

        cout << "[" << w->id << "] Adding Batch: " << processedBatch << "/" << batch_n << " (" << batch_size << ") " << endl;

        w->index->add(batch_size, xb);

        cout << "Added batch: " << processedBatch << endl;

        delete [] xb;
        batch_size_send += batch_size_send;

        { // Saving
            write_index(w->index, w->ivf_index_file_name_batch(processedBatch));
        }

        dataset.~Data();
        initialBatch += 1;
        initOffset = initialBatch * batch_size;
        Data dataset(w->config.BASE_FILE_PATH.data(), w->config.DIM, batch_size, initOffset);
    }
    // Clean
    dataset.~Data();

    //size =0;
    //for (int i = 0; i < w->index->nlist; i++) {
    //    size += w->index->invlists->list_size(i);
    //}
    //cout << "Final Index Size: " << size << endl;

    //{ // Saving
    //    write_index(w->index, w->ivf_index_file_name());
    //}

}