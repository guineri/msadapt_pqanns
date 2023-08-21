//
// Created by gnandrade on 18/03/2021.
//

#include "temporal/TemporalIndex.h"

void TemporalIndex::setup(Config config)
{
    for (int i = 0; i < config.NCENTROIDS; i++) {
        this->readWriteLock.push_back(new RWLock());
    }

    this->temporalSlice = config.TEMPORALSLICES;
    this->config = config;
    // First of all, we need to read file setup from file
    if (indexFileExists(config.OUT_COORD0_INDEX_FILE)
        && indexFileExists(config.OUT_COORD0_COARSE_INDEX_FILE))  {
        // Read Coarse centroids from file!
        this->coarse_centroids =
                dynamic_cast<faiss::IndexFlatL2 *>(faiss::read_index(config.OUT_COORD0_COARSE_INDEX_FILE.data(), 0));
        this->centroidsSize = this->coarse_centroids->ntotal;
        this->temporalIndex.push_back(this->emptyIndex());
    } else {
        cout << "Something went wrong!! TemporalIndex constructor" << endl;
    }
}

void TemporalIndex::read(const char * ivf_index_file_name, const vector<long>& centroids, vector<int> slices)
{
    if (indexFileExists(ivf_index_file_name)) {
        // Clear and read all from file
        this->temporalIndex.clear();
        this->indexSizes.clear();
        this->temporalIndexIds.clear();
        int staticSlices = this->temporalSlice - 1;
        int total = 0;
        for (int i = 0; i < staticSlices; i++) {
            vector<int> parts;
            parts.push_back(staticSlices * slices[0]);
            parts.push_back(i + (staticSlices * slices[1]));
            faiss::IndexIVFPQ *newIndex =
                    dynamic_cast<faiss::IndexIVFPQ *>(read_index_especial(ivf_index_file_name, centroids, parts,
                            this->config.SCALE, 0));
            newIndex->nprobe = config.NPROB;
            //newIndex->precompute_table();
            newIndex->use_precomputed_table = -1; // force desable
            this->temporalIndex.push_back(newIndex);
            this->temporalIndexIds.push_back(i);
            long lsize = 0;
            for (int j = 0; j < newIndex->nlist; j++) {
                lsize += newIndex->invlists->list_size(j);
            }
            this->indexSizes[i] = lsize;
            total += lsize;
        }
        
        cout << total << endl;
        // Always create a stream index to receive new data
        this->temporalIndex.push_back(this->emptyIndex());
    } else {
        cout << "Something went wrong!! TemporalIndex constructor" << endl;
    }
}

void TemporalIndex::search_thread(int n, const float *x, int k,
        std::pair<vector<long>, vector<float>> toSearch, vector<float> &innerThreadsResponses,
                                  mutex& innerThreadsMutex)
{
    double a = Time::getCurrentTime();
    double t = 0.0;
    vector<float> localAnswers;
    for (int i = 0; i < this->temporalIndex.size(); i++) {
        if (!toSearch.first.empty()) {
            bool toBlock = false;
            bool skip = false;
            if (i == (this->temporalIndex.size() - 1)) {
                toBlock = true;
                if (this->config.BUCKETOUTDATEDFLEX >= this->config.TEMPORALUPDATEINTERVAL) {
                    skip = true;
                }
            }

            double aa = Time::getCurrentTime();
            if (!skip) {
                long *I = new long[this->config.K];
                auto *D = new float[this->config.K];

                this->temporalIndex[i]->search_with_centroids(n, x, k,
                    &toSearch.first[0],
                    &toSearch.second[0],
                    D, I, toSearch.first.size(),
                    std::ref(this->readWriteLock),
                    toBlock);

                localAnswers.insert(localAnswers.end(), I, I + this->config.K);
                localAnswers.insert(localAnswers.end(), D, D + this->config.K);

                delete[] I;
                delete[] D;
            }
            t += (Time::getCurrentTime() - aa);
        }
    }

    innerThreadsMutex.lock();
    innerThreadsResponses.insert(innerThreadsResponses.end(), localAnswers.begin(), localAnswers.end());
    innerThreadsMutex.unlock();
    localAnswers.clear();
    double b = Time::getCurrentTime() - a;
    //printf("1\t%.9f\t%.9f\n", t, b);
}

void TemporalIndex::search(int n,  const float *x, int k, long* cidx, float* cdist, float* distances, long* labels,
                           pair<int, int> innerThreads)
{
    std::shared_lock lock(_temporalUpdateLock);
    // Setup Inner Threads
    double a = Time::getCurrentTime();
    mutex innerThreadsMutex;
    vector<float> innerThreadsResponses;
    vector<std::thread> innerSearchThreads;
    int bucketsN = this->config.NPROB;

    if (innerThreads.first > this->config.NPROB) {
        innerThreads.first = this->config.NPROB;
    }

    std::pair<vector<long>, vector<float>> toSearch;
    int totalBucketsDistributed = 0;
    for (int t = 0; t < innerThreads.first; t++) {
        int toSearchBucketsN = std::ceil((double) (bucketsN - totalBucketsDistributed) / (double) (innerThreads.first - t));
        if (t == (innerThreads.first -1)) {
            toSearchBucketsN = bucketsN - totalBucketsDistributed;
        }

        int startOffset = totalBucketsDistributed;

        toSearch.first.insert(toSearch.first.end(),
                cidx + startOffset,
                cidx + startOffset + toSearchBucketsN);
        toSearch.second.insert(toSearch.second.end(),
                cdist + startOffset,
                cdist + startOffset + toSearchBucketsN);

        totalBucketsDistributed += toSearchBucketsN;

        //Dispatch some Threads
        if (t != (innerThreads.first - 1)) {
            innerSearchThreads.emplace_back(&TemporalIndex::search_thread, this, n, x, k, toSearch,
                    std::ref(innerThreadsResponses), std::ref(innerThreadsMutex));
            //cpu_set_t cpuset;
            //CPU_ZERO(&cpuset);
            //CPU_SET(t + innerThreads.second + 1, &cpuset);
            //pthread_setaffinity_np(innerSearchThreads.back().native_handle(), sizeof(cpu_set_t), &cpuset);
        } else {
            break;
        }

        toSearch.first.clear();
        toSearch.first.shrink_to_fit();
        toSearch.second.clear();
        toSearch.second.shrink_to_fit();
    }

    // Last one using base Thread
    this->search_thread(n, x, k,  toSearch, std::ref(innerThreadsResponses), std::ref(innerThreadsMutex));

    // Join
    for (auto& t : innerSearchThreads) {
        t.join();
    }

    double m = Time::getCurrentTime();

    // Merge
    int indexAmount = innerThreads.first * this->temporalIndex.size();
    if (this->config.BUCKETOUTDATEDFLEX >= this->config.TEMPORALUPDATEINTERVAL) {
        indexAmount = innerThreads.first * (this->temporalIndex.size() -1);
    }
    this->merge(innerThreadsResponses, distances, labels, indexAmount);

    toSearch.first.clear();
    toSearch.first.shrink_to_fit();
    toSearch.second.clear();
    toSearch.second.shrink_to_fit();
    innerThreadsResponses.clear();
    innerThreadsResponses.shrink_to_fit();
    innerSearchThreads.clear();

    double c = Time::getCurrentTime() - m;
    double b = Time::getCurrentTime() - a;
}

void TemporalIndex::merge(vector<float> responses, float* distances, long* labels, int n_qp)
{
    int counter[n_qp];
    for (int i = 0; i < n_qp; i++) counter[i] = 0;

    for (int topi = 0; topi < this->config.K; topi++) {
        float bestDist = std::numeric_limits<float>::max();
        long bestId = -1;
        int fromShard = -1;

        for (int shard = 0; shard < n_qp; shard++) {
            if (counter[shard] == this->config.K) continue;

            if (responses[(shard * (this->config.K * 2)) + this->config.K + counter[shard]] < bestDist) {
                bestDist = responses[(shard * (this->config.K * 2)) + this->config.K + counter[shard]];
                bestId = responses[(shard * (this->config.K * 2)) + counter[shard]];
                fromShard = shard;
            }
        }
        labels[topi] = bestId;
        distances[topi] = bestDist;
        counter[fromShard]++;
    }
}

void TemporalIndex::insert(long n, float* data, long* centIdx, long* idx)
{
    std::shared_lock lock(_temporalUpdateLock);
    // Always add to last index
    int lastIndex = this->temporalIndex.size() - 1;
    long lastIndexId = this->temporalIndexIds[lastIndex];

    for (int i = 0; i < n; i++) {
        // Lock centIdx to insert
        this->readWriteLock[centIdx[i]]->wrlock();
        for (int r = 0; r < 5000; r++) {
            this->temporalIndex[lastIndex]->add_core_o(1, &data[i * this->config.DIM], idx, nullptr, &centIdx[i]);
        }
        this->readWriteLock[centIdx[i]]->wrunlock();
    }
}

void TemporalIndex::temporalUpdate()
{
    std::unique_lock lock(_temporalUpdateLock);
    cout << "Starting temporal update" << endl;
    // Remove first index
    // Clear first index memory
    this->temporalIndex.erase(this->temporalIndex.begin());
    int indexId = this->temporalIndexIds[0];
    this->temporalIndexIds.erase( this->temporalIndexIds.begin());
    this->indexSizes.erase(indexId);

    // Add new empty index in the end
    this->temporalIndex.push_back(this->emptyIndex());
    cout << "Finishing temporal update" << endl;
}

void TemporalIndex::clearStreamIndex()
{
    this->temporalIndex.erase(prev(this->temporalIndex.end()));
    int indexId = this->temporalIndexIds[this->temporalIndexIds.size() - 1];
    // Add new empty index in the end
    this->temporalIndex.push_back(this->emptyIndex());
    this->indexSizes[indexSizes.size() - 1];
}

void TemporalIndex::nearestBuckets(int n, float* data, long* idx, float* dist, int k)
{
    this->coarse_centroids->search(n, data, k, dist, idx);
}

void TemporalIndex::verifySizes()
{
    for (int i = 0; i < this->temporalIndex.size(); i++) {
        long size = 0;
        long lastNonZero = -1;
        for (int j = 0; j < this->temporalIndex[i]->nlist; j++) {
            size += this->temporalIndex[i]->invlists->list_size(j);
            if (this->temporalIndex[i]->invlists->list_size(j) > 0) {
                lastNonZero = j;
            }
        }
        int sliceId = this->temporalIndexIds[i];
        cout << "Slice: " << i << "(" << sliceId << "): " << this->indexSizes[sliceId] << " (" << size << ")";
        if (lastNonZero != -1) {
            cout << "[" << lastNonZero << "][" << this->temporalIndex[i]->invlists->get_ids(lastNonZero)[0] << "]";
        }
        cout << endl;
    }
}

faiss::IndexIVFPQ* TemporalIndex::emptyIndex() {
    long newId = 0;
    if (this->temporalIndexIds.size() > 0) {
        newId = this->temporalIndexIds.back() + 1;
    }
    this->temporalIndexIds.push_back(newId);
    this->indexSizes[newId] = 0;
    auto* newIndex = dynamic_cast<faiss::IndexIVFPQ *>(faiss::read_index(
            this->config.OUT_COORD0_INDEX_FILE.data(), 0));
    newIndex->nprobe = this->config.NPROB;
    return newIndex;
}