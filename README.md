# About

TODO

# Dependencies
- [Cmake < 2.8](https://cmake.org/install/)
- [OpenMPI < 4.0.3](https://www.open-mpi.org/faq/?category=building)
- [Faiss](https://github.com/facebookresearch/faiss) 
- [C++ Boost](https://www.boost.org/doc/libs/1_66_0/more/getting_started/unix-variants.html)

# Build
```
sh compile.sh
```

# Dataset definition

To insert or configure a new dataset, edit file `data/datasets.json`. To insert a new one, just include a new json entry in array property `datasets` in `data/datasets.json`. Example:

```
{
   "datasets":[
      ...
      {
         "name":"NEW_DATASET",
         "dim":128,
         "k":10,
         "centroids":4096,
         "subquantizers":8,
         "w":16,
         "base":{
            "path":"/data/dataset/new_dataset_base.bvecs",
            "size":10000
         },
         "train":{
            "path":"/data/train/new_datasett_learn.bvecs",
            "size":10000
         },
         "query":{
            "path":"/data/query/new_dataset_query.bvecs",
            "size":100
         },
         "base_batch_percent": 1.0
      },
      ...
   ]
}
```

Properties:
- name: dataset name. Used in DATASET parameters of Executions step.
- dim: vectors dimensions
- k: k nearest neighbors
- centroids: number of centroids to train
- subquantizers: subquantizers amount
- w: probe
- base: path and size to database vectors
- train: path and size to train vectors
- query: path and size to query vectors
- base_batch_percent: Used in execution mode STREAM with READ_MODE=DISTRIBUTION. This property set max batch amout to send to query processors. (Ex.: base_batch_percent: 0.5 will generate 2 batchs of base.size / 2).

> :warning: At the moment the system just accept input as **bvecs or fvecs**

> :warning: base, train and query path property will be concatenated with `$SCRATCH` variable defined in `run.sh`. So, its property is a relative path from `$SCRATCH` as base: Ex.: `$SCRATCH + query.path`

# Executions

First, set in `run.sh`: 
- `$HOME_DIR`: Path to current pqnns-multi-stream folder
- `$SCRATCH_DIR`: path to storage (by default is the same of `$HOME_DIR`)
- `$OMP_THREADS_C`: Number o threads to Faiss library usage.

All dataset configurations are hardcoded in `src/utils/Config.cpp` 
(TODO: create an .ini file with dataset configurations)

Today we have: `SIFT1B` and `GIST` definitions in `data/datasets.json`


## Create New Index <INDEX_CREATE>

Execution mode designed to just create an IVFADC index with all the base vectors inserted.

```
./run.sh INDEX_CREATE <DATASET>
```

**Output:** IFVADC index will be saved in `$SCRATCH/out/index/qp/<NCENTROIDS>/index_full_db[<DATASET>]_cent[<NCENTROIDS>]`

## Stream <STREAM>

Execution mode in which QUERY STREAMERS sends query flow to COORDINATORS, which forward it to QUERY PROCESSORS, in which we perform a local search and send the result again to COORDINATOR where it makes and generates a global response of the query.

In this mode we perform steps (1) train (if not executed yet); (2) dataset distribution; (3) search. In dataset distribution, query processors can just read slices of the index directly from file (READ_MODE=READ_IVF), or READERS node read slices of database and send vectors to query processors by MPI (READ_MODE=DISTRIBUTED). READ_IVF is (far) faster, but requiere that INDEX_CREATE execution output must be present (file with IVFADC index filled).

Params: 
```
./run.sh STREAM <READ_MODE> \
                <N_COORDINATORS> \
                <N_QUERYPROCESSORS> \
                <DATASET> \
                <REPLICATION_RATE> \
                <STRATEGY> \
                <READERS> \
                <SAVE> \
                <SCALE>
```

- READ_MODE: Read mode could be: (1) **DISTRIBUTE**: Readers node will be reading database file and send index vector to Query Processors following some STRATEGY. (2) **READ_IVF**: Query Processors will be reading index slices following some <TRATEGY directly from filled index file (output of <INDEX_CREATE> execution mode).
  
- N_COORDINATORS: Number of coordinators

- N_QUERYPROCESSORS: Number of query processors

- DATASET: Dataset name (SIFT1B, GIST)

- REPLICATION_RATE: Replication rate, used for strategy 3. Percentual (0.0 = without replication, 0.5 = replicating to 1/2 of n_queryprocessors, etc...). For strategies 1 and 2 this parameter must be 0.
  
- STRATEGY: [1,2,3]: (1) DES; (2) BES; (3) SABES
- READERS: Just used for <READ_MODE>=DISTRIBUTED. Set number of nodes that will be reading dataset vectors and sending to query processors. Its value must be 1 < <READERS> < TOTAL_WORKERS. If READ_MODE>=READ_IVF this parameter will be 0.

- SAVE: true/false. Save a file with (for each query) idx of k nearest vectors.
  
- SCALE: Use to amplify or reduce dataset. 1 is ta neutral size. (...) 0.5=dataset_size x 0.5; 1=dataset_size; 2=dataset_size x 2; 3=dataset_size x 4 (...)

# Artigos p/ leitura

- Product Quantization for Nearest Neighbor Search 
  - Artigo inicial sobre IVFADC - base para o restante das leituras.
  
- Fast approximate nearest neighbors with automatic algorithm configuration
  - FLANN é uma das principais referências da área.
  
- http://ann-benchmarks.com/index.html
  - Framework para avaliação de algoritmos ANN
 
- Large-scale parallel similarity search with Product Quantization for online multimedia services
  - Nosso trabalho em paralelização do IVFADC para memória distribuída.
  
- Approximate similarity search for online multimedia services on distributed CPU-GPU platforms
  - Busca multimedia usando CPU-GPU com algoritmo multicurves.
  
- Billion-scale similarity search with GPUs
  - IVFADC em GPU.

- The Inverted Multi-Index, IEEE Transactions on Pattern Analysis and Machine Intelligence 37  
  - Mudança no IVFADC, múltiplos índices como uma maneira alternativa de achar pontos.

- Locally Optimized Product Quantization for Approximate Nearest Neighbor Search
  - Per cell quantizer.

- Polysemous codes, in: B. Leibe, J. Matas, N. Sebe, M. Welling (Eds.), Computer Vision – ECCV 2016
  - Estratégia interessante para representação de dados compactados.

- Streaming Similarity Search over one Billion Tweets using Parallel Locality-Sensitive Hashing
  - Busca por similaridade em streaming data usando LSH.
  
- Deep Product Quantization Module for Efficient Image Retrieval
  - Deep learning in IVFADC.
