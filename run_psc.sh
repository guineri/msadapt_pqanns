#!/bin/sh
HOME_DIR="/home/freire/gnandrade/readivf/pqnns-multi-stream"
SCRATCH_DIR="/pylon5/ac3uump/freire/gnandrade"

#set variable so that task placement works as expected
export I_MPI_JOB_RESPECT_PROCESS_PLACEMENT=0
export OMP_NUM_THREADS=13

mkdir -p "${SCRATCH_DIR}/out/index/train/"
mkdir -p "${SCRATCH_DIR}/out/index/qp/"
mkdir -p "${SCRATCH_DIR}/out/result/"
mkdir -p "${SCRATCH_DIR}/out/cluster/"

EXEC_MODE=$1
READ_MODE=$2
COORDINATORS=$3
QUERY_PROCESSOR=$4
DATASET=$5
REPLICATION_RATE=$6
STRATEGY=$7
READERS=$8
SAVE=$9
SCALE=${10}
QUERY_STREAMER=1
INDEX_STREAMER=0
LOAD_FACTOR=1
READ_IVF="false"
GENERATE_NEW="false"

if [ $EXEC_MODE = "INDEX_CREATE" ]; then
  GENERATE_NEW="true"
  READ_MODE="none"
  COORDINATORS=1
  QUERY_PROCESSOR=0
  DATASET=$2
  REPLICATION_RATE=1
  STRATEGY=0
  READERS=0
  SAVE="false"
  SCALE=1
  QUERY_STREAMER=0
  INDEX_STREAMER=0
  LOAD_FACTOR=1

  echo "Config: $EXEC_MODE ${COORDINATORS} ${DATASET}"
else
  if [ $READ_MODE = "READ_IVF" ]; then
    READ_IVF="true"
  fi
  echo "Config: ${COORDINATORS} ${QUERY_PROCESSOR} ${DATASET} ${REPLICATION_RATE} ${STRATEGY} ${READERS} ${SAVE} ${SCALE}"
fi

N_WORKERS=$(($COORDINATORS+$QUERY_STREAMER+$INDEX_STREAMER+$QUERY_PROCESSOR))

echo "Config: ${COORDINATORS} ${QUERY_PROCESSOR} ${DATASET} ${REPLICATION_RATE} ${STRATEGY} ${READERS} ${SAVE} ${SCALE}"
OMP_NUM_THREADS=13 mpirun -np $N_WORKERS \
       -genv \
          OMP_NUM_THREADS=13 -genv I_MPI_PIN_DOMAIN=omp \
      ${HOME_DIR}/build/pqnns-multi-stream  \
          -home ${HOME_DIR} \
          -scratch ${SCRATCH_DIR} \
          -c ${COORDINATORS} \
          -qp ${QUERY_PROCESSOR} \
          -re ${READERS} \
          -lf ${LOAD_FACTOR} \
          -strategy ${STRATEGY} \
          -dataset ${DATASET} \
          -rep_rate ${REPLICATION_RATE} \
          -read_ivf ${READ_IVF} \
          -save ${SAVE} \
          -scale ${SCALE} \
          -create_index ${GENERATE_NEW}