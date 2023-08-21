#!/bin/bash
HOME_DIR="/home/gnandrade/Documentos/Doutorado/pqnns-multi-stream"
SCRATCH_DIR="/home/gnandrade/Documentos/Doutorado/pqnns-multi-stream"
OMP_THREADS_C=1

#set variable so that task placement works as expected
#export I_MPI_JOB_RESPECT_PROCESS_PLACEMENT=0
export OMP_NUM_THREADS=$OMP_THREADS_C

mkdir -p "${SCRATCH_DIR}/out/index/train/"
mkdir -p "${SCRATCH_DIR}/out/index/qp/"
mkdir -p "${SCRATCH_DIR}/out/result/"
mkdir -p "${SCRATCH_DIR}/out/cluster/"

rm -rf "${SCRATCH_DIR}/out/result/msc_test/"
mkdir -p "${SCRATCH_DIR}/out/result/msc_test/"

# Default arguments
READ_MODE="READ_IVF"
REPLICATION_RATE=0
READERS=0
READ_IVF="false"
GENERATE_NEW="false"
MS_TEST="true"

# Parsing parameters
POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -mode|--exec-mode)
    EXEC_MODE="$2"
    shift # past argument
    shift # past value
    ;;
    -d|--dataset)
    DATASET="$2"
    shift # past argument
    shift # past value
    ;;
    -checkpoint|--checkpoint)
    CHECKPOINT="$2"
    shift # past argument
    shift # past value
    ;;
    -ds|--distribution-strategy)
    STRATEGY="$2"
    shift # past argument
    shift # past value
    ;;
    -c|--coordinators)
    COORDINATORS="$2"
    shift # past argument
    ;;
    -qp|--query-processors)
    QUERY_PROCESSOR="$2"
    shift # past argument
    ;;
    -qs|--query-streamers)
    QUERY_STREAMER="$2"
    shift # past argument
    ;;
    -is|--insertion-streamers)
    INDEX_STREAMER="$2"
    shift # past argument
    ;;
    -scale|--scale)
    SCALE="$2"
    shift # past argument
    ;;
    -s|--save)
    SAVE=true
    shift # past argument
    ;;
    -qlf|-lfq|--query-load-factor)
    LOAD_FACTOR_QUERY="$2"
    shift # past argument
    shift # past value
    ;;
    -ilf|-lfi|--insertion-load-factor)
    LOAD_FACTOR_INSERT="$2"
    shift # past argument
    shift # past value
    ;;
    -qot|--query-outer-threads)
    OUTER_QUERY="$2"
    shift # past argument
    shift # past value
    ;;
    -iot|--insertion-outer-threads)
    OUTER_INSERT="$2"
    shift # past argument
    shift # past value
    ;;
    -mt|--max-threads)
    MAX_THREADS="$2"
    shift # past argument
    shift # past value
    ;;
    -scs|--stream-control-strategy)
    STREAM_CONTROL_STRATEGY="$2"
    shift # past argument
    shift # past value
    ;;
    -st|--stream-time)
    STREAM_TIME="$2"
    shift # past argument
    shift # past value
    ;;
    -ts|--temporal-slices)
    TEMPORAL_SLICES="$2"
    shift # past argument
    shift # past value
    ;;
    -bof|--bucket-outdated-flex)
    BUCKET_OUTDATED_FLEX="$2"
    shift # past argument
    shift # past value
    ;;
    -tui|--temporal-update-interval)
    TEMPORAL_UPDATED_INTERVAL="$2"
    shift # past argument
    shift # past value
    ;;
    -v|--verbose)
    VERBOSE=true
    shift # past argument
    ;;
    *)    # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    shift # past argument
    ;;
esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters


if [[ -z "$DATASET" ]];
then
  echo "Missing --dataset <DATASET>";
  exit;
fi

[[ -z "$STRATEGY" ]] && STRATEGY=DES;
[[ -z "$SAVE" ]] && SAVE=false;
[[ -z "$LOAD_FACTOR_QUERY" ]] && LOAD_FACTOR_QUERY=0.5;
[[ -z "$LOAD_FACTOR_INSERT" ]] && LOAD_FACTOR_INSERT=0.5;
[[ -z "$OUTER_QUERY" ]] && OUTER_QUERY=1;
[[ -z "$OUTER_INSERT" ]] && OUTER_INSERT=1;
[[ -z "$MAX_THREADS" ]] && MAX_THREADS=$(grep ^cpu\\scores /proc/cpuinfo | uniq |  awk '{print $4}');
[[ -z "$STREAM_CONTROL_STRATEGY" ]] && STREAM_CONTROL_STRATEGY=DEFAULT;
[[ -z "$STREAM_TIME" ]] && STREAM_TIME=60;
[[ -z "$TEMPORAL_SLICES" ]] && TEMPORAL_SLICES=1;
[[ -z "$BUCKET_OUTDATED_FLEX" ]] && BUCKET_OUTDATED_FLEX=600;
[[ -z "$TEMPORAL_UPDATED_INTERVAL" ]] && TEMPORAL_UPDATED_INTERVAL=600;
[[ -z "$COORDINATORS" ]] && COORDINATORS=0;
[[ -z "$QUERY_PROCESSOR" ]] && QUERY_PROCESSOR=1;
[[ -z "$QUERY_STREAMER" ]] && QUERY_STREAMER=0;
[[ -z "$INDEX_STREAMER" ]] && INDEX_STREAMER=0;
[[ -z "$SCALE" ]] && SCALE=1;
[[ -z "$EXEC_MODE" ]] && EXEC_MODE=MS_TEST;
[[ -z "$CHECKPOINT" ]] && CHECKPOINT=-1;

if [[ -n "$VERBOSE" ]];
then
  echo "DATASET                       = ${DATASET}"
  echo "STRATEGY                      = ${STRATEGY}"
  echo "SAVE                          = ${SAVE}"
  echo "LOAD_FACTOR_QUERY             = ${LOAD_FACTOR_QUERY}"
  echo "LOAD_FACTOR_INSERT            = ${LOAD_FACTOR_INSERT}"
  echo "OUTER_QUERY                   = ${OUTER_QUERY}"
  echo "OUTER_INSERT                  = ${OUTER_INSERT}"
  echo "MAX_THREADS                   = ${MAX_THREADS}"
  echo "STREAM_CONTROL_STRATEGY       = ${STREAM_CONTROL_STRATEGY}"
  echo "STREAM_TIME                   = ${STREAM_TIME}"
  echo "TEMPORAL_SLICES               = ${TEMPORAL_SLICES}"
  echo "BUCKET_OUTDATED_FLEX          = ${BUCKET_OUTDATED_FLEX}"
  echo "TEMPORAL_UPDATED_INTERVAL     = ${TEMPORAL_UPDATED_INTERVAL}"
  echo "COORDINATORS                  = ${COORDINATORS}"
  echo "QUERY_PROCESSOR               = ${QUERY_PROCESSOR}"
  echo "QUERY_STREAMER                = ${QUERY_STREAMER}"
  echo "INDEX_STREAMER                = ${INDEX_STREAMER}"
  echo "SCALE                         = ${SCALE}"
  echo "MS_TEST                       = ${MS_TEST}"
fi

if [ $READ_MODE = "READ_IVF" ]; then
  READ_IVF="true"
fi
if [ $EXEC_MODE = "MS_TEST" ]; then
  MS_TEST="true"
  COORDINATORS=0
  QUERY_PROCESSOR=1
  QUERY_STREAMER=0
  INDEX_STREAMER=0
fi
if [ $EXEC_MODE = "INDEX_CREATE" ]; then
  GENERATE_NEW="true"
  READ_MODE="none"
  COORDINATORS=1
  QUERY_PROCESSOR=0
  REPLICATION_RATE=1
  STRATEGY=0
  READERS=0
  SAVE="false"
  SCALE=1
  QUERY_STREAMER=0
  INDEX_STREAMER=0
  LOAD_FACTOR=1
  MS_TEST="false"
fi

if [ $EXEC_MODE = "STREAM" ]; then
  MS_TEST="false"
fi

[[ $STRATEGY == "DES" ]] && STRATEGY=1;
[[ $STRATEGY == "BES" ]] && STRATEGY=2;
[[ $STRATEGY == "SABES" ]] && STRATEGY=3;

[[ $STREAM_CONTROL_STRATEGY == "MS-ADAPT" ]] && STREAM_CONTROL_STRATEGY=1;
[[ $STREAM_CONTROL_STRATEGY == "DEFAULT" ]] && STREAM_CONTROL_STRATEGY=0;

N_WORKERS=$(($COORDINATORS+$QUERY_STREAMER+$INDEX_STREAMER+$QUERY_PROCESSOR))

OMP_NUM_THREADS=$OMP_THREADS_C
mpirun --bind-to none -np $N_WORKERS \
      ${HOME_DIR}/build/pqnns-multi-stream  \
          -home ${HOME_DIR} \
          -scratch ${SCRATCH_DIR} \
          -c ${COORDINATORS} \
          -qp ${QUERY_PROCESSOR} \
          -qs ${QUERY_STREAMER} \
          -is ${INDEX_STREAMER} \
          -re ${READERS} \
          -lfq ${LOAD_FACTOR_QUERY} \
          -lfi ${LOAD_FACTOR_INSERT} \
          -strategy ${STRATEGY} \
          -dataset ${DATASET} \
          -rep_rate ${REPLICATION_RATE} \
          -read_ivf ${READ_IVF} \
          -save ${SAVE} \
          -scale ${SCALE} \
          -create_index ${GENERATE_NEW} \
          -ms_test ${MS_TEST} \
          -outer_query ${OUTER_QUERY} \
          -outer_insert ${OUTER_INSERT} \
          -max-threads ${MAX_THREADS} \
          -stream-strategy ${STREAM_CONTROL_STRATEGY} \
          -stream-time ${STREAM_TIME} \
          -temporal-slices ${TEMPORAL_SLICES} \
          -bucket-outdated-flex ${BUCKET_OUTDATED_FLEX} \
          -temporal-update-interval ${TEMPORAL_UPDATED_INTERVAL} \
          -checkpoint ${CHECKPOINT}