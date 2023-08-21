#!/bin/sh

TESTPATH="results"
OUTPATH="bof_exploration"
mkdir -p $TESTPATH
mkdir -p $TESTPATH/$OUTPATH

## Global Parameters

TEMPORAL_SLICES=5
STREAM_TIME=30
TEMPORAL_UPDATED_INTERVAL=300
STREAM_CONTROL_STRATEGY=DEFAULT
MAX_THREADS=8
QOT=3
IOT=1
QLF=0.5
ILF=0.5

OLDIFS=$IFS; IFS=',';
for config in 30 15 10 5 3 2.5 2 1 0; do

	BUCKET_OUTDATED_FLEX=$config

	if [ "$QOT" = "MS-ADAPT" ]; then
		STREAM_CONTROL_STRATEGY=MS-ADAPT
		QOT=1
		IOT=1	
	fi

	./run_ms_test.sh --dataset SIFT1M \
						 --max-threads $MAX_THREADS \
						 --stream-time $STREAM_TIME \
					     --query-outer-threads $QOT \
						 --insertion-outer-threads $IOT \
						 --query-load-factor $QLF \
						 --insertion-load-factor $ILF \
						 --temporal-slices $TEMPORAL_SLICES \
						 --bucket-outdated-flex $BUCKET_OUTDATED_FLEX \
						 --temporal-update-interval $TEMPORAL_UPDATED_INTERVAL \
						 --stream-control-strategy $STREAM_CONTROL_STRATEGY > $TESTPATH/$OUTPATH/"${QOT}_${IOT}_${BUCKET_OUTDATED_FLEX}_${STREAM_CONTROL_STRATEGY}.out"
done;


