#!/bin/sh

TEMPORAL_SLICES=$1
BUCKET_OUTDATED_FLEX=$2
STREAM_TIME=$3
TEMPORAL_UPDATED_INTERVAL=$4
STREAM_CONTROL_STRATEGY=$5
MAX_THREADS=$6
QOT=$7
IOT=$8

#for QLF in 0.2 0.4 0.5 0.6 0.8 1.0; do
#	for ILF in 0.2 0.4 0.5 0.6 0.8 1.0; do
for QLF in 0.2 0.5 0.8; do
	for ILF in 0.2 0.5 0.8; do
		echo "[$QLF][$ILF]"
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
						 --stream-control-strategy $STREAM_CONTROL_STRATEGY
	done;
done;

