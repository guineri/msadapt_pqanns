//
// Created by gnandrade on 05/04/2020.
//

#ifndef PQNNS_MULTI_STREAM_CONSTANTS_H
#define PQNNS_MULTI_STREAM_CONSTANTS_H

#include <string>
#include <vector>

#define INDEX_WINDOW_SIZE 10
#define UNIQUE_RESPONSE 1

#define MSG_TAG_STREAM_FINALIZE 4098
#define MSG_TAG_RESPONSE_FINALIZE 1
#define MSG_TAG_QUERY_RESPONSE 5
#define MSG_TAG_TRAIN_FINALIZE 4
#define MSG_TAG_TRAIN 6
#define MSG_TAG_HANDSHAKE 7
#define MSG_TAG_INDEX 9
#define MSG_TAG_QUERY 10

#define DES_STRATEGY 1
#define BES_STRATEGY 2
#define SABES_STRATEGY 3

using namespace std;

enum Source {
    COORDINATOR,
    QUERY_STREAMER,
    INDEX_STREAMER,
    QUERY_PROCESSOR,
    ENVIRONMENT,
    UNKNOWN
};

static const vector<string> SourceStr {
        "Coordinator", "QueryStreamer",
        "IndexStreamer", "QueryProcessor",
        "Environment", "Unknown"};

#endif // PQNNS_MULTI_STREAM_CONSTANTS_H
