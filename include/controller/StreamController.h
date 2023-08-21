//
// Created by gnandrade on 24/03/2021.
//

#ifndef PQNNS_MULTI_STREAM_STREAMCONTROLLER_H
#define PQNNS_MULTI_STREAM_STREAMCONTROLLER_H

#include "Worker.h"
#include "workers/QueryProcessor.h"
#include "controller/SCSignals.h"
#include <unistd.h>

#include <thread>

using namespace std;

class StreamController {
public:
    void configure(Worker *w);
    void run();
    void msAdapt();
    void msAdaptLevel();
    void msAdaptLevel2();
    void setupNewConfiguration(int queryOuterThreads, int insertInnerThreads);

private:
    Worker *w;
    int maxThreads;
    int insertOuterThreads;
    int queryOuterThreads;

    // Initial configuration
    vector<vector<int>> qLevels {
                { 5 },
                { 10 },
                { 15 },
                { 20 },
                { 25 },
                { 30 },
                { 35 },
                { 40 },
                { 45 },
                { 50 },
                { 55 }
            };

    //vector<vector<int>> qLevels {
    //           { 1 },
    //            { 2 },
    //            { 3 }
    //        };

    vector<int> iLevels {
                 5,
                 10 ,
                 15 ,
                 20 ,
                 25 ,
                 30 ,
                 35 ,
                 40 ,
                 45 ,
                 50 ,
                 55 
            };

    //vector<int> iLevels {
    //             1,
    //             2,
    //             3
    //        };

    int insertOuterLevel;
    int queryOuterLevel;
    int queryInnerLevel;

    bool incrementQueryResourcesLevels(int level);
    bool decrementQueryResourcesLevels(int level);

    bool incrementInsertionResourcesLevels(int level);
    bool decrementInsertionResourcesLevels(int level);

    //MS-ADAPT Methods
    bool decrementQueryResources(double decreaseFactor);
    bool incrementQueryResources(double increaseFactor);

    bool decrementInsertionResources(double decreaseFactor);
    bool incrementInsertionResources(double increaseFactor);
};


#endif //PQNNS_MULTI_STREAM_STREAMCONTROLLER_H
