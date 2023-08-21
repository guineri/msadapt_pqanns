//
// Created by gnandrade on 06/04/2020.
//

#ifndef PQNNS_MULTI_STREAM_LOGGER_H
#define PQNNS_MULTI_STREAM_LOGGER_H

#include <stdio.h>

#include <algorithm>
#include <iostream>
#include <string>
#include <vector>

#include "Constants.h"
#include "utils/Time.h"

enum LogType { INFO, ERROR, DEBUG };

class Logger {
private:
    vector<LogType> showLogType;
    vector<Source> showSource;

    static Logger* instance;
    Logger();

public:
    static Logger* getInstance();
    void configLog();
    void log(LogType type, Source workerType, int workerId, string message);
};

#endif // PQNNS_MULTI_STREAM_LOGGER_H
