//
// Created by gnandrade on 06/04/2020.
//

#include "utils/Logger.h"

Logger* Logger::instance = 0;

Logger* Logger::getInstance()
{
    if (instance == 0) {
        instance = new Logger();
    }

    return instance;
}

Logger::Logger()
{
    configLog();
}

void Logger::configLog()
{
    // this->showLogType = {INFO, ERROR, DEBUG};
     //this->showSource = {ENVIRONMENT, COORDINATOR,
     //                 QUERY_STREAMER, QUERY_PROCESSOR,
     //                 INDEX_STREAMER};

    this->showLogType = {};
    this->showSource = {};
}

void Logger::log(LogType type, Source workerType, int workerId, string message)
{
    if (count(this->showLogType.begin(), this->showLogType.end(), type) &&
        count(this->showSource.begin(), this->showSource.end(), workerType)) {
        printf("[%s][%s:%d\t]:\t%s\n", Time::getCurrentTimeFormate(),
               SourceStr.at(workerType).c_str(), workerId, message.c_str());
    }
}
