//
// Created by gnandrade on 03/04/2020.
//
#include "Environment.h"

int main(int argc, char* argv[])
{
    Config conf;
    conf.loadConfig(argc, argv);

    Environment env(conf);
    env.config();
    env.startUp(argc, argv);
    env.terminate();
    return 0;
}
