/*
 * Copyright 2016 Jacopo Urbani
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 **/

#include <kognac/kognac.h>
#include <kognac/compressor.h>
#include <kognac/logs.h>
#include <kognac/progargs.h>

#include <thread>

using namespace std;

void printHelp(const char *programName, ProgramArgs &desc) {
    cout << "Usage: " << programName << " [options]" << endl << endl;
    string s = desc.tostring();
    cout << s << endl;
}

void initParams(int argc, const char** argv, ProgramArgs &vm) {
    ProgramArgs::GroupArgs& g = *vm.newGroup("Options");

    int nHardwareThreads = std::thread::hardware_concurrency();
    if (nHardwareThreads == 0) {
        nHardwareThreads = 8;
    }
    int defaultConcurrentReaders = 2;

    if (nHardwareThreads % defaultConcurrentReaders != 0) {
        defaultConcurrentReaders = 1;
    }

    g.add<string>("i","input", "", "input directory", true)
        .add("l", "logLevel", INFOL, "Set the log level (accepted values: trace, debug, info, warning, error, fatal)", false)
        .add<string>("h","help", "", "print help message", false)
        .add<bool>("f","fp", false, "Use FPTree to mine classes", false)
        .add<int>("s","minSupport", 1000, "Sets the minimum support necessary to indentify class patterns", false)
        .add<int>("p","maxPatternLength", 3, "Sets the maximum length of class patterns", false)
        .add<int>("", "maxThreads", nHardwareThreads, "Sets the maximum number of threads to use during the compression", false)
        .add<int>("", "maxConcThreads", defaultConcurrentReaders, "Sets the number of concurrent threads that reads the raw input", false)
        .add<string>("o","output", "", "output directory", true)
        .add<bool>("", "serializeTax", false, "Should I also serialize the content of the classes' taxonomy on a file?", false)
        .add<bool>("c","compressGraph", false, "Should I also compress the graph. If set, I create a compressed version of the triples.", false)
        .add<int>("", "sampleArg1", 0, "Argument for the method to identify the popular terms. If the method is sample, then it represents the top k elements to extract. If it is hash or mgcs, then it indicates the number o  popular terms", true)
        .add<string>("", "sampleMethod", "cm", "Method to use to identify the popular terms. Can be either 'sample', 'cm', 'mgcs', 'cm_mgcs'", false)
        .add<int>("", "sampleArg2", 500, "This argument is used during the sampling procedure. It determines the sampling rate (x/10000)", false)
        .add<int64_t>("", "startCounter", 0, "Number to use to start assigning IDs.", false);

    vm.parse(argc, argv);
}

int main(int argc, const char **argv) {
    ProgramArgs vm;
    initParams(argc, argv, vm);

    if (argc == 1 || vm.count("help") || !vm.count("input")
            || !vm.count("output") || !vm.count("sampleArg1")) {
        printHelp(argv[0], vm);
        return 0;
    }
    vm.check();

    // Get parameters from command line
    Logger::setMinLevel(vm["logLevel"].as<int>());
    const string input = vm["input"].as<string>();
    const string output = vm["output"].as<std::string>();
    const int parallelThreads = vm["maxThreads"].as<int>();
    const int maxConcurrentThreads = vm["maxConcThreads"].as<int>();
    const int sampleArg = vm["sampleArg1"].as<int>();
    const int sampleArg2 = vm["sampleArg2"].as<int>();
    const bool compressGraph = vm["compressGraph"].as<bool>();
    const int maxPatternLength = vm["maxPatternLength"].as<int>();
    const int minSupport = vm["minSupport"].as<int>();
    const bool useFP = vm["fp"].as<bool>();
    const bool serializeTaxonomy = vm["serializeTax"].as<bool>();
    const int64_t startCounter = vm["startCounter"].as<int64_t>();
    int sampleMethod = PARSE_COUNTMIN;
    if (vm["sampleMethod"].as<string>() == string("sample")) {
        sampleMethod = PARSE_SAMPLE;
    } else if (vm["sampleMethod"].as<string>() == string("cm_mgcs")) {
        sampleMethod = PARSE_COUNTMIN_MGCS;
    } else if (vm["sampleMethod"].as<string>() != string("cm")) {
        cerr << "Unrecognized option " << vm["sampleMethod"].as<string>() << endl;
        return 1;
    }

    Kognac kognac(input, output, maxPatternLength);
    LOG(INFOL) << "Sampling the graph ...";
    kognac.sample(sampleMethod, sampleArg, sampleArg2, parallelThreads,
            maxConcurrentThreads);
    LOG(INFOL) << "Creating the dictionary mapping ...";
    kognac.compress(parallelThreads, maxConcurrentThreads, useFP, minSupport,
            serializeTaxonomy, startCounter);

    if (compressGraph) {
        LOG(INFOL) << "Compressing the triples ...";
        kognac.compressGraph(parallelThreads, maxConcurrentThreads);
    }
    LOG(INFOL) << "Done.";


    return 0;
}
