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

#include <kognac/sorter.h>
#include <kognac/filemerger.h>
#include <kognac/utils.h>
#include <kognac/lz4io.h>
#include <kognac/triplewriters.h>
#include <kognac/logs.h>

#include <vector>
#include <algorithm>

void Sorter::sortUnsortedFiles(vector<string> &inputFiles, string dir,
        string prefixOutputFiles, int fileSize) {
    SortedTripleWriter writer(dir, prefixOutputFiles, fileSize);
    for (vector<string>::iterator itr = inputFiles.begin();
            itr != inputFiles.end(); ++itr) {
        LOG(DEBUGL) << "Started reading " << *itr;
        LZ4Reader reader(*itr);
        const bool quad = reader.parseByte() != 0;
        while (!reader.isEof()) {
            int64_t t1 = reader.parseLong();
            int64_t t2 = reader.parseLong();
            int64_t t3 = reader.parseLong();
            if (quad) {
                int64_t count = reader.parseLong();
                writer.write(t1, t2, t3, count);
            } else {
                writer.write(t1, t2, t3);
            }
        }
        LOG(DEBUGL) << "Finished reading " << *itr;
        Utils::remove(*itr);
    }
}

void Sorter::sort(vector<string> &inputFiles, int filesPerMerge,
        string prefixOutputFiles) {
    int segment = 0;
    while (inputFiles.size() > 0) {
        //Take out the filesPerMerge lastFiles
        vector<string> inputForSorting;
        for (int i = 0; i < filesPerMerge && inputFiles.size() > 0; ++i) {
            inputForSorting.push_back(inputFiles.back());
            inputFiles.pop_back();
        }

        //Sort them and write a new file
        FileMerger<Triple> merger(inputForSorting);
        string fileOutput = prefixOutputFiles + string("-")
            + to_string(segment);
        LZ4Writer writer(fileOutput);
        while (!merger.isEmpty()) {
            Triple t = merger.get();
            t.writeTo(&writer);
        }

        //Delete the old files
        for (vector<string>::iterator itr = inputForSorting.begin();
                itr != inputForSorting.end(); ++itr) {
            Utils::remove(*itr);
        }
        segment++;
    }
}

bool TripleCmp(const Triple &t1, const Triple &t2) {
    if (t1.s < t2.s) {
        return true;
    } else if (t1.s == t2.s) {
        if (t1.p < t2.p) {
            return true;
        } else if (t1.p == t2.p) {
            return t1.o < t2.o;
        }
    }
    return false;
}

void Sorter::sortBufferAndWriteToFile(vector<Triple> &v, string fileOutput) {
    std::sort(v.begin(), v.end(), TripleCmp);
    LZ4Writer writer(fileOutput);
    for (vector<Triple>::iterator itr = v.begin(); itr != v.end(); ++itr) {
        itr->writeTo(&writer);
    }
}

void Sorter::mergeSort(string inputDir, int nThreads, bool initialSorting,
        int64_t fileSize, int filesPerMerge) {
    size_t filesInDir = 0;
    int iteration = 0;
    LOG(DEBUGL) << "nthreads=" << nThreads;

    /*** SORT THE ORIGINAL FILES IN BLOCKS OF N RECORDS ***/
    if (initialSorting) {
        vector<string> unsortedFiles = Utils::getFiles(inputDir);
        vector<string> *splits = new vector<string> [nThreads];
        //Give each file to a different split
        int currentSplit = 0;
        for (vector<string>::iterator itr = unsortedFiles.begin();
                itr != unsortedFiles.end(); ++itr) {
            splits[currentSplit].push_back(*itr);
            currentSplit = (currentSplit + 1) % nThreads;
        }
        //Sort the files
        std::thread *threads = new std::thread[nThreads - 1];
        for (int i = 1; i < nThreads; ++i) {
            string prefixOutputFile = DIR_SEP + string("sorted-inputfile-")
                + to_string(i);
            threads[i - 1] = std::thread(
                    std::bind(&Sorter::sortUnsortedFiles, splits[i], inputDir,
                        prefixOutputFile, static_cast<int>(fileSize)));
        }
        string prefixOutputFile = DIR_SEP + string("sorted-inputfile-0");
        sortUnsortedFiles(splits[0], inputDir, prefixOutputFile, static_cast<int>(fileSize));
        for (int i = 1; i < nThreads; ++i) {
            threads[i - 1].join();
        }
        delete[] threads;
        delete[] splits;
    }

    /*** MERGE SORT ***/
    LOG(DEBUGL) << "Start merge sorting procedure";
    do {
        //Read all the files and store them in a vector
        vector<string> files = Utils::getFiles(inputDir);
        filesInDir = files.size();
        if (files.size() <= nThreads) {
            return; //No need to do sorting
        }

        LOG(DEBUGL) << "(Sorted) files to merge: " << (uint64_t)files.size() << " maxLimit: " << nThreads;

        //Split the files in nThreads splits
        vector<string> *splits = new vector<string> [nThreads];
        int currentSplit = 0;
        for (vector<string>::iterator itr = files.begin(); itr != files.end();
                ++itr) {
            splits[currentSplit].push_back(*itr);
            currentSplit = (currentSplit + 1) % nThreads;
        }

        //Start the threads and wait until they are finished
        std::thread *threads = new std::thread[nThreads - 1];
        for (int i = 1; i < nThreads; ++i) {
            string prefixOutputFile = inputDir + DIR_SEP + string("merged-file-")
                + to_string(i) + string("-") + to_string(iteration);
            threads[i - 1] = std::thread(
                    std::bind(&Sorter::sort, splits[i], filesPerMerge,
                        prefixOutputFile));
        }
        string prefixOutputFile = inputDir + DIR_SEP + string("merged-file-0-")
            + to_string(iteration);
        sort(splits[0], filesPerMerge, prefixOutputFile);
        for (int i = 1; i < nThreads; ++i) {
            threads[i - 1].join();
        }

        delete[] threads;
        delete[] splits;
        iteration++;
    } while (filesInDir / filesPerMerge > nThreads);
}
