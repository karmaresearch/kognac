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

#include <kognac/compressor.h>
#include <kognac/filereader.h>
#include <kognac/schemaextractor.h>
#include <kognac/triplewriters.h>
#include <kognac/utils.h>
#include <kognac/hashfunctions.h>
#include <kognac/factory.h>
#include <kognac/lruset.h>
#include <kognac/flajolet.h>
#include <kognac/stringscol.h>
#include <kognac/MisraGries.h>
#include <kognac/sorter.h>
#include <kognac/filemerger.h>
#include <kognac/filemerger2.h>
#include <kognac/logs.h>
#include <kognac/consts.h>

#include <iostream>
#include <utility>
#include <cstdlib>

using namespace std;

bool lessTermFrequenciesDesc(const std::pair<string, int64_t> &p1,
        const std::pair<string, int64_t> &p2) {
    return p1.second > p2.second || (p1.second == p2.second && p1.first > p2.first);
}

bool sampledTermsSorter1(const std::pair<string, size_t> &p1,
        const std::pair<string, size_t> &p2) {
    return p1.first < p2.first;
}

bool sampledTermsSorter2(const std::pair<string, size_t> &p1,
        const std::pair<string, size_t> &p2) {
    return p1.second > p2.second;
}

bool cmpSampledTerms(const char* s1, const char* s2) {
    if (s1 == s2) {
        return true;
    }
    if (s1 && s2 && s1[0] == s2[0] && s1[1] == s2[1]) {
        int l = Utils::decode_short(s1, 0);
        return Utils::compare(s1, 2, 2 + l, s2, 2, 2 + l) == 0;
    }
    return false;
}

struct cmpInfoFiles {
    bool operator()(FileInfo i, FileInfo j) {
        return (i.size > j.size);
    }
} cmpInfoFiles;

Compressor::Compressor(string input, string kbPath) :
    input(input), kbPath(kbPath) {
        tmpFileNames = NULL;
        finalMap = new ByteArrayToNumberMap();
        poolForMap = new StringCollection(64 * 1024 * 1024);
        totalCount = 0;
        nTerms = 0;
        dictFileNames = NULL;
        uncommonDictFileNames = NULL;
    }

void Compressor::addPermutation(const int permutation, int &output) {
    output |= 1 << permutation;
}

void Compressor::parsePermutationSignature(int signature, int *output) {
    int p = 0, idx = 0;
    while (signature) {
        if (signature & 1) {
            output[idx++] = p;
        }
        signature = signature >> 1;
        p++;
    }
}

void Compressor::uncompressTriples(ParamsUncompressTriples params) {
    DiskReader *filesreader = params.reader;
    DiskLZ4Writer *fileswriter = params.writer;
    const int idwriter = params.idwriter;
    Hashtable *table1 = params.table1;
    Hashtable *table2 = params.table2;
    Hashtable *table3 = params.table3;
    SchemaExtractor *extractor = params.extractor;
    int64_t *distinctValues = params.distinctValues;
    std::vector<string> *resultsMGS = params.resultsMGS;
    size_t sizeHeap = params.sizeHeap;
    const bool ignorePredicates = params.ignorePredicates;

    int64_t count = 0;
    int64_t countNotValid = 0;
    const char *supportBuffer = NULL;

    char *supportBuffers[3];
    if (extractor != NULL) {
        supportBuffers[0] = new char[MAX_TERM_SIZE + 2];
        supportBuffers[1] = new char[MAX_TERM_SIZE + 2];
        supportBuffers[2] = new char[MAX_TERM_SIZE + 2];
    }

    MG *heap = NULL;
    if (resultsMGS != NULL) {
        heap = new MG(sizeHeap);
    }

    FlajoletMartin estimator;

    DiskReader::Buffer buffer = filesreader->getfile();
    while (buffer.b != NULL) {
        FileReader reader(buffer.b, buffer.size, buffer.gzipped);
        while (reader.parseTriple()) {
            if (reader.isTripleValid()) {
                count++;

                if (heap != NULL && count % 5000 == 0) {
                    //Dump the heap
                    std::vector<string> lt = heap->getPositiveTerms();
                    std::copy(lt.begin(), lt.end(), std::back_inserter(*resultsMGS));
                }

                int length;
                supportBuffer = reader.getCurrentS(length);

                if (heap != NULL) {
                    heap->add(supportBuffer, length);
                }

                if (extractor != NULL) {
                    Utils::encode_short(supportBuffers[0], length);
                    memcpy(supportBuffers[0] + 2, supportBuffer, length);
                }
                int64_t h1 = table1->add(supportBuffer, length);
                int64_t h2 = table2->add(supportBuffer, length);
                int64_t h3 = table3->add(supportBuffer, length);
                fileswriter->writeByte(idwriter, 0);
                estimator.addElement(h1, h2, h3);

                //This is an hack to save memcpy...
                fileswriter->writeVLong(idwriter, length + 2);
                fileswriter->writeShort(idwriter, length);
                fileswriter->writeRawArray(idwriter, supportBuffer, length);

                if (!ignorePredicates) {
                    supportBuffer = reader.getCurrentP(length);
                    if (heap != NULL) {
                        heap->add(supportBuffer, length);
                    }

                    if (extractor != NULL) {
                        Utils::encode_short(supportBuffers[1], length);
                        memcpy(supportBuffers[1] + 2, supportBuffer, length);
                    }
                    h1 = table1->add(supportBuffer, length);
                    h2 = table2->add(supportBuffer, length);
                    h3 = table3->add(supportBuffer, length);
                    fileswriter->writeByte(idwriter, 0);
                    estimator.addElement(h1, h2, h3);

                    fileswriter->writeVLong(idwriter, length + 2);
                    fileswriter->writeShort(idwriter, length);
                    fileswriter->writeRawArray(idwriter, supportBuffer, length);
                } else {
                    fileswriter->writeByte(idwriter, 0);
                    fileswriter->writeVLong(idwriter, 2);
                    fileswriter->writeShort(idwriter, 0);
                }

                supportBuffer = reader.getCurrentO(length);

                if (heap != NULL) {
                    heap->add(supportBuffer, length);
                }

                if (extractor != NULL) {
                    Utils::encode_short(supportBuffers[2], length);
                    memcpy(supportBuffers[2] + 2, supportBuffer, length);
                    extractor->extractSchema(supportBuffers);
                }
                h1 = table1->add(supportBuffer, length);
                h2 = table2->add(supportBuffer, length);
                h3 = table3->add(supportBuffer, length);
                fileswriter->writeByte(idwriter, 0);
                estimator.addElement(h1, h2, h3);

                fileswriter->writeVLong(idwriter, length + 2);
                fileswriter->writeShort(idwriter, length);
                fileswriter->writeRawArray(idwriter, supportBuffer, length);
            } else {
                countNotValid++;
            }
        }
        //Get next file
        filesreader->releasefile(buffer);
        buffer = filesreader->getfile();
    }

    fileswriter->setTerminated(idwriter);
    *distinctValues = estimator.estimateCardinality();

    if (extractor != NULL) {
        delete[] supportBuffers[0];
        delete[] supportBuffers[1];
        delete[] supportBuffers[2];
    }

    if (heap != NULL) {
        std::vector<string> lt = heap->getPositiveTerms();
        std::copy(lt.begin(), lt.end(), std::back_inserter(*resultsMGS));
        delete heap;
    }

    LOG(DEBUGL) << "Parsed triples: " << count << " not valid: " << countNotValid;
}

void Compressor::sampleTerm(const char *term, int sizeTerm, int sampleArg,
        int dictPartitions, GStringToNumberMap *map/*,
                                                     LRUSet *duplicateCache, LZ4Writer **dictFile*/) {
    if (abs(rand() % 10000) < sampleArg) {
        GStringToNumberMap::iterator itr = map->find(string(term + 2, sizeTerm - 2));
        if (itr != map->end()) {
            itr->second = itr->second + 1;
        } else {
            //char *newTerm = new char[sizeTerm];
            //memcpy(newTerm, (const char*) term, sizeTerm);
            map->insert(std::make_pair(string(term + 2, sizeTerm - 2), 1));
        }
    } else {
        /*if (!duplicateCache->exists(term)) {
          AnnotatedTerm t;
          t.term = term;
          t.size = sizeTerm;
          t.tripleIdAndPosition = -1;

        //Which partition?
        int partition = Utils::getPartition(term + 2, sizeTerm - 2,
        dictPartitions);
        //Add it into the file
        t.writeTo(dictFile[partition]);
        //Add it into the map
        duplicateCache->add(term);
        }*/
    }
}

#ifdef COUNTSKETCH

void Compressor :: uncompressTriplesForMGCS (vector<FileInfo> &files, MG *heap, CountSketch *cs, string outFile,
        SchemaExtractor *extractor, int64_t *distinctValues) {
    LZ4Writer out(outFile);
    int64_t count = 0;
    int64_t countNotValid = 0;
    const char *supportBuffer = NULL;

    char *supportBuffers[3];
    if (extractor != NULL) {
        supportBuffers[0] = new char[MAX_TERM_SIZE + 2];
        supportBuffers[1] = new char[MAX_TERM_SIZE + 2];
        supportBuffers[2] = new char[MAX_TERM_SIZE + 2];
    }

    FlajoletMartin estimator;

    for (int i = 0; i < files.size(); ++i) {
        FileReader reader(files[i]);
        while (reader.parseTriple()) {
            if (reader.isTripleValid()) {
                count++;
                int length;

                // Read S
                supportBuffer = reader.getCurrentS(length);

                if (extractor != NULL) {
                    Utils::encode_short(supportBuffers[0], length);
                    memcpy(supportBuffers[0] + 2, supportBuffer, length);
                }
                // Add to Misra-Gries heap
                heap->add(supportBuffer, length);

                // Add to Count Sketch and get the 3 keys for estimation
                int64_t h1, h2, h3;
                cs->add(supportBuffer, length, h1, h2, h3);


                out.writeByte(0);
                estimator.addElement(h1, h2, h3);

                //This is an hack to save memcpy...
                out.writeVLong(length + 2);
                out.writeShort(length);
                out.writeRawArray(supportBuffer, length);


                // Read P
                supportBuffer = reader.getCurrentP(length);
                if (extractor != NULL) {
                    Utils::encode_short(supportBuffers[1], length);
                    memcpy(supportBuffers[1] + 2, supportBuffer, length);
                }

                // Add to Misra-Gries heap
                heap->add(supportBuffer, length);

                // Add to COunt Sketch and get the 3 keys for estimation
                cs->add(supportBuffer, length, h1, h2, h3);

                out.writeByte(0);
                estimator.addElement(h1, h2, h3);

                out.writeVLong(length + 2);
                out.writeShort(length);
                out.writeRawArray(supportBuffer, length);


                // Read O
                supportBuffer = reader.getCurrentO(length);
                if (extractor != NULL) {
                    Utils::encode_short(supportBuffers[2], length);
                    memcpy(supportBuffers[2] + 2, supportBuffer, length);
                    extractor->extractSchema(supportBuffers);
                }

                // Add to Misra-Gries heap
                heap->add(supportBuffer, length);

                // Add to COunt Sketch and get the 3 keys for estimation
                cs->add(supportBuffer, length, h1, h2, h3);

                out.writeByte(0);
                estimator.addElement(h1, h2, h3);

                out.writeVLong(length + 2);
                out.writeShort(length);
                out.writeRawArray(supportBuffer, length);
            } else {
                countNotValid++;
            }
        }
    }

    *distinctValues = estimator.estimateCardinality();

    if (extractor != NULL) {
        delete[] supportBuffers[0];
        delete[] supportBuffers[1];
        delete[] supportBuffers[2];
    }

    LOG(DEBUGL) << "Parsed triples: " << count << " not valid: " << countNotValid;
}

void Compressor :: extractTermsForMGCS (ParamsExtractCommonTermProcedure params, const set<string>& freq, const CountSketch *cs) {
    string inputFile = params.inputFile;
    GStringToNumberMap *map = params.map;
    int dictPartitions = params.dictPartitions;
    string *dictFileName = params.dictFileName;
    //int maxMapSize = params.maxMapSize;

    int64_t tripleId = params.idProcess;
    int pos = 0;
    int parallelProcesses = params.parallelProcesses;
    string *udictFileName = params.singleTerms;
    const bool copyHashes = params.copyHashes;

    LZ4Reader reader(inputFile);
    map->set_empty_key(EMPTY_KEY);
    map->set_deleted_key(DELETED_KEY);

    //Create an array of files.
    LZ4Writer **dictFile = new LZ4Writer*[dictPartitions];
    LZ4Writer **udictFile = new LZ4Writer*[dictPartitions];
    for (int i = 0; i < dictPartitions; ++i) {
        dictFile[i] = new LZ4Writer(dictFileName[i]);
        udictFile[i] = new LZ4Writer(udictFileName[i]);
    }

    uint64_t countInfrequent = 0, countFrequent = 0;


    char *prevEntries[3];
    int sPrevEntries[3];
    if (copyHashes) {
        prevEntries[0] = new char[MAX_TERM_SIZE + 2];
        prevEntries[1] = new char[MAX_TERM_SIZE + 2];
    } else {
        prevEntries[0] = prevEntries[1] = NULL;
    }

    while (!reader.isEof()) {
        int sizeTerm = 0;
        reader.parseByte(); //Ignore it. Should always be 0
        const char *term = reader.parseString(sizeTerm);

        if (copyHashes) {
            if (pos != 2) {
                memcpy(prevEntries[pos], term, sizeTerm);
                sPrevEntries[pos] = sizeTerm;
            } else {
                prevEntries[2] = (char*)term;
                sPrevEntries[2] = sizeTerm;

                extractTermForMGCS(prevEntries[0], sPrevEntries[0], countFrequent, countInfrequent,
                        dictPartitions, copyHashes, tripleId, 0, prevEntries,
                        sPrevEntries, dictFile, udictFile, freq, cs);

                extractTermForMGCS(prevEntries[1], sPrevEntries[1], countFrequent, countInfrequent,
                        dictPartitions, copyHashes, tripleId, 1, prevEntries,
                        sPrevEntries, dictFile, udictFile, freq, cs);

                extractTermForMGCS(term, sizeTerm, countFrequent, countInfrequent, dictPartitions,
                        copyHashes, tripleId, pos, prevEntries, sPrevEntries, dictFile,
                        udictFile, freq, cs);
            }
        } else {
            extractTermForMGCS(term, sizeTerm, countFrequent, countInfrequent, dictPartitions,
                    copyHashes, tripleId, pos, prevEntries, sPrevEntries, dictFile,
                    udictFile, freq, cs);
        }

        pos = (pos + 1) % 3;
        if (pos == 0) {
            tripleId += parallelProcesses;
        }
    }

    LOG(DEBUGL) << "Hashtable size after extraction " << map->size() << ". Frequent terms " << countFrequent << " infrequent " << countInfrequent;

    if (copyHashes) {
        delete[] prevEntries[0];
        delete[] prevEntries[1];
    }

    for (int i = 0; i < dictPartitions; ++i) {
        delete dictFile[i];
        delete udictFile[i];
    }

    delete[] dictFile;
    delete[] udictFile;
}


void Compressor :: extractTermForMGCS(const char *term, const int sizeTerm, uint64_t& countFreq, uint64_t& countInfrequent,
        const int dictPartition, const bool copyHashes, const int64_t tripleId, const int pos,
        char **prevEntries, int *sPrevEntries, LZ4Writer **dictFile, LZ4Writer **udictFile,
        const set<string>& freq, const CountSketch *cs) {
    char *trm = new char[sizeTerm - 1];
    memcpy(trm, term + 2, sizeTerm - 2);
    trm[sizeTerm - 2] = '\0';
    string str(trm);

    if (freq.find(str) == freq.end()) { // Term infrequent
        countInfrequent++;
        int partition = Utils::getPartition(term + 2, sizeTerm - 2, dictPartition);
        AnnotatedTerm t;
        t.size = sizeTerm;
        t.term = term;
        t.tripleIdAndPosition = (int64_t) (tripleId << 2) + (pos & 0x3);

        if (!copyHashes) {
            //Add it into the file
            t.useHashes = false;
        } else {
            //Output the three pairs
            t.useHashes = true;
            if (pos == 0) {
                int64_t hashp = Hashes::murmur3_56(prevEntries[1] + 2, sPrevEntries[1] - 2);
                int64_t hasho = Hashes::murmur3_56(prevEntries[2] + 2, sPrevEntries[2] - 2);
                t.hashT1 = hashp;
                t.hashT2 = hasho;
            } else if (pos == 1) {
                int64_t hashs = Hashes::murmur3_56(prevEntries[0] + 2, sPrevEntries[0] - 2);
                int64_t hasho = Hashes::murmur3_56(prevEntries[2] + 2, sPrevEntries[2] - 2);
                t.hashT1 = hashs;
                t.hashT2 = hasho;
            } else {
                //pos = 2
                int64_t hashs = Hashes::murmur3_56(prevEntries[0] + 2, sPrevEntries[0] - 2);
                int64_t hashp = Hashes::murmur3_56(prevEntries[1] + 2, sPrevEntries[1] - 2);
                t.hashT1 = hashs;
                t.hashT2 = hashp;
            }
        }

        t.writeTo(udictFile[partition]);
    }
}
#endif

void Compressor::uncompressAndSampleTriples(vector<FileInfo> &files,
        string outFile, string *dictFileName, int dictPartitions, int sampleArg,
        GStringToNumberMap *map, SchemaExtractor *extractor) {
    //Triples
    LZ4Writer out(outFile);
    map->set_empty_key(EMPTY_KEY);

    char *supportBuffers[3];
    if (extractor != NULL) {
        supportBuffers[0] = new char[MAX_TERM_SIZE + 2];
        supportBuffers[1] = new char[MAX_TERM_SIZE + 2];
        supportBuffers[2] = new char[MAX_TERM_SIZE + 2];
    }

    int64_t count = 0;
    int64_t countNotValid = 0;
    const char *supportBuffer = NULL;
    char *supportBuffer2 = new char[MAX_TERM_SIZE];
    for (int i = 0; i < files.size(); ++i) {
        FileReader reader(files[i]);
        while (reader.parseTriple()) {
            if (reader.isTripleValid()) {
                count++;

                int length;
                supportBuffer = reader.getCurrentS(length);
                Utils::encode_short(supportBuffer2, length);
                memcpy(supportBuffer2 + 2, supportBuffer, length);

                out.writeByte(0);
                out.writeString(supportBuffer2, length + 2);

                sampleTerm(supportBuffer2, length + 2, sampleArg,
                        dictPartitions, map/*, &duplicateCache, dictFile*/);

                if (extractor != NULL) {
                    memcpy(supportBuffers[0], supportBuffer2, length + 2);
                }

                supportBuffer = reader.getCurrentP(length);
                Utils::encode_short(supportBuffer2, length);
                memcpy(supportBuffer2 + 2, supportBuffer, length);

                out.writeByte(0);
                out.writeString(supportBuffer2, length + 2);

                sampleTerm(supportBuffer2, length + 2, sampleArg,
                        dictPartitions, map/*, &duplicateCache, dictFile*/);

                if (extractor != NULL) {
                    memcpy(supportBuffers[1], supportBuffer2, length + 2);
                }


                supportBuffer = reader.getCurrentO(length);
                Utils::encode_short(supportBuffer2, length);
                memcpy(supportBuffer2 + 2, supportBuffer, length);

                out.writeByte(0);
                out.writeString(supportBuffer2, length + 2);

                sampleTerm(supportBuffer2, length + 2, sampleArg,
                        dictPartitions, map/*, &duplicateCache, dictFile*/);

                if (extractor != NULL) {
                    memcpy(supportBuffers[2], supportBuffer2, length + 2);
                    extractor->extractSchema(supportBuffers);
                }

            } else {
                countNotValid++;
            }
        }
    }

    if (extractor != NULL) {
        delete[] supportBuffers[0];
        delete[] supportBuffers[1];
        delete[] supportBuffers[2];
    }

    delete[] supportBuffer2;
    LOG(DEBUGL) << "Parsed triples: " << count << " not valid: " << countNotValid;
}

void Compressor::extractUncommonTerm(const char *term, const int sizeTerm,
        ByteArrayToNumberMap *map,
        const int idwriter,
        DiskLZ4Writer *writer,
        const int64_t tripleId,
        const int pos,
        const int partitions,
        const bool copyHashes,
        char **prevEntries, int *sPrevEntries) {

    if (map->find(term) == map->end()) {
        if (!copyHashes) {
            //Use the simpler data structure
            SimplifiedAnnotatedTerm t;
            t.size = sizeTerm - 2;
            t.term = term + 2;
            t.tripleIdAndPosition = (int64_t) (tripleId << 2) + (pos & 0x3);
            t.writeTo(idwriter, writer);
        } else {
            AnnotatedTerm t;
            t.size = sizeTerm;
            t.term = term;
            t.tripleIdAndPosition = (int64_t) (tripleId << 2) + (pos & 0x3);
            //Output the three pairs
            t.useHashes = true;
            if (pos == 0) {
                int64_t hashp = Hashes::murmur3_56(prevEntries[1] + 2, sPrevEntries[1] - 2);
                int64_t hasho = Hashes::murmur3_56(prevEntries[2] + 2, sPrevEntries[2] - 2);
                t.hashT1 = hashp;
                t.hashT2 = hasho;
            } else if (pos == 1) {
                int64_t hashs = Hashes::murmur3_56(prevEntries[0] + 2, sPrevEntries[0] - 2);
                int64_t hasho = Hashes::murmur3_56(prevEntries[2] + 2, sPrevEntries[2] - 2);
                t.hashT1 = hashs;
                t.hashT2 = hasho;
            } else { //pos = 2
                int64_t hashs = Hashes::murmur3_56(prevEntries[0] + 2, sPrevEntries[0] - 2);
                int64_t hashp = Hashes::murmur3_56(prevEntries[1] + 2, sPrevEntries[1] - 2);
                t.hashT1 = hashs;
                t.hashT2 = hashp;
            }
            t.writeTo(idwriter, writer);
        }
    } else {
        //What happen here?
    }
}

uint64_t Compressor::getEstimatedFrequency(const string &e) const {
    int64_t v1 = table1 ? table1->get(e.c_str(), static_cast<int>(e.size())) : 0;
    int64_t v2 = table2 ? table2->get(e.c_str(), static_cast<int>(e.size())) : 0;
    int64_t v3 = table3 ? table3->get(e.c_str(), static_cast<int>(e.size())) : 0;
    return min(v1, min(v2, v3));
}

void Compressor::extractCommonTerm(const char* term, const int sizeTerm,
        int64_t &countFrequent,
        const int64_t thresholdForUncommon, Hashtable *table1,
        Hashtable *table2, Hashtable *table3,
        const int dictPartitions,
        int64_t &minValueToBeAdded,
        const uint64_t maxMapSize,  GStringToNumberMap *map,
        std::priority_queue<std::pair<string, int64_t>,
        std::vector<std::pair<string, int64_t> >,
        priorityQueueOrder> &queue) {

    int64_t v1, v2, v3;

    bool valueHighEnough = false;
    v1 = table1->get(term + 2, sizeTerm - 2);
    int64_t minValue = -1;
    if (v1 < thresholdForUncommon) {
        //termInfrequent = true;
    } else {
        v2 = table2->get(term + 2, sizeTerm - 2);
        if (v2 < thresholdForUncommon) {
            //termInfrequent = true;
        } else {
            v3 = table3->get(term + 2, sizeTerm - 2);
            if (v3 < thresholdForUncommon) {
                //termInfrequent = true;
            } else {
                minValue = min(v1, min(v2, v3));
                valueHighEnough = minValue > minValueToBeAdded;
            }
        }
    }
    countFrequent++;
    bool mapTooSmall = map->size() < maxMapSize;
    if (mapTooSmall || valueHighEnough) {
	std::string sterm(term + 2, sizeTerm - 2);
        if (map->find(sterm) == map->end()) {
	    std::pair<string, int64_t> pair = std::make_pair(sterm, minValue);
	    map->insert(pair);
	    queue.push(pair);
	    if (! mapTooSmall) {
		//Replace term and minCount with values to be added
		std::pair<string, int64_t> elToRemove = queue.top();
		queue.pop();
		map->erase(elToRemove.first);
		minValueToBeAdded = queue.top().second;
	    }
        }
    }
}

void Compressor::extractUncommonTerms(const int dictPartitions,
        DiskLZ4Reader *reader,
        const int inputFileId,
        const bool copyHashes, const int idProcess,
        const int parallelProcesses,
        DiskLZ4Writer *writer,
        const bool ignorePredicates) {

    char *prevEntries[3];
    int sPrevEntries[3];
    if (copyHashes) {
        prevEntries[0] = new char[MAX_TERM_SIZE + 2];
        prevEntries[1] = new char[MAX_TERM_SIZE + 2];
    } else {
        prevEntries[0] = prevEntries[1] = NULL;
    }
    int64_t tripleId = idProcess;
    int pos = 0;

    while (!reader->isEOF(inputFileId)) {
        int sizeTerm = 0;
        int flag = reader->readByte(inputFileId); //Ignore it. Should always be 0
        if (flag != 0) {
            LOG(ERRORL) << "Flag should always be zero!";
            throw 10;
        }

        const char *term = reader->readString(inputFileId, sizeTerm);
        if (copyHashes) {
            if (pos != 2) {
                memcpy(prevEntries[pos], term, sizeTerm);
                sPrevEntries[pos] = sizeTerm;
            } else {
                prevEntries[2] = (char*)term;
                sPrevEntries[2] = sizeTerm;

                extractUncommonTerm(prevEntries[0], sPrevEntries[0], finalMap,
                        inputFileId,
                        writer, tripleId, 0,
                        dictPartitions, copyHashes,
                        prevEntries, sPrevEntries);

                if (ignorePredicates) {
                    //Do nothing. The procedure extractUncommonTerm only extracts the uncommon terms
                } else {
                    extractUncommonTerm(prevEntries[1], sPrevEntries[1], finalMap,
                            inputFileId,
                            writer, tripleId, 1,
                            dictPartitions, copyHashes,
                            prevEntries, sPrevEntries);
                }

                extractUncommonTerm(term, sizeTerm, finalMap,
                        inputFileId,
                        writer, tripleId, pos,
                        dictPartitions, copyHashes,
                        prevEntries, sPrevEntries);
            }
        } else {
            if (ignorePredicates && pos == 1) {
                //Do nothing. The procedure extractUncommonTerm only extracts the uncommon terms
            } else {
                extractUncommonTerm(term, sizeTerm, finalMap,
                        inputFileId,
                        writer, tripleId, pos,
                        dictPartitions, copyHashes,
                        prevEntries, sPrevEntries);
            }
        }

        pos = (pos + 1) % 3;
        if (pos == 0) {
            tripleId += parallelProcesses;
        }
    }

    writer->setTerminated(inputFileId);

    if (copyHashes) {
        delete[] prevEntries[0];
        delete[] prevEntries[1];
    }
}

void Compressor::extractCommonTerms(ParamsExtractCommonTermProcedure params) {
    DiskLZ4Reader *reader = params.reader;
    const int idReader = params.idReader;
    Hashtable **tables = params.tables;
    GStringToNumberMap *map = params.map;
    int dictPartitions = params.dictPartitions;
    int maxMapSize = params.maxMapSize;
    const bool ignorePredicates = params.ignorePredicates;

    int pos = 0;
    uint64_t thresholdForUncommon = params.thresholdForUncommon;

    Hashtable *table1 = tables[0];
    Hashtable *table2 = tables[1];
    Hashtable *table3 = tables[2];

    map->set_empty_key(EMPTY_KEY);
    map->set_deleted_key(DELETED_KEY);

    int64_t minValueToBeAdded = 0;
    std::priority_queue<std::pair<string, int64_t>,
        std::vector<std::pair<string, int64_t> >, priorityQueueOrder> queue;

    int64_t countFrequent = 0;

    while (!reader->isEOF(idReader)) {
        int sizeTerm = 0;
        reader->readByte(idReader); //Ignore it. Flag should always be 0
        const char *term = reader->readString(idReader, sizeTerm);
        if (!ignorePredicates || pos != 1) {
            extractCommonTerm(term, sizeTerm, countFrequent,
                    thresholdForUncommon, table1, table2, table3,
                    dictPartitions, minValueToBeAdded,
                    maxMapSize, map, queue);
        }

        pos = (pos + 1) % 3;
    }

    LOG(DEBUGL) << "Hashtable size after extraction " << (uint64_t)map->size() << ". Frequent terms " << countFrequent;

}

void Compressor::mergeCommonTermsMaps(ByteArrayToNumberMap *finalMap,
        GStringToNumberMap *maps, int nmaps) {
    char supportTerm[MAX_TERM_SIZE];
    for (int i = 0; i < nmaps; i++) {
        for (GStringToNumberMap::iterator itr = maps[i].begin();
                itr != maps[i].end(); ++itr) {
            Utils::encode_short(supportTerm, static_cast<int>(itr->first.size()));
            memcpy(supportTerm + 2, itr->first.c_str(), itr->first.size());

            ByteArrayToNumberMap::iterator foundValue = finalMap->find(supportTerm);
            if (foundValue == finalMap->end()) {
                const char *newkey = poolForMap->addNew(supportTerm,
                        Utils::decode_short(supportTerm) + 2);
                finalMap->insert(std::make_pair(newkey, itr->second));
            }
        }
        maps[i].clear();
    }
}

bool comparePairs(std::pair<const char *, int64_t> i,
        std::pair<const char *, int64_t> j) {
    if (i.second > j.second) {
        return true;
    } else if (i.second == j.second) {
        int s1 = Utils::decode_short((char*) i.first);
        int s2 = Utils::decode_short((char*) j.first);
        return Utils::compare(i.first, 2, s1 + 2, j.first, 2, s2 + 2) > 0;
    } else {
        return false;
    }
}

void Compressor::assignNumbersToCommonTermsMap(ByteArrayToNumberMap *map,
        int64_t *counters, LZ4Writer **writers, LZ4Writer **invWriters,
        int ndictionaries, bool preserveMapping) {
    std::vector<std::pair<const char *, int64_t> > pairs;
    for (ByteArrayToNumberMap::iterator itr = map->begin(); itr != map->end();
            ++itr) {
        std::pair<const char *, int64_t> pair;
        pair.first = itr->first;
        pair.second = itr->second;
        pairs.push_back(pair);

#ifdef DEBUG
        /*        const char* text = SchemaExtractor::support.addNew(itr->first, Utils::decode_short(itr->first) + 2);
                  int64_t hash = Hashes::murmur3_56(itr->first + 2, Utils::decode_short(itr->first));
                  SchemaExtractor::properties.insert(make_pair(hash, text));*/
#endif
    }
    std::sort(pairs.begin(), pairs.end(), &comparePairs);

    int counterIdx = 0;
    for (int i = 0; i < pairs.size(); ++i) {
        nTerm key;
        ByteArrayToNumberMap::iterator itr = map->find(pairs[i].first);
        int size = Utils::decode_short(pairs[i].first) + 2;
        int part = Utils::getPartition(pairs[i].first + 2,
                Utils::decode_short(pairs[i].first), ndictionaries);

        if (preserveMapping) {
            key = counters[part];
            counters[part] += ndictionaries;
        } else {
            key = counters[counterIdx];
            counters[counterIdx] += ndictionaries;
            counterIdx = (counterIdx + 1) % ndictionaries;
        }
        writers[part]->writeLong(key);
        writers[part]->writeString(pairs[i].first, size);
        itr->second = key;

        //This is needed for the smart compression
        if (invWriters != NULL) {
            int part2 = key % ndictionaries;
            invWriters[part2]->writeLong(key);
            invWriters[part2]->writeString(pairs[i].first, size);
        }
    }
}

void Compressor::newCompressTriples(ParamsNewCompressProcedure params) {
    int64_t compressedTriples = 0;
    int64_t compressedTerms = 0;
    int64_t uncompressedTerms = 0;
    DiskLZ4Reader *uncommonTermsReader = params.readerUncommonTerms;
    DiskLZ4Reader *r = params.reader;
    const int idReader = params.idReader;
    const bool ignorePredicates = params.ignorePredicates;

    const int nperms = params.nperms;
    MultiDiskLZ4Writer *writer = params.writer;
    const int startIdxWriter = params.idxWriter;
    int detailPerms[6];
    Compressor::parsePermutationSignature(params.signaturePerms, detailPerms);

    int64_t nextTripleId = -1;
    int nextPos = -1;
    int64_t nextTerm = -1;

    if (!uncommonTermsReader->isEOF(idReader)) {
        int64_t tripleId = uncommonTermsReader->readLong(idReader);
        nextTripleId = tripleId >> 2;
        nextPos = tripleId & 0x3;
        nextTerm = uncommonTermsReader->readLong(idReader);
    } else {
        LOG(DEBUGL) << "No uncommon file is provided";
    }

    int64_t currentTripleId = params.part;
    int increment = params.parallelProcesses;

    int64_t triple[3];
    char *tTriple = new char[MAX_TERM_SIZE * 3];
    bool valid[3];

    while (!r->isEOF(idReader)) {
        for (int i = 0; i < 3; ++i) {
            valid[i] = false;
            int flag = r->readByte(idReader);
            if (flag == 1) {
                //convert number
                triple[i] = r->readLong(idReader);
                valid[i] = true;
            } else {
                //Match the text against the hashmap
                int size;
                const char *tTerm = r->readString(idReader, size);

                if (currentTripleId == nextTripleId && nextPos == i) {
                    triple[i] = nextTerm;
                    valid[i] = true;
                    if (!uncommonTermsReader->isEOF(idReader)) {
                        int64_t tripleId = uncommonTermsReader->readLong(idReader);
                        nextTripleId = tripleId >> 2;
                        nextPos = tripleId & 0x3;
                        int64_t n2 = uncommonTermsReader->readLong(idReader);
                        nextTerm = n2;
                    } else {
                        //BOOST_LOG_TRIVIAL(debug) << "File " << idReader << " is finished";
                    }
                    compressedTerms++;
                } else {
                    bool ok = false;
                    if (ignorePredicates && i == 1) {
                        if (size != 2) {
                            LOG(ERRORL) << "Strange. This term should be empty ...";
                            throw 10;
                        }
                        triple[i] = 0; //Give the ID 0 to all predicates
                        valid[i] = true;
                        ok = true;
                        compressedTerms++;
                    } else {
                        //Check the hashmap
                        if (params.commonMap != NULL) {
                            ByteArrayToNumberMap::iterator itr =
                                params.commonMap->find(tTerm);
                            if (itr != params.commonMap->end()) {
                                triple[i] = itr->second;
                                valid[i] = true;
                                ok = true;
                                compressedTerms++;
                            }
                        }
                    }

                    if (!ok) {
                        throw 10;
                    }
                }
            }
        }

        if (valid[0] && valid[1] && valid[2]) {
            for (int i = 0; i < nperms; ++i) {
                switch (detailPerms[i]) {
                    case IDX_SPO:
                        writer->writeLong(startIdxWriter + i, triple[0]);
                        writer->writeLong(startIdxWriter + i, triple[1]);
                        writer->writeLong(startIdxWriter + i, triple[2]);
                        break;
                    case IDX_OPS:
                        writer->writeLong(startIdxWriter + i, triple[2]);
                        writer->writeLong(startIdxWriter + i, triple[1]);
                        writer->writeLong(startIdxWriter + i, triple[0]);
                        break;
                    case IDX_SOP:
                        writer->writeLong(startIdxWriter + i, triple[0]);
                        writer->writeLong(startIdxWriter + i, triple[2]);
                        writer->writeLong(startIdxWriter + i, triple[1]);
                        break;
                    case IDX_OSP:
                        writer->writeLong(startIdxWriter + i, triple[2]);
                        writer->writeLong(startIdxWriter + i, triple[0]);
                        writer->writeLong(startIdxWriter + i, triple[1]);
                        break;
                    case IDX_PSO:
                        writer->writeLong(startIdxWriter + i, triple[1]);
                        writer->writeLong(startIdxWriter + i, triple[0]);
                        writer->writeLong(startIdxWriter + i, triple[2]);
                        break;
                    case IDX_POS:
                        writer->writeLong(startIdxWriter + i, triple[1]);
                        writer->writeLong(startIdxWriter + i, triple[2]);
                        writer->writeLong(startIdxWriter + i, triple[0]);
                        break;
                }
            }
            compressedTriples++;
        } else {
            throw 10; //should never happen
        }
        currentTripleId += increment;
    }

    for (int i = 0; i < nperms; ++i) {
        writer->setTerminated(startIdxWriter + i);
    }

    if (uncommonTermsReader != NULL) {
        if (!(uncommonTermsReader->isEOF(idReader))) {
            LOG(ERRORL) << "There are still elements to read in the uncommon file";
            throw 10;
        }
    }
    delete[] tTriple;

    LOG(DEBUGL) << "Compressed triples " << compressedTriples << " compressed terms " << compressedTerms << " uncompressed terms " << uncompressedTerms;
}

bool Compressor::isSplittable(string path) {
    if (Utils::ends_with(path, ".gz")
            || Utils::ends_with(path, ".bz2")) {
        return false;
    } else {
        return true;
    }
}

vector<FileInfo> *Compressor::splitInputInChunks(const string &input, int nchunks, string prefix) {
    /*** Get list all files ***/
    vector<FileInfo> infoAllFiles;
    int64_t totalSize = 0;
    if (Utils::isDirectory(input)) {
        std::vector<string> children = Utils::getFilesWithPrefix(input, prefix);
        for(auto f : children) {
            int64_t fileSize = Utils::fileSize(f);
            totalSize += fileSize;
            FileInfo i;
            i.size = fileSize;
            i.start = 0;
            i.path = f;
            i.splittable = isSplittable(f);
            infoAllFiles.push_back(i);
        }
    } else {
        int64_t fileSize = Utils::fileSize(input);
        totalSize += fileSize;
        FileInfo i;
        i.size = fileSize;
        i.start = 0;
        i.path = input;
        i.splittable = isSplittable(input);
        infoAllFiles.push_back(i);
    }

    LOG(INFOL) << "Going to parse " << (uint64_t)infoAllFiles.size() << " files. Total size in bytes: " << totalSize << " bytes";

    /*** Sort the input files by size, and split the files through the multiple processors ***/
    std::sort(infoAllFiles.begin(), infoAllFiles.end(), cmpInfoFiles);
    vector<FileInfo> *files = new vector<FileInfo>[nchunks];
    uint64_t *splitSizes = new uint64_t[nchunks];
    uint64_t splitTargetSize = totalSize + nchunks - 1 / nchunks;
    for (int i = 0; i < nchunks; i++) {
	splitSizes[i] = 0;
    }
    int processedFiles = 0;
    int currentSplit = 0;
    while (processedFiles < infoAllFiles.size()) {
        FileInfo f = infoAllFiles[processedFiles++];
        if (!f.splittable) {
	    while (splitSizes[currentSplit] >= splitTargetSize) {
		currentSplit = (currentSplit + 1) % nchunks;
	    }
            files[currentSplit].push_back(f);
	    splitSizes[currentSplit] += f.size;
	    currentSplit = (currentSplit + 1) % nchunks;
        } else {
            uint64_t assignedFileSize = 0;
            while (assignedFileSize < f.size) {
                uint64_t sizeToCopy;
		while (splitSizes[currentSplit] >= splitTargetSize) {
		    currentSplit = (currentSplit + 1) % nchunks;
		}
		sizeToCopy = min(f.size - assignedFileSize,
                            splitTargetSize - splitSizes[currentSplit]);

                //Copy inside the split
                FileInfo splitF;
                splitF.path = f.path;
                splitF.start = assignedFileSize;
                splitF.splittable = true;
                splitF.size = sizeToCopy;
                files[currentSplit].push_back(splitF);
                splitSizes[currentSplit] += sizeToCopy;
                assignedFileSize += sizeToCopy;
		currentSplit = (currentSplit + 1) % nchunks;
            }
        }
    }

    delete[] splitSizes;
    infoAllFiles.clear();

    for (int i = 0; i < nchunks; ++i) {
        int64_t totalSize = 0;
        for (vector<FileInfo>::iterator itr = files[i].begin();
                itr < files[i].end(); ++itr) {
            totalSize += itr->size;
        }
        LOG(DEBUGL) << "Files in split " << i << ": " << (uint64_t)files[i].size() << " size " << totalSize;
    }
    return files;
}

void Compressor::parse(int dictPartitions, int sampleMethod, int sampleArg,
        int sampleArg2, int parallelProcesses, int maxReadingThreads,
        bool copyHashes, SchemaExtractor *schemaExtrator,
        bool onlySample,
        bool ignorePredicates) {

    tmpFileNames = new string[parallelProcesses];
    vector<FileInfo> *files = splitInputInChunks(input, parallelProcesses);
    int j = 0;
    for(int i = maxReadingThreads; i < parallelProcesses; ++i) {
        for(auto el : files[i])
            files[j].push_back(el);
        files[i].clear();
        j = (j + 1) % maxReadingThreads;
    }

    /*** Set name dictionary files ***/
    dictFileNames = new string*[maxReadingThreads];
    uncommonDictFileNames = new string*[maxReadingThreads];
    for (int i = 0; i < maxReadingThreads ; ++i) {
        string *df = new string[dictPartitions];
        string *df2 = new string[dictPartitions];
        for (int j = 0; j < dictPartitions; ++j) {
            df[j] = kbPath + string(DIR_SEP + "dict-file-") + to_string(i) + string("-")
                + to_string(j);
            df2[j] = kbPath + string(DIR_SEP + "udict-file-") + to_string(i)
                + string("-") + to_string(j);
        }
        dictFileNames[i] = df;
        uncommonDictFileNames[i] = df2;
    }

    SchemaExtractor *extractors = new SchemaExtractor[parallelProcesses];

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    GStringToNumberMap *commonTermsMaps =
        new GStringToNumberMap[parallelProcesses];
    if (sampleMethod == PARSE_COUNTMIN) {
        do_countmin(dictPartitions, sampleArg, parallelProcesses,
                maxReadingThreads, copyHashes,
                extractors, files, commonTermsMaps, false, ignorePredicates);
    } else if (sampleMethod == PARSE_COUNTMIN_MGCS) {
        do_countmin(dictPartitions, sampleArg, parallelProcesses,
                maxReadingThreads, copyHashes,
                extractors, files, commonTermsMaps, true, ignorePredicates);
    } else if (sampleMethod == PARSE_MGCS) {
        LOG(ERRORL) << "No int64_ter supported";
        throw 10;
        //do_mcgs();
    } else { //PARSE_SAMPLE
        if (ignorePredicates) {
            LOG(ERRORL) << "The option ignorePredicates is not implemented in combination with sampling";
            throw 10;
        }
        do_sample(dictPartitions, sampleArg, sampleArg2, maxReadingThreads,
                copyHashes, parallelProcesses, extractors, files,
                commonTermsMaps);
    }

    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(DEBUGL) << "Time heavy hitters detection = " << sec.count() * 1000 << " ms";

    /*** Merge the schema extractors ***/
    if (copyHashes) {
        LOG(DEBUGL) << "Merge the extracted schema";
        for (int i = 0; i < parallelProcesses; ++i) {
            schemaExtrator->merge(extractors[i]);
        }
        if (!onlySample) {
            LOG(DEBUGL) << "Prepare the schema...";
            schemaExtrator->prepare();
            LOG(DEBUGL) << "... done";
        }
    }
    delete[] extractors;

    if (!onlySample) {
        /*** Extract the uncommon terms ***/
        LOG(DEBUGL) << "Extract the uncommon terms";
        DiskLZ4Reader **readers = new DiskLZ4Reader*[maxReadingThreads];
        DiskLZ4Writer **writers = new DiskLZ4Writer*[maxReadingThreads];
        for (int i = 0; i < maxReadingThreads; ++i) {
            readers[i] = new DiskLZ4Reader(tmpFileNames[i],
                    parallelProcesses / maxReadingThreads, 3);
            writers[i] = new DiskLZ4Writer(uncommonDictFileNames[i][0],
                    parallelProcesses / maxReadingThreads, 3);
        }

        std::thread *threads = new std::thread[parallelProcesses];
        for (int i = 1; i < parallelProcesses; ++i) {
            threads[i - 1] = std::thread(
                    std::bind(&Compressor::extractUncommonTerms, this,
                        dictPartitions, readers[i % maxReadingThreads],
                        i / maxReadingThreads,
                        copyHashes,
                        i, parallelProcesses,
                        writers[i % maxReadingThreads],
                        ignorePredicates));
        }
        extractUncommonTerms(dictPartitions, readers[0], 0, copyHashes, 0,
                parallelProcesses,
                writers[0],
                ignorePredicates);
        for (int i = 1; i < parallelProcesses; ++i) {
            threads[i - 1].join();
        }

        for (int i = 0; i < maxReadingThreads; ++i) {
            delete readers[i];
            delete writers[i];
        }
        delete[] readers;
        delete[] writers;

        LOG(DEBUGL) << "Finished the extraction of the uncommon terms";
        delete[] threads;
    }
    delete[] commonTermsMaps;
    delete[] files;
}

uint64_t Compressor::getThresholdForUncommon(
        const int parallelProcesses,
        const uint64_t sizeHashTable,
        const int sampleArg,
        int64_t *distinctValues,
        Hashtable **tables1,
        Hashtable **tables2,
        Hashtable **tables3) {
    nTerms = 0;
    for (int i = 0; i < parallelProcesses; ++i) {
        nTerms = max(nTerms, distinctValues[i]);
    }
    LOG(DEBUGL) << "Estimated number of terms per partition: " << nTerms;
    int64_t termsPerBlock = max((int64_t)1, (int64_t)(nTerms / sizeHashTable)); //Terms per block
    int64_t tu1 = max((int64_t) 1, tables1[0]->getThreshold(sizeHashTable - sampleArg));
    int64_t tu2 = max((int64_t) 1, tables2[0]->getThreshold(sizeHashTable - sampleArg));
    int64_t tu3 = max((int64_t) 1, tables3[0]->getThreshold(sizeHashTable - sampleArg));
    return max(4 * termsPerBlock, min(min(tu1, tu2), tu3));
}

void Compressor::do_countmin_secondpass(const int dictPartitions,
        const int sampleArg,
        const int maxReadingThreads,
        const int parallelProcesses,
        bool copyHashes,
        const uint64_t sizeHashTable,
        Hashtable **tables1,
        Hashtable **tables2,
        Hashtable **tables3,
        int64_t *distinctValues,
        GStringToNumberMap *commonTermsMaps,
        bool ignorePredicates) {

    /*** Calculate the threshold value to identify uncommon terms ***/
    totalCount = tables1[0]->getTotalCount() / 3;
    uint64_t thresholdForUncommon = getThresholdForUncommon(
            parallelProcesses, sizeHashTable,
            sampleArg, distinctValues,
            tables1, tables2, tables3);
    LOG(DEBUGL) << "Threshold to mark elements as uncommon: " <<
        thresholdForUncommon;

    /*** Extract the common URIs ***/
    Hashtable *tables[3];
    tables[0] = tables1[0];
    tables[1] = tables2[0];
    tables[2] = tables3[0];
    ParamsExtractCommonTermProcedure params;
    params.tables = tables;
    params.dictPartitions = dictPartitions;
    params.maxMapSize = sampleArg;
    params.parallelProcesses = parallelProcesses;
    params.thresholdForUncommon = thresholdForUncommon;
    params.copyHashes = copyHashes;
    params.ignorePredicates = ignorePredicates;
    std::thread *threads = new std::thread[parallelProcesses - 1];

    //Init the DiskReaders
    DiskLZ4Reader **readers = new DiskLZ4Reader*[maxReadingThreads];
    for (int i = 0; i < maxReadingThreads; ++i) {
        readers[i] = new DiskLZ4Reader(tmpFileNames[i],
                parallelProcesses / maxReadingThreads, 3);
    }

    LOG(DEBUGL) << "Extract the common terms";
    for (int i = 1; i < parallelProcesses; ++i) {
        params.reader = readers[i % maxReadingThreads];
        params.idReader = i / maxReadingThreads;
        params.map = &commonTermsMaps[i];
        params.idProcess = i;
        threads[i - 1] = std::thread(
                std::bind(&Compressor::extractCommonTerms, this, params));
    }
    params.reader = readers[0];
    params.idReader = 0;
    params.map = &commonTermsMaps[0];
    params.idProcess = 0;
    extractCommonTerms(params);
    for (int i = 1; i < parallelProcesses; ++i) {
        threads[i - 1].join();
    }
    for (int i = 0; i < maxReadingThreads; ++i)
        delete readers[i];
    delete[] readers;
    delete[] threads;
}

void Compressor::do_countmin(const int dictPartitions, const int sampleArg,
        const int parallelProcesses, const int maxReadingThreads,
        const bool copyHashes, SchemaExtractor *extractors,
        vector<FileInfo> *files,
        GStringToNumberMap *commonTermsMaps,
        bool usemisgra,
        bool ignorePredicates) {

    /*** Uncompress the triples in parallel ***/
    Hashtable **tables1 = new Hashtable*[parallelProcesses];
    Hashtable **tables2 = new Hashtable*[parallelProcesses];
    Hashtable **tables3 = new Hashtable*[parallelProcesses];
    int64_t *distinctValues = new int64_t[parallelProcesses];
    memset(distinctValues, 0, sizeof(int64_t)*parallelProcesses);

    /*** If we intend to use Misra to store the popular terms, then we must init
     * it ***/
    vector<string> *resultsMGS = NULL;
    if (usemisgra) {
        resultsMGS = new vector<string>[parallelProcesses];
    }

    /*** Calculate size of the hash table ***/
    int64_t nBytesInput = Utils::getNBytes(input);
    bool isInputCompressed = Utils::isCompressed(input);
    int64_t maxSize;
    if (!isInputCompressed) {
        maxSize = nBytesInput / 1000;
    } else {
        maxSize = nBytesInput / 100;
    }
    // Fixed: maxSize for very small inputs
    maxSize = std::max((int64_t)sampleArg, maxSize);
    LOG(DEBUGL) << "Size Input: " << nBytesInput <<
        " bytes. Max table size=" << maxSize;
    int64_t memForHashTables = (int64_t)(Utils::getSystemMemory() * 0.7)
        / (3 * parallelProcesses);
    //Divided numer hash tables
    const uint64_t sizeHashTable = std::min((int64_t)maxSize,
            std::max((int64_t)1000000,
                (int64_t)(memForHashTables / sizeof(int64_t))));

    LOG(DEBUGL) << "Size hash table " << sizeHashTable;

    if (parallelProcesses % maxReadingThreads != 0) {
        LOG(ERRORL) << "The maximum number of threads must be a multiplier of the reading threads";
        throw 10;
    }

    //Set up the output file names
    for (int i = 0; i < maxReadingThreads; ++i) {
        tmpFileNames[i] = kbPath + string(DIR_SEP + "tmp-") + to_string(i);
    }

    DiskReader **readers = new DiskReader*[maxReadingThreads];
    std::thread *threadReaders = new std::thread[maxReadingThreads];
    DiskLZ4Writer **writers = new DiskLZ4Writer*[maxReadingThreads];
    for (int i = 0; i < maxReadingThreads; ++i) {
        readers[i] = new DiskReader(max(2,
                    (int)(parallelProcesses / maxReadingThreads) * 2), &files[i]);
        threadReaders[i] = std::thread(std::bind(&DiskReader::run, readers[i]));
        writers[i] = new DiskLZ4Writer(tmpFileNames[i], parallelProcesses / maxReadingThreads, 3);
    }

    std::thread *threads = new std::thread[parallelProcesses - 1];

    ParamsUncompressTriples params;
    //Set only global params
    params.sizeHeap = sampleArg;
    params.ignorePredicates = ignorePredicates;

    for (int i = 1; i < parallelProcesses; ++i) {
        tables1[i] = new Hashtable(sizeHashTable,
                &Hashes::dbj2s_56);
        tables2[i] = new Hashtable(sizeHashTable,
                &Hashes::fnv1a_56);
        tables3[i] = new Hashtable(sizeHashTable,
                &Hashes::murmur3_56);

        params.reader = readers[i % maxReadingThreads];
        params.table1 = tables1[i];
        params.table2 = tables2[i];
        params.table3 = tables3[i];
        params.writer = writers[i % maxReadingThreads];
        params.idwriter = i / maxReadingThreads;
        params.extractor = copyHashes ? extractors + i : NULL;
        params.distinctValues = distinctValues + i;
        params.resultsMGS = usemisgra ? &resultsMGS[i] : NULL;
        threads[i - 1] = std::thread(
                std::bind(&Compressor::uncompressTriples, this,
                    params));
    }
    tables1[0] = new Hashtable(sizeHashTable,
            &Hashes::dbj2s_56);
    tables2[0] = new Hashtable(sizeHashTable,
            &Hashes::fnv1a_56);
    tables3[0] = new Hashtable(sizeHashTable,
            &Hashes::murmur3_56);
    params.reader = readers[0];
    params.table1 = tables1[0];
    params.table2 = tables2[0];
    params.table3 = tables3[0];
    params.writer = writers[0];
    params.idwriter = 0;
    params.extractor = copyHashes ? extractors : NULL;
    params.distinctValues = distinctValues;
    params.resultsMGS = usemisgra ? &resultsMGS[0] : NULL;
    uncompressTriples(params);

    for (int i = 1; i < parallelProcesses; ++i) {
        if (threads[i - 1].joinable())
            threads[i - 1].join();
    }
    for (int i = 0; i < maxReadingThreads; ++i) {
        if (threadReaders[i].joinable())
            threadReaders[i].join();
    }
    for (int i = 0; i < maxReadingThreads; ++i) {
        delete readers[i];
        delete writers[i];
    }
    delete[] readers;
    delete[] writers;
    delete[] threadReaders;

    //Merging the tables
    LOG(DEBUGL) << "Merging the tables...";

    for (int i = 0; i < parallelProcesses; ++i) {
        if (i != 0) {
            LOG(DEBUGL) << "Merge table " << (i);
            tables1[0]->merge(tables1[i]);
            tables2[0]->merge(tables2[i]);
            tables3[0]->merge(tables3[i]);
            delete tables1[i];
            delete tables2[i];
            delete tables3[i];
        }
    }

    /*** If misra-gries is not active, then we must perform another pass to
     * extract the strings of the common terms. Otherwise, MGS gives it to
     * us ***/
    if (!usemisgra) {
        do_countmin_secondpass(dictPartitions, sampleArg, maxReadingThreads,
                parallelProcesses,
                copyHashes, sizeHashTable, tables1, tables2,
                tables3, distinctValues, commonTermsMaps, ignorePredicates);
    } else {
        /*** Determine a minimum threshold value from the count_min tables to
         * mark the element has common ***/
        uint64_t minFreq = getThresholdForUncommon(
                parallelProcesses, sizeHashTable,
                sampleArg, distinctValues,
                tables1, tables2, tables3);
        LOG(DEBUGL) << "The minimum frequency required is " << minFreq;

        /*** Then go through all elements and take the frequency from the
         * count_min tables ***/
        std::vector<std::pair<string, int64_t>> listFrequentTerms;
        for (int i = 0; i < parallelProcesses; ++i) {
            for (vector<string>::const_iterator itr = resultsMGS[i].begin();
                    itr != resultsMGS[i].end(); ++itr) {
                //Get the frequency and add it
                uint64_t v1 = tables1[0]->get(*itr);
                uint64_t v2 = tables2[0]->get(*itr);
                uint64_t v3 = tables3[0]->get(*itr);
                uint64_t freq = min(v1, min(v2, v3));
                if (freq >= minFreq)
                    listFrequentTerms.push_back(make_pair(*itr, freq));
            }
        }

        /*** Sort the list and keep the highest n ***/
        LOG(DEBUGL) << "There are " << (uint64_t)listFrequentTerms.size() << " potential frequent terms";
        std::sort(listFrequentTerms.begin(), listFrequentTerms.end(),
                lessTermFrequenciesDesc);

        /*** Copy the first n terms in the map ***/
        char supportBuffer[MAX_TERM_SIZE];
        finalMap->set_empty_key(EMPTY_KEY);
        finalMap->set_deleted_key(DELETED_KEY);
        for (int i = 0; finalMap->size() < sampleArg && i < listFrequentTerms.size(); ++i) {
            std::pair<string, int64_t> pair = listFrequentTerms[i];
            if (i == 0 || pair.first != listFrequentTerms[i - 1].first) {
                memcpy(supportBuffer + 2, pair.first.c_str(), pair.first.size());
                Utils::encode_short(supportBuffer, 0, static_cast<int>(pair.first.size()));
                const char *newkey = poolForMap->addNew(supportBuffer,
                        static_cast<int>(pair.first.size() + 2));
                finalMap->insert(std::make_pair(newkey, pair.second));
            }
        }

        /*** delete msgs data structures ***/
        delete[] resultsMGS;
    }

    /*** Delete the hashtables ***/
    LOG(DEBUGL) << "Delete some datastructures";
    table1 = std::shared_ptr<Hashtable>(tables1[0]);
    table2 = std::shared_ptr<Hashtable>(tables2[0]);
    table3 = std::shared_ptr<Hashtable>(tables3[0]);
    delete[] distinctValues;
    delete[] tables1;
    delete[] tables2;
    delete[] tables3;

    /*** Merge the hashmaps with the common terms ***/
    if (!usemisgra) {
        finalMap->set_empty_key(EMPTY_KEY);
        finalMap->set_deleted_key(DELETED_KEY);
        LOG(DEBUGL) << "Merge the local common term maps";
        mergeCommonTermsMaps(finalMap, commonTermsMaps, parallelProcesses);
    }
    LOG(DEBUGL) << "Size hashtable with common terms " << (uint64_t)finalMap->size();

    delete[] threads;
}

void Compressor::do_sample(const int dictPartitions, const int sampleArg,
        const int sampleArg2,
        const int maxReadingThreads,
        bool copyHashes,
        const int parallelProcesses,
        SchemaExtractor *extractors, vector<FileInfo> *files,
        GStringToNumberMap *commonTermsMaps) {
    //Perform only a sample
    int chunksToProcess = 0;
    std::thread *threads = new std::thread[maxReadingThreads - 1];
    while (chunksToProcess < parallelProcesses) {
        for (int i = 1;
                i < maxReadingThreads
                && (chunksToProcess + i) < parallelProcesses;
                ++i) {
            tmpFileNames[chunksToProcess + i] = kbPath + DIR_SEP + string("tmp-")
                + to_string(chunksToProcess + i);
            threads[i - 1] = std::thread(
                    std::bind(&Compressor::uncompressAndSampleTriples,
                        this,
                        files[chunksToProcess + i],
                        tmpFileNames[chunksToProcess + i],
                        dictFileNames[chunksToProcess + i],
                        dictPartitions,
                        sampleArg2,
                        &commonTermsMaps[chunksToProcess + i],
                        copyHashes ? extractors + i : NULL));
        }
        tmpFileNames[chunksToProcess] = kbPath + DIR_SEP + string("tmp-" + to_string(chunksToProcess));
        uncompressAndSampleTriples(files[chunksToProcess],
                tmpFileNames[chunksToProcess],
                dictFileNames[chunksToProcess],
                dictPartitions,
                sampleArg2,
                &commonTermsMaps[chunksToProcess],
                copyHashes ? extractors : NULL);
        for (int i = 1; i < maxReadingThreads; ++i) {
            threads[i - 1].join();
        }
        chunksToProcess += maxReadingThreads;
    }
    //This is just a rough estimate
    totalCount = commonTermsMaps[0].size() * 100 / sampleArg2 / 3 * 4
        * dictPartitions;
    delete[] threads;

    /*** Collect all terms in the sample in one vector ***/
    std::vector<std::pair<string, size_t>> sampledTerms;
    for (int i = 0; i < parallelProcesses; ++i) {
        //Get all elements in the map
        for (GStringToNumberMap::iterator itr = commonTermsMaps[i].begin();
                itr != commonTermsMaps[i].end(); ++itr) {
            sampledTerms.push_back(make_pair(itr->first, itr->second));
        }
        commonTermsMaps[i].clear();
    }

    /*** Sort all the pairs by term ***/
    LOG(DEBUGL) << "Sorting the sample of " << (uint64_t)sampledTerms.size();
    std::sort(sampledTerms.begin(), sampledTerms.end(), &sampledTermsSorter1);

    /*** Merge sample ***/
    LOG(DEBUGL) << "Merging sorted results";
    std::vector<std::pair<string, size_t>> sampledTermsUniq;
    size_t i = 0;
    for (std::vector<std::pair<string, size_t>>::iterator
            itr = sampledTerms.begin(); itr != sampledTerms.end();  ++itr) {
        if (i == 0 || itr->first != sampledTermsUniq.back().first) {
            // Add a new pair
            sampledTermsUniq.push_back(make_pair(itr->first, itr->second));
        } else if (i != 0) {
            //Increment the previous
            sampledTermsUniq.back().second += itr->second;
        }
        i++;
    }

    /*** Sort by descending order ***/
    std::sort(sampledTermsUniq.begin(), sampledTermsUniq.end(),
            &sampledTermsSorter2);

    /*** Pick the top n and copy them in finalMap ***/
    LOG(DEBUGL) << "Copy in the hashmap the top k. The sorted sample contains " << (uint64_t)sampledTermsUniq.size();
    finalMap->set_empty_key(EMPTY_KEY);
    finalMap->set_deleted_key(DELETED_KEY);
    char supportTerm[MAX_TERM_SIZE];
    for (int i = 0; i < sampleArg && i < sampledTermsUniq.size(); ++i) {
        Utils::encode_short(supportTerm, static_cast<int>(sampledTermsUniq[i].first.size()));
        memcpy(supportTerm + 2, sampledTermsUniq[i].first.c_str(),
                sampledTermsUniq[i].first.size());
        const char *newkey = poolForMap->addNew(supportTerm,
                static_cast<int>(sampledTermsUniq[i].first.size() + 2));
        finalMap->insert(make_pair(newkey, sampledTermsUniq[i].second));
    }

    LOG(DEBUGL) << "Size hashtable with common terms " << (uint64_t)finalMap->size();
}

bool Compressor::areFilesToCompress(int parallelProcesses, string *tmpFileNames) {
    for (int i = 0; i < parallelProcesses; ++i) {
        if (Utils::exists(tmpFileNames[i]) && Utils::fileSize(tmpFileNames[i]) > 0) {
            return true;
        }
    }
    return false;
}

void Compressor::sortAndDumpToFile2(vector<TriplePair> &pairs,
        string outputFile) {
    std::sort(pairs.begin(), pairs.end(), TriplePair::sLess);
    LZ4Writer outputSegment(outputFile);

    for (vector<TriplePair>::iterator itr = pairs.begin(); itr != pairs.end();
            ++itr) {
        itr->writeTo(&outputSegment);
    }
}

void Compressor::sortAndDumpToFile(vector<SimplifiedAnnotatedTerm> &terms,
        string outputFile,
	int nthreads,
        bool removeDuplicates) {
    if (removeDuplicates) {
        throw 10; //I removed the code below to check for duplicates
    }
    //BOOST_LOG_TRIVIAL(DEBUG) << "Sorting and writing to file " << outputFile << " " << terms.size() << " elements. Removedupl=" << removeDuplicates;
    // std::sort(terms.begin(), terms.end(), SimplifiedAnnotatedTerm::sless);
    Sorter::sort_int(terms.begin(), terms.end(), SimplifiedAnnotatedTerm::sless, nthreads);
    //BOOST_LOG_TRIVIAL(DEBUG) << "Finished sorting";
    LZ4Writer *outputSegment = new LZ4Writer(outputFile);
    //const char *prevTerm = NULL;
    //int sizePrevTerm = 0;
    int64_t countOutput = 0;
    for (vector<SimplifiedAnnotatedTerm>::iterator itr = terms.begin();
            itr != terms.end();
            ++itr) {
        //if (!removeDuplicates || prevTerm == NULL
        //        || !itr->equals(prevTerm, sizePrevTerm)) {
        itr->writeTo(outputSegment);
        //prevTerm = itr->term;
        //sizePrevTerm = itr->size;
        countOutput++;
        //}
    }
    delete outputSegment;
    //BOOST_LOG_TRIVIAL(DEBUG) << "Written sorted elements: " << countOutput;
}

void Compressor::sortAndDumpToFile(vector<SimplifiedAnnotatedTerm> &terms,
        DiskLZ4Writer *writer,
        const int id) {
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    std::sort(terms.begin(), terms.end(), SimplifiedAnnotatedTerm::sless);
    std::chrono::duration<double> dur = std::chrono::system_clock::now() - start;
    //BOOST_LOG_TRIVIAL(DEBUG) << "Time sorting " << terms.size() << " elements was " << dur.count() << "sec.";
    start = std::chrono::system_clock::now();
    writer->writeLong(id, terms.size());
    for (vector<SimplifiedAnnotatedTerm>::iterator itr = terms.begin();
            itr != terms.end();
            ++itr) {
        itr->writeTo(id, writer);
    }
    dur = std::chrono::system_clock::now() - start;
    //BOOST_LOG_TRIVIAL(DEBUG) << "Time dumping " << terms.size() << " terms on the writing buffer " << dur.count() << "sec.";

}

void Compressor::immemorysort(string **inputFiles,
        int maxReadingThreads,
        int parallelProcesses,
        string outputFile, //int *noutputFiles,
        bool removeDuplicates,
        const int64_t maxSizeToSort,
        int sampleInterval,
        bool sample) {
    //std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    //Split maxSizeToSort in n threads
    const int64_t maxMemPerThread = maxSizeToSort / parallelProcesses;

    DiskLZ4Reader **readers = new DiskLZ4Reader*[maxReadingThreads];
    memset(readers, 0, sizeof(DiskLZ4Reader*)*maxReadingThreads);
    bool empty = true;
    for (int i = 0; i < maxReadingThreads; ++i) {
        if (Utils::exists(inputFiles[i][0])) {
            readers[i] = new DiskLZ4Reader(inputFiles[i][0],
                    parallelProcesses / maxReadingThreads,
                    3);
            empty = false;
        }
    }
    DiskLZ4Writer **writers = new DiskLZ4Writer*[maxReadingThreads];
    for (int i = 0; i < maxReadingThreads; ++i) {
        writers[i] = new DiskLZ4Writer(outputFile + string(".") + to_string(i),
                parallelProcesses / maxReadingThreads,
                3);
    }

    std::vector<std::vector<string>> chunks;
    chunks.resize(maxReadingThreads);
    MultiDiskLZ4Writer **sampleWriters = new MultiDiskLZ4Writer*[maxReadingThreads];
    for (int i = 0; i < parallelProcesses; ++i) {
        chunks[i % maxReadingThreads].push_back(
                outputFile + "-" + to_string(i) + "-sample");
    }
    for (int i = 0; i < maxReadingThreads; ++i) {
        sampleWriters[i] = new MultiDiskLZ4Writer(chunks[i], 3, 3);
    }

    if (!empty) {
        std::thread *threads = new std::thread[parallelProcesses - 1];
        for (int i = 1; i < parallelProcesses; ++i) {
            DiskLZ4Reader *reader = readers[i % maxReadingThreads];
            DiskLZ4Writer *writer = writers[i % maxReadingThreads];
            MultiDiskLZ4Writer *sampleWriter = sampleWriters[i % maxReadingThreads];
            if (reader) {
                threads[i - 1] = std::thread(
                        std::bind(
                            &Compressor::inmemorysort_seq,
                            reader,
                            writer,
                            sampleWriter,
                            i / maxReadingThreads,
                            i,
                            maxMemPerThread,
                            removeDuplicates,
                            sampleInterval,
                            sample));
            }
        }
        DiskLZ4Reader *reader = readers[0];
        DiskLZ4Writer *writer = writers[0];
        MultiDiskLZ4Writer *sampleWriter = sampleWriters[0];
        if (reader) {
            inmemorysort_seq(reader, writer,
                    sampleWriter, 0, 0,
                    maxMemPerThread,
                    removeDuplicates,
                    sampleInterval,
                    sample);
        }
        for (int i = 1; i < parallelProcesses; ++i) {
            threads[i - 1].join();
        }
        delete[] threads;
    }
    for (int i = 0; i < maxReadingThreads; ++i) {
        if (readers[i])
            delete readers[i];
        delete writers[i];
        delete sampleWriters[i];
    }
    delete[] readers;
    delete[] writers;
    delete[] sampleWriters;
}

void Compressor::inmemorysort_seq(DiskLZ4Reader *reader,
        DiskLZ4Writer *writer,
        MultiDiskLZ4Writer *sampleWriter,
        const int idReader,
        int idx,
        const uint64_t maxMemPerThread,
        bool removeDuplicates,
        int sampleInterval,
        bool sample) {

    vector<SimplifiedAnnotatedTerm> terms;
    //vector<string> outputfiles;
    StringCollection supportCollection(BLOCK_SUPPORT_BUFFER_COMPR);
    int64_t bytesAllocated = 0;

    //BOOST_LOG_TRIVIAL(DEBUG) << "Start immemory_seq method. MaxMemPerThread=" << maxMemPerThread;

    //int64_t count = 0;
    int sampleCount = 0;
    int sampleAdded = 0;
    while (!reader->isEOF(idReader)) {
        SimplifiedAnnotatedTerm t;
        t.readFrom(idReader, reader);
        if (sample) {
            if (sampleCount % sampleInterval == 0) {
                sampleWriter->writeString(idReader, t.term, t.size);
                sampleAdded++;
                sampleCount = 0;
            }
            sampleCount++;
        }

        if ((bytesAllocated + sizeof(AnnotatedTerm) * terms.size())
                >= maxMemPerThread) {
            sortAndDumpToFile(terms, writer, idReader);
            terms.clear();
            supportCollection.clear();
            bytesAllocated = 0;
        }

        t.term = supportCollection.addNew((char *) t.term, t.size);
        terms.push_back(t);
        bytesAllocated += t.size;
    }

    if (terms.size() > 0) {
        sortAndDumpToFile(terms, writer, idReader);
    }
    writer->setTerminated(idReader);
    sampleWriter->setTerminated(idReader);
}

/*void Compressor::sampleTuples(string input, std::vector<string> *output) {
  LZ4Reader reader(input);
  while (!reader.isEof()) {
  AnnotatedTerm t;
  t.readFrom(&reader);
  const int r = rand() % 100; //from 0 to 99
  if (r < 1) { //Sample 1%
  output->push_back(string(t.term + 2, t.size - 2));
  }
  }
  }*/

bool _sampleLess(const std::pair<const char *, int> &c1,
        const std::pair<const char *, int> &c2) {
    int ret = memcmp(c1.first, c2.first, min(c1.second, c2.second));
    if (ret == 0) {
        return (c1.second - c2.second) < 0;
    } else {
        return ret < 0;
    }
}



std::vector<string> Compressor::getPartitionBoundaries(const string kbdir,
        const int partitions) {
    //Read all sample strings
    std::vector<std::pair<const char *, int>> sample;
    StringCollection col(10 * 1024 * 1024);
    std::vector<string> children = Utils::getFilesWithSuffix(kbdir, "-sample");
    for (auto child : children) {
        //Read the content
        {
            LZ4Reader r(child);
            while (!r.isEof()) {
                int size;
                const char *s = r.parseString(size);
                const char *news = col.addNew(s, size);
                sample.push_back(std::make_pair(news, size));
            }
        }
        Utils::remove(child);
    }

    /*if (sample.empty()) {
      BOOST_LOG_TRIVIAL(ERROR) << "Sample should not be empty!";
      throw 10;
      }*/

    //Sort the sample
    std::sort(sample.begin(), sample.end(), &_sampleLess);

    std::vector<string> output;
    size_t sizePartition = sample.size() / partitions;
    if (sizePartition == 0) {
        sizePartition = 1;
    }
    LOG(DEBUGL) << "sample.size()=" << (uint64_t)sample.size()
        << " sizePartition=" << (uint64_t)sizePartition;
    for (size_t i = sizePartition; i < sample.size(); i += sizePartition) {
        if (output.size() < partitions - 1) {
            //Add element in the partition
            string s = string(sample[i-1].first, sample[i-1].second);
            output.push_back(s);
        }
    }
    return output;
}

void Compressor::sortRangePartitionedTuples(DiskLZ4Reader *reader,
        int idReader,
	const std::string dirPrefix,
        const std::string outputFile,
        const std::vector<std::string> *boundaries) {
    int idx = 0; //partition within the same file
    int idxFile = 0; //multiple sorted files in the stream
    LZ4Writer *output = NULL;

    //Read the input file, and range-partition its content
    string bound;
    bool isLast;
    if (boundaries->size() > 0) {
        bound = boundaries->at(idx);
        isLast = false;
    } else {
        isLast = true;
    }
    int64_t counter = 0;
    int64_t countFile = 0;
    while (!reader->isEOF(idReader)) {
        if (countFile == 0) {
            countFile = reader->readLong(idReader);
            assert(countFile > 0);
            idx = 0;
            //Create a new file
            delete output;
            output = new LZ4Writer(dirPrefix + std::to_string(idx) + DIR_SEP + outputFile + "-" +
                    to_string(idxFile));
            if (boundaries->size() > 0) {
                bound = boundaries->at(idx);
                isLast = false;
            } else {
                isLast = true;
            }
            idxFile++;
        }

        SimplifiedAnnotatedTerm t;
        t.readFrom(idReader, reader);
        assert(t.tripleIdAndPosition != -1);
        string term = string(t.term, t.size);
        if (!isLast && term > bound) {
            do {
                idx++;
                if (idx < boundaries->size()) {
                    bound = boundaries->at(idx);
                } else {
                    isLast = true;
                }
            } while (!isLast && term > bound);
	    delete output;
	    output = new LZ4Writer(dirPrefix + std::to_string(idx) + DIR_SEP + outputFile + "-" +
		    to_string(idxFile));
        }
        t.writeTo(output);
        counter++;
        countFile--;
    }
    LOG(DEBUGL) << "Partitioned " << counter << " terms.";
    delete output;
}

void Compressor::rangePartitionFiles(int readThreads, int maxThreads,
	const std::string dirPrefix,
        const std::string prefixInputFiles,
        const std::vector<string> &boundaries) {
    DiskLZ4Reader **readers = new DiskLZ4Reader*[readThreads];
    std::vector<string> infiles;
    for (int i = 0; i < readThreads; ++i) {
        string infile = prefixInputFiles + string(".")
            + to_string(i);
        readers[i] = new DiskLZ4Reader(infile,
                maxThreads / readThreads,
                3);
        infiles.push_back(infile);
    }

    std::vector<std::thread> threads(maxThreads);
    for (int i = 1; i < maxThreads; ++i) {
        DiskLZ4Reader *reader = readers[i % readThreads];
        string outputFile = std::string("r-") + std::to_string(i);
        threads[i] =
            std::thread(std::bind(&Compressor::sortRangePartitionedTuples,
                        reader,
                        i / readThreads,
			dirPrefix,
                        outputFile,
                        &boundaries));
    }
    sortRangePartitionedTuples(readers[0],
            0,
	    dirPrefix,
            "r-0",
            &boundaries);
    for (int i = 1; i < maxThreads; ++i) {
        threads[i].join();
    }
    for (int i = 0; i < readThreads; ++i) {
        delete readers[i];
    }
    for (int i = 0; i < infiles.size(); ++i) {
        Utils::remove(infiles[i]);
        Utils::remove(infiles[i] + ".idx");
    }
    delete[] readers;
}

void Compressor::sortPartition(ParamsSortPartition params) {
    std::string dirPrefix = params.dirPrefix;
    MultiDiskLZ4Reader *reader = params.reader;
    MultiMergeDiskLZ4Reader *mergerReader = params.mergerReader;
    DiskLZ4Writer *dictWriter = params.dictWriter;
    const int idDictWriter = params.idDictWriter;
    DiskLZ4Writer *writer = params.writer;
    int idReader = params.idReader;
    int idWriter = params.idWriter;
    string prefixIntFiles = params.prefixIntFiles;
    int part = params.part;
    uint64_t *counter = params.counter;
    uint64_t maxMem = params.maxMem;
    int nthreads = params.threadsPerPartition;


    std::string parentDir = dirPrefix + std::to_string(part);
    std::vector<string> children = Utils::getFiles(parentDir);
    std::vector<string> filesToSort;
    for(auto child : children) {
	if (Utils::fileSize(child) > 0) {
	    filesToSort.push_back(child);
	} else {
	    Utils::remove(child);
	}
    }
    reader->addInput(idReader, filesToSort);

    string outputFile = prefixIntFiles + "tmp";

    //Keep all the prefixes stored in a map to increase the size of URIs we can
    //keep in main memory
    StringCollection colprefixes(4 * 1024 * 1024);
    ByteArraySet prefixset;
    prefixset.set_empty_key(EMPTY_KEY);

    StringCollection col(128 * 1024 * 1024);
    std::vector<SimplifiedAnnotatedTerm> tuples;
    std::vector<string> sortedFiles;
    int64_t bytesAllocated = 0;
    int idx = 0;
    std::unique_ptr<char[]> tmpprefix = std::unique_ptr<char[]>(new char[MAX_TERM_SIZE]);

    //Load all the files until I fill main memory.
    //for (int i = 0; i < filesToSort.size(); ++i) {
    //    string file = filesToSort[i];
    //    std::unique_ptr<LZ4Reader> r = std::unique_ptr<LZ4Reader>(new LZ4Reader(file));
    while (!reader->isEOF(idReader)) {
        SimplifiedAnnotatedTerm t;
        t.readFrom(idReader, reader);
        assert(t.prefix == NULL);
        if ((bytesAllocated +
                    (sizeof(SimplifiedAnnotatedTerm) * tuples.size()))
                >= maxMem) {
            LOG(DEBUGL) << "Dumping file " << idx << " with "
                << (uint64_t)tuples.size() << " tuples ...";
            string ofile = outputFile + string(".") + to_string(idx);
            idx++;
            sortAndDumpToFile(tuples, ofile, nthreads, false);
            sortedFiles.push_back(ofile);
            tuples.clear();
            col.clear();
            bytesAllocated = 0;
        }

        //Check if I can compress the prefix of the string
        t.splitOffPrefix();
        if (t.prefix != NULL) {
            //Check if the prefix exists in the map
            Utils::encode_short(tmpprefix.get(), t.prefixSize);
            memcpy(tmpprefix.get() + 2, t.prefix, t.prefixSize);
            auto itr = prefixset.find((const char*)tmpprefix.get());
            if (itr == prefixset.end()) {
                const char *prefixtoadd = colprefixes.addNew(tmpprefix.get(),
                        t.prefixSize + 2);
                t.prefix = prefixtoadd;
                prefixset.insert(prefixtoadd);
                assert(Utils::decode_short(prefixtoadd) == t.prefixSize);
            } else {
                t.prefix = *itr;
                assert(Utils::decode_short(*itr) == t.prefixSize);
            }
        }
        t.term = col.addNew((char*) t.term, t.size);

        tuples.push_back(t);
        bytesAllocated += t.size;
    }

    for (auto file : filesToSort) {
        Utils::remove(file);
    }
    LOG(DEBUGL) << "Number of prefixes " << (uint64_t)prefixset.size();

    if (idx == 0) {
        //All data fit in main memory. Do not need to write it down
        LOG(DEBUGL) << "All terms (" << (uint64_t)tuples.size() << ") fit in main memory";
        // std::sort(tuples.begin(), tuples.end(), SimplifiedAnnotatedTerm::sless);
	Sorter::sort_int(tuples.begin(), tuples.end(), SimplifiedAnnotatedTerm::sless, nthreads);

        //The following code is replicated below.
        int64_t counterTerms = -1;
        int64_t counterPairs = 0;
        {
            //LZ4Writer writer(outputfile);
            //LZ4Writer dictWriter(dictfile);

            //Write the output
            int64_t prev = -1;

            for (size_t i = 0; i < tuples.size(); ++i) {
                SimplifiedAnnotatedTerm *t = &tuples[i];
                if (prev == -1 || !t->equals(tuples[prev])) {
                    counterTerms++;
                    prev = i;

                    dictWriter->writeLong(idDictWriter, counterTerms);
                    if (t->prefix != NULL) {
                        int64_t len = t->prefixSize + PREFIX_HEADER_LEN + t->size;
                        dictWriter->writeVLong(idDictWriter, len);
                        dictWriter->writeRawArray(idDictWriter, PREFIX_HEADER, PREFIX_HEADER_LEN);
                        dictWriter->writeRawArray(idDictWriter, t->prefix + 2, t->prefixSize);
                        dictWriter->writeRawArray(idDictWriter, t->term, t->size);
                    } else {
                        dictWriter->writeString(idDictWriter, t->term, t->size);
                    }
                }
                //Write the output
                counterPairs++;
                assert(t->tripleIdAndPosition != -1);
                writer->writeLong(idWriter, counterTerms);
                writer->writeLong(idWriter, t->tripleIdAndPosition);
            }
        }

        *counter = counterTerms + 1;
        LOG(DEBUGL) << "Partition " << part << " contains " <<
            counterPairs << " tuples " <<
            (counterTerms + 1) << " terms";
        for (auto f : sortedFiles) {
            Utils::remove(f);
        }
    } else {
        if (tuples.size() > 0) {
            string ofile = outputFile + string(".") + to_string(idx);
            sortAndDumpToFile(tuples, ofile, nthreads, false);
            sortedFiles.push_back(ofile);
            tuples.clear();
            col.clear();
        }

        LOG(DEBUGL) << "Merge " << (uint64_t)sortedFiles.size()
            << " files in order to sort the partition";

#define NSORT	4
        while (sortedFiles.size() > NSORT) {
            //Add files to the batch
            std::vector<string> batchFiles;
	    for (int i = 0; i < NSORT; i++) {
		batchFiles.push_back(sortedFiles[i]);
		std::vector<string> cont;
		cont.push_back(sortedFiles[i]);
		mergerReader->addInput(idWriter * NSORT + i, cont);
	    }

            //Create output file
            string ofile = outputFile + string(".") + to_string(++idx);
            LZ4Writer writer(ofile);

            //Merge batch of files
            FileMerger2<SimplifiedAnnotatedTerm> merger(mergerReader,
                    idWriter * NSORT, NSORT);
            //FileMerger<SimplifiedAnnotatedTerm> merger(batchFiles);
            while (!merger.isEmpty()) {
                SimplifiedAnnotatedTerm t = merger.get();
                t.writeTo(&writer);
            }
	    for (int i = 0; i < NSORT; i++) {
		mergerReader->unsetPartition(idWriter * NSORT + i);
	    }

            //Remove them
            sortedFiles.push_back(ofile);
            for (auto f : batchFiles) {
                Utils::remove(f);
                sortedFiles.erase(sortedFiles.begin());
            }
        }
        LOG(DEBUGL) << "Final merge";

        //Create a file
        //std::unique_ptr<LZ4Writer> dictWriter(new LZ4Writer(dictfile));

        char *previousTerm = new char[MAX_TERM_SIZE];
        int previousTermSize = 0;
        int64_t counterTerms = -1;
        int64_t counterPairs = 0;
        //Sort the files
        for (int i = 0; i < sortedFiles.size(); ++i) {
            std::vector<string> cont1;
            cont1.push_back(sortedFiles[i]);
            mergerReader->addInput(idWriter * NSORT + i, cont1);
        }

        FileMerger2<SimplifiedAnnotatedTerm> merger(mergerReader,
                idWriter * NSORT, static_cast<int>(sortedFiles.size()));
        while (!merger.isEmpty()) {
            SimplifiedAnnotatedTerm t = merger.get();
            if (!t.equals(previousTerm, previousTermSize)) {
                counterTerms++;

                memcpy(previousTerm, t.term, t.size);
                previousTermSize = t.size;

                dictWriter->writeLong(idDictWriter, counterTerms);
                assert(t.prefix == NULL);
                dictWriter->writeString(idDictWriter, t.term, t.size);
            }

            //Write the output
            counterPairs++;
            assert(t.tripleIdAndPosition != -1);
            writer->writeLong(idWriter, counterTerms);
            writer->writeLong(idWriter, t.tripleIdAndPosition);
        }

        //remove the intermediate files
        int i = 0;
        for (auto f : sortedFiles) {
            mergerReader->unsetPartition(idWriter * NSORT + i);
            Utils::remove(f);
            i++;
        }

        delete[] previousTerm;
        *counter = counterTerms + 1;
        LOG(DEBUGL) << "Partition " << part << " contains "
            << counterPairs << " tuples "
            << (counterTerms + 1) << " terms";

    }
    writer->setTerminated(idWriter);
    dictWriter->setTerminated(idDictWriter);
}

void Compressor::concatenateFiles_seq(int part, std::vector<std::string> *rangeFiles) {
    LOG(DEBUGL) << "Concatenating files in partition " << part;
    std::vector<string> filestoconcat;

    for(auto child : *rangeFiles) {
	string ext = Utils::extension(child);
	if (ext == string(".") + to_string(part)) {
	    filestoconcat.push_back(child);
	}
    }

    if (filestoconcat.size() > 1) {
        string dest = filestoconcat[0];
        std::ofstream fdest(dest, std::ios_base::binary | std::ios_base::app | std::ios_base::ate);
	std::vector<char> buffer;
        for (int i = 1; i < filestoconcat.size(); ++i) {
            std::ifstream input(filestoconcat[i], std::ios_base::binary);
	    uint64_t size = Utils::fileSize(filestoconcat[i]);
	    if (buffer.size() < size) {
		buffer.resize(size);
	    }
	    input.read(&buffer[0], size);
	    fdest.write(&buffer[0], size);
            input.close();
            Utils::remove(filestoconcat[i]);
        }
        fdest.close();
    }
}

void Compressor::concatenateFiles(std::vector<std::string> &rangeFiles,
        int parallelProcesses,
        int maxReadingThreads) {

    std::thread *threads = new std::thread[maxReadingThreads-1];
    int part = 0;
    while (part < parallelProcesses) {
        for (int i = 1; i < maxReadingThreads; ++i) {
            threads[i-1] = std::thread(Compressor::concatenateFiles_seq,
                    part + i, &rangeFiles);
        }
	concatenateFiles_seq(part, &rangeFiles);
        for (int i = 1; i < maxReadingThreads; ++i) {
            threads[i-1].join();
        }
        part += maxReadingThreads;
    }
    delete[] threads;
}

void Compressor::sortPartitionsAndAssignCounters(const string dirPrefix,
	string prefixInputFile,
        string dictfile,
        string outputfile, int partitions,
        int64_t & counter, int parallelProcesses, int maxReadingThreads) {

    string parentDir = Utils::parentDir(prefixInputFile);
    std::vector<string> children = Utils::getFiles(parentDir);
    std::vector<string> rangeFiles;
    for(auto child : children) {
        if (Utils::contains(child, "range") && Utils::hasExtension(child)) {
	    if (Utils::fileSize(child) > 0) {
		rangeFiles.push_back(child);
	    } else {
		Utils::remove(child);
	    }
	}
    }

#if 0
    //Before I start sorting the files, I concatenate files together
    concatenateFiles(rangeFiles, parallelProcesses, maxReadingThreads);
    // When concatenating, again determine the list of files.
    children = Utils::getFiles(parentDir);
    rangeFiles.clear();
    for(auto child : children) {
        if (Utils::contains(child, "range") && Utils::hasExtension(child)) {
	    rangeFiles.push_back(child);
	}
    }
#endif

    std::vector<string> outputfiles;
    std::vector<uint64_t> counters(partitions);

    DiskLZ4Writer **writers = new DiskLZ4Writer*[maxReadingThreads];
    MultiDiskLZ4Writer **dictwriters = new MultiDiskLZ4Writer*[maxReadingThreads];
    MultiDiskLZ4Reader **mreaders = new MultiDiskLZ4Reader*[maxReadingThreads];
    MultiMergeDiskLZ4Reader **mergereaders = new MultiMergeDiskLZ4Reader*[maxReadingThreads];
    ParamsSortPartition params;

    for (int j = 0; j < maxReadingThreads; ++j) {
	string out = prefixInputFile + "-sortedpart-" + to_string(j);
	outputfiles.push_back(out);
	writers[j] = new DiskLZ4Writer(out, partitions / maxReadingThreads, 3);
	mergereaders[j] = new MultiMergeDiskLZ4Reader(
		partitions / maxReadingThreads * NSORT, 3, 8);
	mergereaders[j]->start();

	std::vector<string> dictfiles;
	int filesPerPart = partitions / maxReadingThreads;
	for (int k = 0; k < filesPerPart; ++k) {
	    string dictpartfile = dictfile + string(".") +
		to_string(k * maxReadingThreads  + j);
	    dictfiles.push_back(dictpartfile);
	}
	dictwriters[j] = new MultiDiskLZ4Writer(dictfiles, 3, 4);
    }

#define MAXPAR	8
    // Don't run sortPartition for all partitions simultaneously, if there are many partitions
    // (say more than 8). Instead, opt for more memory per thread, and divide the surplus of threads
    // among the sorters.

    int nparallel = partitions;
    if (partitions > MAXPAR) {
	// We want a number that is a multiple of maxReadingThreads, and a dividor of partitions.
	nparallel = maxReadingThreads;
	while (nparallel < MAXPAR) {
	    if (partitions % (nparallel * 2) == 0) {
		nparallel *= 2;
	    } else {
		break;
	    }
	}
    }

    std::vector<std::thread> threads(partitions);

    int threadsPerPartition = partitions / nparallel;
    // Be a bit optimistic, bump it up to the next power of 2, since the threads won't be all sorting at the
    // same time.
    if (threadsPerPartition > 1) {
	int p = 1;
	while (p <= threadsPerPartition) {
	    p <<= 1;
	}
	threadsPerPartition = p;
    }

    int64_t maxMem = max((int64_t) 128 * 1024 * 1024,
            (int64_t) (Utils::getSystemMemory() * 0.7)) / nparallel;
    LOG(DEBUGL) << "nparallel = " << nparallel << ", max memory per thread " << maxMem;

    for (int i = 0; i < partitions; i += nparallel) {
	for (int j = 0; j < maxReadingThreads; ++j) {
	    mreaders[j] = new MultiDiskLZ4Reader(nparallel / maxReadingThreads, 3, 4);
	    mreaders[j]->start();
	}

	for (int j = 0; j < nparallel; j++) {
	    int part = i + j;
	    params.dirPrefix = dirPrefix;
	    params.reader = mreaders[j % maxReadingThreads];
	    params.mergerReader = mergereaders[part % maxReadingThreads];
	    params.dictWriter = dictwriters[j % maxReadingThreads];
	    params.idDictWriter = part / maxReadingThreads;
	    params.writer = writers[j % maxReadingThreads];
	    params.idWriter = part / maxReadingThreads;
	    params.idReader = j / maxReadingThreads;
	    params.prefixIntFiles = outputfiles[part % maxReadingThreads] + to_string(part);
	    params.part = part;
	    params.counter = &counters[part];
	    params.maxMem = maxMem;
	    params.threadsPerPartition = threadsPerPartition;

	    threads[j] = std::thread(std::bind(Compressor::sortPartition,
			params));
	}
	for (int j = 0; j < nparallel; ++j) {
	    threads[j].join();
	}

	for (int j = 0; j < maxReadingThreads; ++j) {
	    LOG(DEBUGL) << "Delete multidisk reader " << j;
	    delete mreaders[j];
	}
    }

    for (int j = 0; j < maxReadingThreads; ++j) {
	LOG(DEBUGL) << "Delete multidisk merge reader " << j;
	mergereaders[j]->stop();
	delete mergereaders[j];
	LOG(DEBUGL) << "Delete writer " << j;
	delete writers[j];
	LOG(DEBUGL) << "Delete dict writer " << j;
	delete dictwriters[j];
    }

    delete[] writers;
    delete[] dictwriters;
    delete[] mreaders;
    delete[] mergereaders;

    LOG(DEBUGL) << "Finished sorting partitions. Now shuffling by triple ID";

    //Re-read the sorted tuples and write by tripleID
    DiskLZ4Reader **readers = new DiskLZ4Reader*[maxReadingThreads];
    MultiDiskLZ4Writer **twriters = new MultiDiskLZ4Writer*[maxReadingThreads];
    std::mutex *mutexes = new std::mutex[parallelProcesses];
    for (int i = 0; i < maxReadingThreads; ++i) {
        readers[i] = new DiskLZ4Reader(outputfiles[i],
                partitions / maxReadingThreads,
                3);
        int filesToPart = partitions / maxReadingThreads;
        std::vector<string> filesForThread;
        for (int j = 0; j < filesToPart; ++j) {
            string outfile = outputfile + string(".0.") +
                to_string(j * maxReadingThreads + i);
            filesForThread.push_back(outfile);
        }
        twriters[i] = new MultiDiskLZ4Writer(filesForThread, filesToPart, 3);
    }

    int64_t startCounter = counter;
    for (int i = 0; i < partitions; ++i) {
        threads[i] = std::thread(std::bind(
                    &Compressor::assignCountersAndPartByTripleID,
                    startCounter, readers[i % maxReadingThreads],
                    i / maxReadingThreads,
                    twriters,
                    mutexes,
                    parallelProcesses,
                    maxReadingThreads));
        startCounter += counters[i];
    }
    for (int i = 0; i < partitions; ++i) {
        threads[i].join();
    }

    for (int i = 0; i < partitions; ++i) {
        twriters[i % maxReadingThreads]->setTerminated(i / maxReadingThreads);
    }

    for (int i = 0; i < maxReadingThreads; ++i) {
        delete readers[i];
        Utils::remove(outputfiles[i]);
        Utils::remove(outputfiles[i] + ".idx");
        delete twriters[i];
    }
    delete[] readers;
    delete[] mutexes;
    delete[] twriters;
}

void Compressor::assignCountersAndPartByTripleID(int64_t startCounter,
        DiskLZ4Reader *r, int idReader, MultiDiskLZ4Writer **writers,
        std::mutex *locks,
        int partitions,
        int maxReadingThreads) {

    //LZ4Writer **outputs = new LZ4Writer*[parallelProcesses];
    //for (int i = 0; i < parallelProcesses; ++i) {
    //    outputs[i] = new LZ4Writer(outfile + string(".") + to_string(i));
    //}
    std::vector<std::vector<std::pair<int64_t, int64_t>>> buffers;
    buffers.resize(partitions);

    const int64_t maxNProcessedTuples = 10000000;
    int64_t counter = 0;
    while (!r->isEOF(idReader)) {
        const int64_t c = r->readLong(idReader);
        const int64_t tid = r->readLong(idReader);
        const  int idx = (int64_t) (tid >> 2) % partitions;
        if (counter == maxNProcessedTuples) {
            int nProcessedParts = 0;
            int currentPart = 0;
            int skippedParts = 0;
            std::vector<bool> processedParts(partitions);
            for (int i = 0; i < partitions; ++i) {
                processedParts[i] = false;
            }
            while (nProcessedParts < partitions) {
                if (!buffers[currentPart].empty()) {
                    //Check if we can get a lock
                    bool resp = locks[currentPart].try_lock();
                    if (resp || skippedParts == partitions) {
                        if (!resp) {
                            locks[currentPart].lock();
                        }
                        MultiDiskLZ4Writer *writer = writers[currentPart % maxReadingThreads];
                        const int idWriter = currentPart / maxReadingThreads;
                        for (int j = 0; j < buffers[currentPart].size(); ++j) {
                            auto pair = buffers[currentPart][j];
                            writer->writeLong(idWriter, pair.first);
                            writer->writeLong(idWriter, pair.second);
                        }
                        locks[currentPart].unlock();
                        buffers[currentPart].clear();
                        skippedParts = 0;

                        processedParts[currentPart] = true;
                        nProcessedParts++;
                    } else {
                        skippedParts++;
                    }
                } else if (!processedParts[currentPart]) {
                    nProcessedParts++;
                    processedParts[currentPart] = true;
                }
                currentPart = (currentPart + 1) % partitions;
            }
            counter = 0;
        }
        buffers[idx].push_back(make_pair(tid, c + startCounter));
        counter++;

        //outputs[idx]->writeLong(tid);
        //outputs[idx]->writeLong(c + startCounter);
    }

    if (counter > 0) {
        int nProcessedParts = 0;
        int currentPart = 0;
        int skippedParts = 0;
        std::vector<bool> processedParts(partitions);
        for (int i = 0; i < partitions; ++i) {
            processedParts[i] = false;
        }

        while (nProcessedParts < partitions) {
            if (!buffers[currentPart].empty()) {
                //Check if we can get a lock
                bool resp = locks[currentPart].try_lock();
                if (resp || skippedParts == partitions) {
                    if (!resp) {
                        locks[currentPart].lock();
                    }
                    int a = currentPart % maxReadingThreads;
                    MultiDiskLZ4Writer *writer = writers[a];
                    const int idWriter = currentPart / maxReadingThreads;
                    for (int j = 0; j < buffers[currentPart].size(); ++j) {
                        auto pair = buffers[currentPart][j];
                        writer->writeLong(idWriter, pair.first);
                        writer->writeLong(idWriter, pair.second);
                    }
                    locks[currentPart].unlock();
                    buffers[currentPart].clear();
                    skippedParts = 0;

                    processedParts[currentPart] = true;
                    nProcessedParts++;
                } else {
                    skippedParts++;
                }
            } else if (!processedParts[currentPart]) {
                nProcessedParts++;
                processedParts[currentPart] = true;
            }
            currentPart = (currentPart + 1) % partitions;
        }
        counter = 0;
    }
}

void Compressor::mergeNotPopularEntries(string prefixInputFile,
        string dictOutput,
        string outputFile2,
        int64_t * startCounter, int increment,
        int parallelProcesses,
        int maxReadingThreads) {

    //Sample one file: Get boundaries for parallelProcesses range partitions
    string p = Utils::parentDir(prefixInputFile);
    const std::vector<string> boundaries = getPartitionBoundaries(p,
            parallelProcesses);
    assert(boundaries.size() <= parallelProcesses - 1);

    //Range-partitions all the files in the input collection
    LOG(DEBUGL) << "Range-partitions the files...";
    string dirPrefix = Utils::parentDir(prefixInputFile) + DIR_SEP
	+ Utils::filename(prefixInputFile) + "-ranges-";
    for (int i = 0; i < parallelProcesses; ++i) {
	Utils::create_directories(dirPrefix + std::to_string(i));
    }
    rangePartitionFiles(maxReadingThreads, parallelProcesses, dirPrefix, prefixInputFile,
            boundaries);

    //Collect all ranged-partitions files by partition and globally sort them.
    LOG(DEBUGL) << "Sort and assign the counters to the files...";
    sortPartitionsAndAssignCounters(dirPrefix,
	    prefixInputFile,
            dictOutput,
            outputFile2,
            parallelProcesses,
            *startCounter, parallelProcesses,
            maxReadingThreads);
    for (int i = 0; i < parallelProcesses; ++i) {
	Utils::remove_all(dirPrefix + std::to_string(i));
    }
}

void Compressor::sortByTripleID(//vector<string> *inputFiles,
        MultiDiskLZ4Reader *reader,
        DiskLZ4Writer * writer,
        const int idWriter,
        string tmpfileprefix,
        const uint64_t maxMemory) {

    //First sort the input files in chunks of x elements
    int idx = 0;
    vector<string> filesToMerge;
    vector<TriplePair> pairs;
    int64_t count = 0;
    while (!reader->isEOF(idWriter)) {
        if (sizeof(TriplePair) * pairs.size() >= maxMemory) {
            string file = tmpfileprefix + string(".") + to_string(idx++);
            sortAndDumpToFile2(pairs, file);
            filesToMerge.push_back(file);
            pairs.clear();
        }

        TriplePair tp;
        tp.readFrom(idWriter, reader);
        pairs.push_back(tp);

        count++;
        if (count % 100000000 == 0)
            LOG(DEBUGL) << "Loaded " << count << " Memory so far " << Utils::getUsedMemory();
    }

    if (filesToMerge.empty()) {
        LOG(DEBUGL) << "Sorting and dumping all triples";
        //Sort them
        std::sort(pairs.begin(), pairs.end(), TriplePair::sLess);
        //Dump them inmmediately
        for (size_t i = 0; i < pairs.size(); ++i) {
            TriplePair tp = pairs[i];
            writer->writeLong(idWriter, tp.tripleIdAndPosition);
            writer->writeLong(idWriter, tp.term);
        }
    } else {
        LOG(DEBUGL) << "Start merging the fragments";
        if (pairs.size() > 0) {
            string file = tmpfileprefix + string(".") + to_string(idx++);
            sortAndDumpToFile2(pairs, file);
            filesToMerge.push_back(file);
        }
        pairs.clear();

        //Then do a merge sort and write down the results on outputFile
        FileMerger<TriplePair> merger(filesToMerge);
        while (!merger.isEmpty()) {
            TriplePair tp = merger.get();
            writer->writeLong(idWriter, tp.tripleIdAndPosition);
            writer->writeLong(idWriter, tp.term);
        }

        //Remove the input files
        for (int i = 0; i < filesToMerge.size(); ++i) {
            Utils::remove(filesToMerge[i]);
        }
    }
    writer->setTerminated(idWriter);
}

void Compressor::compressTriples(const int maxReadingThreads,
        const int parallelProcesses,
        const int ndicts,
        string * permDirs, int nperms,
        int signaturePerms, vector<string> &notSoUncommonFiles,
        vector<string> &finalUncommonFiles,
        string * tmpFileNames,
        StringCollection * poolForMap,
        ByteArrayToNumberMap * finalMap,
        const bool ignorePredicates) {

    LOG(DEBUGL) << "Start compression threads... ";
    /*** Compress the triples ***/
    int iter = 0;
    while (areFilesToCompress(parallelProcesses, tmpFileNames)) {
        LOG(DEBUGL) << "Start iteration " << iter;
        string prefixOutputFile = "input-" + to_string(iter);

        DiskLZ4Reader **readers = new DiskLZ4Reader*[maxReadingThreads];
        for (int i = 0; i < maxReadingThreads; ++i) {
            readers[i] = new DiskLZ4Reader(tmpFileNames[i],
                    parallelProcesses / maxReadingThreads,
                    3);
        }
        DiskLZ4Reader **uncommonReaders = new DiskLZ4Reader*[maxReadingThreads];
        for (int i = 0; i < maxReadingThreads; ++i) {
            uncommonReaders[i] = new DiskLZ4Reader(finalUncommonFiles[i],
                    parallelProcesses / maxReadingThreads,
                    3);
        }

        //Set up the output
        std::vector<std::vector<string>> chunks;
        chunks.resize(maxReadingThreads);
        //Set up the output files
        for (int i = 0; i < parallelProcesses; ++i) {
            for (int j = 0; j < nperms; ++j) {
                string file = permDirs[j] + DIR_SEP + prefixOutputFile + to_string(i);
                chunks[i % maxReadingThreads].push_back(file);
            }
        }

        MultiDiskLZ4Writer **writers = new MultiDiskLZ4Writer*[maxReadingThreads];
        for (int i = 0; i < maxReadingThreads; ++i) {
            writers[i] = new MultiDiskLZ4Writer(chunks[i], 3, 3);
        }

        std::thread *threads = new std::thread[parallelProcesses - 1];
        ParamsNewCompressProcedure p;
        p.nperms = nperms;
        p.signaturePerms = signaturePerms;
        p.commonMap = iter == 0 ? finalMap : NULL;
        p.parallelProcesses = parallelProcesses;
        p.ignorePredicates = ignorePredicates;
        for (int i = 1; i < parallelProcesses; ++i) {
            p.part = i;
            p.idReader = i / maxReadingThreads;
            p.reader = readers[i % maxReadingThreads];
            p.readerUncommonTerms = uncommonReaders[i % maxReadingThreads];
            p.writer = writers[i % maxReadingThreads];
            p.idxWriter = (i / maxReadingThreads) * nperms;
            threads[i - 1] = std::thread(
                    std::bind(&Compressor::newCompressTriples, p));
        }
        p.idReader = 0;
        p.reader = readers[0];
        p.part = 0;
        p.writer = writers[0];
        p.idxWriter = 0;
        p.readerUncommonTerms = uncommonReaders[0];
        Compressor::newCompressTriples(p);
        for (int i = 1; i < parallelProcesses; ++i) {
            threads[i - 1].join();
        }
        delete[] threads;

        for (int i = 0; i < maxReadingThreads; ++i) {
            delete readers[i];
            Utils::remove(tmpFileNames[i]);
            Utils::remove(tmpFileNames[i] + ".idx");
            delete uncommonReaders[i];
            delete writers[i];
        }
        delete[] readers;
        delete[] writers;
        delete[] uncommonReaders;

        //New iteration!
        finalMap->clear();
        iter++;
        LOG(DEBUGL) << "Finish iteration";
    }
}

void Compressor::sortFilesByTripleSource(string kbPath,
        const int maxReadingThreads,
        const int parallelProcesses,
        const int ndicts, vector<string> uncommonFiles,
        vector<string> &outputFiles) {

    LOG(DEBUGL) << "Memory used so far: " << Utils::getUsedMemory();

    /*** Sort the files which contain the triple source ***/
    vector<vector<string>> inputFinalSorting(parallelProcesses);

    assert(uncommonFiles.size() == 1);
    string pdir = Utils::parentDir(uncommonFiles[0]);
    string nameFile = Utils::filename(uncommonFiles[0]);
    std::vector<string> children = Utils::getFilesWithPrefix(pdir, nameFile);
    for(auto child : children) {
        if (Utils::hasExtension(child)) {
            string ext = Utils::extension(child);
            int idx;
            stringstream(ext.substr(1, ext.length() - 1)) >> idx;
            inputFinalSorting[idx].push_back(child);
        }
    }

    MultiDiskLZ4Reader **readers = new MultiDiskLZ4Reader*[maxReadingThreads];
    DiskLZ4Writer **writers = new DiskLZ4Writer*[maxReadingThreads];
    for (int i = 0; i < maxReadingThreads; ++i) {
        const int filesPerPart = parallelProcesses / maxReadingThreads;
        outputFiles.push_back(kbPath + DIR_SEP + string("listUncommonTerms") + to_string(i));
        writers[i] = new DiskLZ4Writer(outputFiles.back(),
                filesPerPart,
                3);
        readers[i] = new MultiDiskLZ4Reader(filesPerPart, 3, 10);
        readers[i]->start();
        //Add inputs
        for (int j = 0; j < filesPerPart; ++j) {
            int idx = j * maxReadingThreads + i;
            assert(inputFinalSorting[idx].size() < 2);
            readers[i]->addInput(j, inputFinalSorting[idx]);
        }
    }

    LOG(DEBUGL) << "Start threads ...";

    std::thread *threads = new std::thread[parallelProcesses - 1];
    const int64_t maxMem = max((int64_t) MIN_MEM_SORT_TRIPLES,
            (int64_t) (Utils::getSystemMemory() * 0.6) / parallelProcesses);
    for (int i = 1; i < parallelProcesses; ++i) {
        threads[i - 1] = std::thread(
                std::bind(&Compressor::sortByTripleID,
                    readers[i % maxReadingThreads],
                    writers[i % maxReadingThreads],
                    i / maxReadingThreads,
                    kbPath + DIR_SEP + string("listUncommonTerms-tmp") + to_string(i),
                    maxMem));
    }
    sortByTripleID(readers[0], /*&inputFinalSorting[0],*/ writers[0], 0,
            kbPath + DIR_SEP + string("listUncommonTerms-tmp") + to_string(0),
            maxMem);

    for (int i = 1; i < parallelProcesses; ++i) {
        threads[i - 1].join();
    }
    delete[] threads;

    for (int i = 0; i < maxReadingThreads; ++i) {
        delete readers[i];
        delete writers[i];
    }
    delete[] writers;
    delete[] readers;
    for (auto files : inputFinalSorting) {
        for (auto file : files)
            Utils::remove(file);
    }
}

void Compressor::sortDictionaryEntriesByText(string **input,
        const int ndicts,
        const int maxReadingThreads,
        const int parallelProcesses,
        string * prefixOutputFiles,
        ByteArrayToNumberMap * map,
        bool filterDuplicates,
        int sampleInterval,
        bool sample) {
    int64_t maxMemAllocate = max((int64_t) (BLOCK_SUPPORT_BUFFER_COMPR * 2),
            (int64_t) (Utils::getSystemMemory() * 0.70 / ndicts));
    LOG(DEBUGL) << "Max memory to use to sort inmemory a number of terms: " << maxMemAllocate << " bytes";
    immemorysort(input, maxReadingThreads, parallelProcesses, prefixOutputFiles[0],
            filterDuplicates, maxMemAllocate, sampleInterval, sample);
}

void Compressor::compress(string * permDirs, int nperms, int signaturePerms,
        string * dictionaries,
        int ndicts,
        int parallelProcesses,
        int maxReadingThreads,
        const bool ignorePredicates) {

    /*** Sort the infrequent terms ***/
    LOG(DEBUGL) << "Sorting uncommon dictionary entries for partitions";
    string *uncommonDictionaries = new string[ndicts];
    for (int i = 0; i < ndicts; ++i) {
        uncommonDictionaries[i] = dictionaries[i] + string("-u");
    }
    sortDictionaryEntriesByText(uncommonDictFileNames,
            ndicts,
            maxReadingThreads,
            parallelProcesses,
            uncommonDictionaries,
            NULL,
            false,
            1000,   // sample rate. TODO: make it depend on size of input?
            true);
    LOG(DEBUGL) << "...done";

    /*** Deallocate the dictionary files ***/
    for (int i = 0; i < maxReadingThreads; ++i) {
        delete[] dictFileNames[i];
        Utils::remove(uncommonDictFileNames[i][0]);
        Utils::remove(uncommonDictFileNames[i][0] + string(".idx"));
        delete[] uncommonDictFileNames[i];
    }
    delete[] dictFileNames;
    delete[] uncommonDictFileNames;

    /*** Create the final dictionaries to be written and initialize the
     * counters and other data structures ***/
    LZ4Writer **writers = new LZ4Writer*[ndicts];
    int64_t *counters = new int64_t[ndicts];
    vector<string> notSoUncommonFiles;
    vector<string> uncommonFiles;

    for (int i = 0; i < ndicts; ++i) {
        writers[i] = new LZ4Writer(dictionaries[i]);
        counters[i] = i;
        notSoUncommonFiles.push_back(dictionaries[i] + string("-np1"));
        uncommonFiles.push_back(dictionaries[i] + string("-np2"));
    }

    /*** Assign a number to the popular entries ***/
    LOG(DEBUGL) << "Assign a number to " << (uint64_t)finalMap->size() <<
        " popular terms in the dictionary";
    assignNumbersToCommonTermsMap(finalMap, counters, writers, NULL, ndicts, true);

    /*** Assign a number to the other entries. Split them into two files.
     * The ones that must be loaded into the hashmap, and the ones used for
     * the merge join ***/
    LOG(DEBUGL) << "Merge (and assign counters) of dictionary entries";
    if (ndicts > 1) {
        LOG(ERRORL) << "The current version of the code supports only one dictionary partition";
        throw 10;
    }
    mergeNotPopularEntries(uncommonDictionaries[0], dictionaries[0],
            uncommonFiles[0], &counters[0], ndicts,
            parallelProcesses, maxReadingThreads);
    LOG(DEBUGL) << "... done";

    /*** Remove unused data structures ***/
    for (int i = 0; i < ndicts; ++i) {
        delete writers[i];
    }
    delete[] uncommonDictionaries;
    delete[] writers;

    /*** Sort files by triple source ***/
    LOG(DEBUGL) << "Sort uncommon triples by triple id";
    vector<string> sortedFiles;
    sortFilesByTripleSource(kbPath, maxReadingThreads, parallelProcesses,
            ndicts, uncommonFiles, sortedFiles);
    LOG(DEBUGL) << "... done";

    /*** Compress the triples ***/
    compressTriples(maxReadingThreads, parallelProcesses, ndicts,
            permDirs, nperms, signaturePerms,
            notSoUncommonFiles, sortedFiles, tmpFileNames,
            poolForMap, finalMap, ignorePredicates);
    LOG(DEBUGL) << "... done";

    /*** Clean up remaining datastructures ***/
    delete[] counters;
    for (int i = 0; i < maxReadingThreads; ++i) {
        Utils::remove(sortedFiles[i]);
        Utils::remove(sortedFiles[i] + ".idx");
    }
    delete[] tmpFileNames;
    delete poolForMap;
    delete finalMap;
    poolForMap = NULL;
    finalMap = NULL;
    LOG(DEBUGL) << "Compression is finished";
}

bool stringComparator(string stringA, string stringB) {
    const char *ac = stringA.c_str();
    const char *bc = stringB.c_str();
    for (int i = 0; ac[i] != '\0' && bc[i] != '\0'; i++) {
        if (ac[i] != bc[i]) {
            return ac[i] < bc[i];
        }
    }
    return stringA.size() < stringB.size();
}

Compressor::~Compressor() {
    if (finalMap != NULL)
        delete finalMap;
    if (poolForMap != NULL)
        delete poolForMap;
}

uint64_t Compressor::calculateSizeHashmapCompression() {
    int64_t memoryAvailable = static_cast<int64_t>(Utils::getSystemMemory() * 0.70);
    return memoryAvailable;
}

uint64_t Compressor::calculateMaxEntriesHashmapCompression() {
    int64_t memoryAvailable = min((int)(Utils::getSystemMemory() / 3 / 50), 90000000);
    return memoryAvailable;
}

bool _lessExtensions(const string & a, const string & b) {
    string ea = Utils::extension(a);
    string eb = Utils::extension(b);

    int idxa, idxb;
    std::istringstream ss1(ea.substr(1, ea.length() - 1));
    ss1 >> idxa;
    std::istringstream ss2(eb.substr(1, eb.length() - 1));
    ss2 >> idxb;
    return idxa < idxb;
}

std::vector<string> Compressor::getAllDictFiles(string prefixDict) {
    std::vector<string> output;

    string parentDir = Utils::parentDir(prefixDict);
    string filename = Utils::filename(prefixDict);
    std::vector<string> children = Utils::getFilesWithPrefix(parentDir, filename);
    for(auto child : children) {
        if (Utils::hasExtension(child)) {
            output.push_back(child);
        }
    }

    std::sort(output.begin(), output.end(), _lessExtensions);
    return output;
}
