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

#include <string>
#include <vector>
#include <mutex>
#include <list>
#include <climits>

#include <kognac/schemaextractor.h>
#include <kognac/lz4io.h>
#include <kognac/hashmap.h>
#include <kognac/fpgrowth.h>
#include <kognac/compressor.h>

using namespace std;

struct Kognac_TextClassID {
    const char *term;
    int size;
    int64_t classID, classID2;

    void readFrom(LZ4Reader *reader) {
        term = reader->parseString(size);
        classID = reader->parseLong();
        classID2 = reader->parseLong();
        //pred = reader->parseLong();
    }

    void writeTo(LZ4Writer *writer) const {
        writer->writeString(term, size);
        writer->writeLong(classID);
        writer->writeLong(classID2);
        //writer->writeLong(pred);
    }

    bool eqText(const Kognac_TextClassID &el) const {
        if (el.size == size) {
            return memcmp(term, el.term, size) == 0;
        }
        return false;
    }

    bool eqText(const char* t, const int s) const {
        if (size == s) {
            return memcmp(t, term, size) == 0;
        }
        return false;
    }

    static bool lessTextFirst(const Kognac_TextClassID &rhs,
                              const Kognac_TextClassID &lhs) {
        //Check the text
        int resp = memcmp(rhs.term, lhs.term, min(rhs.size, lhs.size));
        if (resp != 0)
            return resp < 0;
        else if (lhs.size != rhs.size) {
            return rhs.size < lhs.size;
        }
        return rhs.classID < lhs.classID ||
               (rhs.classID == lhs.classID && rhs.classID2 < lhs.classID2);
    }

    static bool lessClassIDFirst(const Kognac_TextClassID &rhs,
                                 const Kognac_TextClassID &lhs) {
        if (rhs.classID != lhs.classID) {
            return rhs.classID < lhs.classID;
        }
        int resp = memcmp(rhs.term, lhs.term, min(rhs.size, lhs.size));
        return resp < 0;
    }
};

template<class K, bool M(const Kognac_TextClassID
                         &, const Kognac_TextClassID &)>
class Kognac_TwoWayMerger {
private:
    LZ4Reader *reader1;
    LZ4Reader *reader2;
    K min1;
    K min2;
    K *min;

public:
    Kognac_TwoWayMerger(string file1, string file2) {
        reader1 = new LZ4Reader(file1);
        reader2 = new LZ4Reader(file2);
        min = NULL;
        if (!reader1->isEof()) {
            min1.readFrom(reader1);
            min = &min1;
        } else {
            delete reader1;
            reader1 = NULL;
        }

        if (!reader2->isEof()) {
            min2.readFrom(reader2);
            if (M(min2, min1)) {
                min = &min2;
            }
        } else {
            delete reader2;
            reader2 = NULL;
        }
    }

    bool isEmpty() {
        return min == NULL;
    }

    K get() const {
        return *min;
    }

    void next() {
        if (min == &min1) {
            if (!reader1->isEof()) {
                min1.readFrom(reader1);
            } else {
                delete reader1;
                reader1 = NULL;
            }
        } else {
            //min == min2
            if (!reader2->isEof()) {
                min2.readFrom(reader2);
            } else {
                delete reader2;
                reader2 = NULL;
            }
        }

        //Set the minimum
        if (reader1 != NULL) {
            if (reader2 == NULL || M(min1, min2)) {
                min = &min1;
            } else {
                min = &min2;
            }
        } else if (reader2 != NULL) {
            min = &min2;
        } else {
            min = NULL;
        }
    }
};

class Kognac_TermBufferWriter {
private:
    const int npartitions;
    const string outputfile;
    const bool onlyMinClass;

    std::unique_ptr<int[]> partitionCounters;
    std::unique_ptr<std::unique_ptr<StringCollection>[]> stringBuffers;
    std::unique_ptr<std::vector<Kognac_TextClassID>[]> elementsBuffers;
    int64_t maxMemoryPerBuffer;

    //Used to store a cache of hashes
    std::map<int64_t, int64_t> cacheClassAssignments;
    std::list<int64_t> queueElements;
    //int64_t memReservedForCache;

    int64_t count;
    int64_t countWritten;

    void dumpBuffer(const int partition);

public:
    Kognac_TermBufferWriter(const int64_t maxMem, const int npartitions,
                            string outputfile, const bool onlyMinClass);

    void write(const Kognac_TextClassID &pair);

    int64_t getClassFromCache(const int64_t hashTerm) {
        int64_t cacheId = LONG_MAX;
        if (cacheClassAssignments.count(hashTerm)) {
            cacheId = cacheClassAssignments.find(hashTerm)->second;
        }
        return cacheId;
    }

    /*int64_t getClassFromCache2(const char *key, const size_t sizekey) {
        int64_t cacheId = LONG_MAX;
        //TODO
        return cacheId;
    }*/


    void insertInCache(const int64_t key, const int64_t hashClass);

    //void insertInCache2(const char *textKey, const size_t size, const int64_t hashClass);

    void flush();

    ~Kognac_TermBufferWriter();
};

class Kognac {
private:
    //The path that contains the input to read
    const string inputPath;
    //In case the sampling phase has already splitted the input, I keep it here
    std::vector<string> splittedInput;

    //The path where the output will be stored
    const string outputPath;

    //An instance of the Compressor object, which contains the code for the
    //frequency estimation
    std::unique_ptr<Compressor> compr;

    //Contains the schema info and the taxonomy
    SchemaExtractor extractor;

    //Contains the frequent patterns that we will mine from the input
    std::shared_ptr<FPTree<uint64_t> > frequentPatterns;
    std::map<uint64_t, uint64_t> classesWithFrequency;
    std::map<string, uint64_t> classesHash; //Used only for debugging
    std::map<uint64_t, string> classesHash2; //Used only for debugging
    std::mutex mut; //Used for atomic inserts in frequentPatterns
    const int maxPatternLength;

    //The procedure sample populates this list with the most frequent terms
    std::vector<std::pair<string, uint64_t> > mostFrequentTerms;

    //std::vector<std::pair<string, uint64_t>> getTermFrequencies(
    //            const std::set<string> &elements) const;

    void assignIdsToMostPopularTerms(StringCollection &col,
                                     ByteArrayToNumberMap &map,
                                     int64_t &counter,
                                     std::ostream &out);

    void extractAllTermsWithClassIDs(const int nthreads,
                                     const int nReadingThreads,
                                     const bool useFP,
                                     string outputdir,
                                     ByteArrayToNumberMap &frequentTermsMap,
                                     std::map<uint64_t, uint64_t> &c);

    void extractAllTermsWithClassIDs_int(const int64_t maxMem,
                                         DiskLZ4Reader *reader,
                                         const int idReader,
                                         string outputfile,
                                         ByteArrayToNumberMap *frequentTermsMap,
                                         std::map<uint64_t,
                                         uint64_t> *frequencyClasses,
                                         const int nthreads);

    void extractAllTermsWithClassIDsNOFP_int(const int64_t maxMem,
            DiskLZ4Reader *reader,
            int idReader,
            string outputfile,
            ByteArrayToNumberMap *frequentTermsMap,
            std::map<uint64_t,
            uint64_t> *frequencyClasses,
            const int nthreads);

    void mergeAllTermsWithClassIDs(const int nthreads, string inputDir);

    void pickSmallestClassID(const int nthreads,
                             string inputDir,
                             const bool useFP);

    void pickSmallestClassIDPart(string inputFile,
                                 const bool useFP);

    void mergeAllTermsWithClassIDsPart(std::vector<string> inputFiles);

    void processTerm(Kognac_TermBufferWriter &writer, const int pos,
                     const char* term, const char* otherterm1,
                     const char* otherterm2,
                     std::map<uint64_t, uint64_t> *freqsClasses,
                     const bool useFP) const;

    void sortTermsByClassId(string inputdir, string outputdir);

    void assignIdsToAllTerms(string inputdir, int64_t &counter,
                             std::ostream &out);

    void loadDictionaryMap(std::istream &in,
                           CompressedByteArrayToNumberMap &map,
                           StringCollection &supportDictionaryMap);

    int64_t getIDOrText(DiskLZ4Reader *reader, const int idReader,
                     int &size, char *text,
                     const CompressedByteArrayToNumberMap &map);

    void addTransactionToFrequentPatterns(
        std::vector<std::pair<uint64_t,
        uint64_t> > &classes);

    void compressGraph_seq(DiskLZ4Reader *reader,
                           int idReader, string outputUncompressed,
                           const bool firstPass,
                           CompressedByteArrayToNumberMap *map,
                           int64_t *countCompressedTriples,
                           LZ4Writer *finalWriter);

public:
    Kognac(string input, string output, const int maxPatternLength);

    void sample(const int sampleMethod, const int sampleArg1,
                const int sampleArg2, const int parallelThreads,
                const int maxConcurrentThreads);

    void compress(const int nthreads,
                  const int nReadingThreads,
                  const bool useFP,
                  const int minSupport,
                  const bool serializeTaxonomy);

    void compressGraph(const int nthreads, const int nReadingThreads);

    static void sortCompressedGraph(string inputFile, string outputFile, int v = 0);

    ~Kognac();
};
