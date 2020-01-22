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

#ifndef COMPRESSOR_H_
#define COMPRESSOR_H_

#include <kognac/filereader.h>
#include <kognac/hashtable.h>

#include <kognac/lz4io.h>
#include <kognac/hashfunctions.h>
#include <kognac/hashmap.h>
#include <kognac/factory.h>

#include <kognac/diskreader.h>
#include <kognac/disklz4writer.h>
#include <kognac/disklz4reader.h>
#include <kognac/multidisklz4writer.h>
#include <kognac/multidisklz4reader.h>
#include <kognac/multimergedisklz4reader.h>

#ifdef COUNTSKETCH
#include <kognac/CountSketch.h>
#endif

#include <kognac/MisraGries.h>

#include <sparsehash/dense_hash_map>

#include <queue>
#include <iostream>
#include <string>
#include <vector>
#include <list>
#include <set>
#include <thread>
#include <mutex>
#include <assert.h>

using namespace std;

#define IDX_SPO 0
#define IDX_OPS 1
#define IDX_POS 2
#define IDX_SOP 3
#define IDX_OSP 4
#define IDX_PSO 5

class SchemaExtractor;
struct ParamsExtractCommonTermProcedure {
    DiskLZ4Reader *reader;
    int idReader;

    Hashtable **tables;
    GStringToNumberMap *map;
    int dictPartitions;
    int maxMapSize;
    int idProcess;
    int parallelProcesses;
    uint64_t thresholdForUncommon;
    bool copyHashes;
    bool ignorePredicates;
};

struct ParamsNewCompressProcedure {
    int nperms;
    int signaturePerms;
    int part;
    int parallelProcesses;
    DiskLZ4Reader *reader;
    int idReader;
    bool ignorePredicates;
    ByteArrayToNumberMap *commonMap;
    DiskLZ4Reader *readerUncommonTerms;

    MultiDiskLZ4Writer *writer;
    int idxWriter;
};

struct ParamsUncompressTriples {
    DiskReader *reader;
    Hashtable *table1;
    Hashtable *table2;
    Hashtable *table3;
    DiskLZ4Writer *writer;
    int idwriter;
    SchemaExtractor *extractor;
    int64_t *distinctValues;
    std::vector<string> *resultsMGS;
    size_t sizeHeap;
    bool ignorePredicates;
};

struct ParamsSortPartition {
    std::string dirPrefix;
    MultiDiskLZ4Reader *reader;
    MultiMergeDiskLZ4Reader *mergerReader;
    MultiDiskLZ4Writer *dictWriter;
    int idDictWriter;
    //string dictfile;
    DiskLZ4Writer *writer;
    int idWriter;
    int idReader;
    string prefixIntFiles;
    int part;
    uint64_t *counter;
    int64_t maxMem;
    int threadsPerPartition;
};

struct TriplePair {
    int64_t tripleIdAndPosition;
    int64_t term;

    void readFrom(LZ4Reader *reader) {
        tripleIdAndPosition = reader->parseLong();
        term = reader->parseLong();
    }

    void readFrom(int idReader, DiskLZ4Reader *reader) {
        tripleIdAndPosition = reader->readLong(idReader);
        term = reader->readLong(idReader);
    }

    void writeTo(LZ4Writer *writer) {
        writer->writeLong(tripleIdAndPosition);
        writer->writeLong(term);
    }

    bool greater(const TriplePair &t1) const {
        return tripleIdAndPosition > t1.tripleIdAndPosition;
    }

    static bool sLess(const TriplePair &t1, const TriplePair &t2) {
        return t1.tripleIdAndPosition < t2.tripleIdAndPosition;
    }

};

#define PREFIX_HEADER "<http://"
#define PREFIX_HEADER_LEN 8

struct SimplifiedAnnotatedTerm {
    const char *term;
    int64_t tripleIdAndPosition;
    //int prefixid;
    const char *prefix;
    int size;
    int prefixSize;

    SimplifiedAnnotatedTerm() {
        prefix = NULL;
        size = 0;
        prefixSize = 0;
    }

    void readFrom(const int id, DiskLZ4Reader *reader) {
        term = reader->readString(id, size);
        tripleIdAndPosition = reader->readLong(id);
    }

    void readFrom(LZ4Reader *reader) {
        term = reader->parseString(size);
        tripleIdAndPosition = reader->parseLong();
    }

    void writeTo(LZ4Writer *writer) {
        if (prefix != NULL) {
            int64_t len = prefixSize + PREFIX_HEADER_LEN + size;
            writer->writeVLong(len);
            writer->writeRawArray(PREFIX_HEADER, PREFIX_HEADER_LEN);
            writer->writeRawArray(prefix + 2, prefixSize);
            writer->writeRawArray(term, size);
        } else {
            writer->writeString(term, size);
        }
        writer->writeLong(tripleIdAndPosition);
    }

    void writeTo(const int id,
                 DiskLZ4Writer *writer) {
        if (prefix != NULL) {
            int64_t len = prefixSize + PREFIX_HEADER_LEN + size;
            writer->writeVLong(id, len);
            writer->writeRawArray(id, PREFIX_HEADER, PREFIX_HEADER_LEN);
            writer->writeRawArray(id, prefix + 2, prefixSize);
            writer->writeRawArray(id, term, size);
        } else {
            writer->writeString(id, term, size);
        }
        writer->writeLong(id, tripleIdAndPosition);
    }

    bool equals(SimplifiedAnnotatedTerm &t) {
        if (t.prefix == prefix && t.size == size) {
            return memcmp(t.term, term, size) == 0;
        }
        return false;
    }

    bool equals(char *oldterm, int oldsz) {
        if (oldsz != size) {
            return false;
        }
        return memcmp(term, oldterm, size) == 0;
    }

    // splitOffPrefix will try and split of a header, but the header will not yet include
    // the encoding of its length.
    const void splitOffPrefix() {
        if (prefix == NULL && size > 10 && memcmp(term, PREFIX_HEADER, PREFIX_HEADER_LEN) == 0) {
            const char *endprefix = (const char *) memchr(term + PREFIX_HEADER_LEN, '#', size - PREFIX_HEADER_LEN);
            if (endprefix) {
                prefix = term + PREFIX_HEADER_LEN;
                prefixSize = static_cast<int>(endprefix + 1 - prefix);
                size -= prefixSize + PREFIX_HEADER_LEN;
                term = endprefix + 1;
            } else {
                //Try to get subdomain structures
                endprefix = (const char *) memchr(term + PREFIX_HEADER_LEN, '/', size - PREFIX_HEADER_LEN);
                if (endprefix) {
                    prefix = term + PREFIX_HEADER_LEN;
                    prefixSize = static_cast<int>(endprefix + 1 - prefix);
                    size -= prefixSize + PREFIX_HEADER_LEN;
                    term = endprefix + 1;
                } else {
                    prefixSize = 0;
                    prefix = NULL;
                }
            }
            if (prefixSize != 0) {
                assert(term == prefix + prefixSize);
            } else {
                assert(prefix == NULL);
            }
        } else {
            prefixSize = 0;
            prefix = NULL;
        }
    }

    std::string tostring() const {
        if (prefix == NULL) {
            return std::string(term, size);
        }
        return PREFIX_HEADER + std::string(prefix+2, prefixSize) + std::string(term, size);
    }

    void log(std::string v) const {
        LOG(DEBUGL) << v;
        if (prefix != NULL) {
            LOG(DEBUGL) << "prefix: " << std::string(prefix + 2, prefixSize);
        }
        LOG(DEBUGL) << "term: " << std::string(term, size);
    }

#if 0
    static bool sless(const SimplifiedAnnotatedTerm &i,
                      const SimplifiedAnnotatedTerm &j) {
	bool result = i.tostring() < j.tostring();
	if (result != sless1(i, j)) {
	    abort();
	}
	return result;
    }

    // sless assumes that either all terms have at least attempted split-off headers
    // or all terms have no headers.
    static bool sless1(const SimplifiedAnnotatedTerm &i,
#else
    static bool sless(const SimplifiedAnnotatedTerm &i,
#endif
                      const SimplifiedAnnotatedTerm &j) {
        if (i.prefix == j.prefix) {
            // First case: same (or no) header
            int ret = memcmp(i.term, j.term, min(i.size, j.size));
            if (ret == 0) {
                return (i.size - j.size) < 0;
            } else {
                return ret < 0;
            }
        }
        if (i.prefix == NULL) {
            // Now we know j.prefix != NULL, and also that no header could be split off from i.
            // so we only have to compare the header.
            int minsize = min(i.size, PREFIX_HEADER_LEN);
            int ret = memcmp(i.term, PREFIX_HEADER, minsize);
            if (ret != 0) {
                return ret < 0;
            }
            minsize = min(i.size - PREFIX_HEADER_LEN, j.prefixSize);
            ret = memcmp(i.term + PREFIX_HEADER_LEN, j.prefix + 2, minsize);
            if (ret != 0) {
                return ret < 0;
            } else {
		if (minsize == i.size - PREFIX_HEADER_LEN) {
		    LOG(ERRORL) << "Assumption in SimplifiedAnnotatedTerm violated";
		    abort();
		}
		minsize = min(i.size - PREFIX_HEADER_LEN - j.prefixSize, j.size);
		ret = memcmp(i.term + PREFIX_HEADER_LEN + j.prefixSize, j.term, minsize);
		return ret < 0;
            }
        } else if (j.prefix != NULL) {
            // Compare the two prefixes. They should not be equal.
            const int minsize = min(i.prefixSize, j.prefixSize);
            int ret = memcmp(i.prefix + 2, j.prefix + 2, minsize);
            if (ret == 0) {
                if (minsize == i.prefixSize) {
                    ret = memcmp(i.term, j.prefix + 2 + minsize, j.prefixSize - minsize);
                } else {
                    ret = memcmp(i.prefix + 2 + minsize, j.term, i.prefixSize - minsize);
                }
                if (ret == 0) {
                    LOG(ERRORL) << "Assumption in SimplifiedAnnotatedTerm violated";
		    abort();
                }
            }
            return ret < 0;
        } else {
            // Now we know i.prefix != NULL, and also that no header could be split off from j,
            // so we only have to compare the header.
            int minsize = min(PREFIX_HEADER_LEN, j.size);
            int ret = memcmp(PREFIX_HEADER, j.term, minsize);
            if (ret != 0) {
                return ret < 0;
            }
            minsize = min(i.prefixSize, j.size - PREFIX_HEADER_LEN);
            ret = memcmp(i.prefix + 2, j.term + PREFIX_HEADER_LEN, minsize);
            if (ret != 0) {
                return ret < 0;
            } else {
		if (minsize == j.size - PREFIX_HEADER_LEN) {
		    LOG(ERRORL) << "Assumption in SimplifiedAnnotatedTerm violated";
		    abort();
		}
		minsize = min(j.size - PREFIX_HEADER_LEN - i.prefixSize, i.size);
		ret = memcmp(i.term, j.term + PREFIX_HEADER_LEN + i.prefixSize, minsize);
		return ret < 0;
            }
        }
    }

    bool greater(const SimplifiedAnnotatedTerm &t1) const {
        return !sless(*this, t1);
    }

};

struct AnnotatedTerm {
    const char *term;
    int size;
    int64_t tripleIdAndPosition;

    bool useHashes;
    int64_t hashT1, hashT2;

    AnnotatedTerm() {
        term = NULL;
        size = 0;
        tripleIdAndPosition = -1;
        useHashes = false;
    }

    static bool sLess(const AnnotatedTerm &t1, const AnnotatedTerm &t2) {
        int l1 = t1.size - 2;
        int l2 = t2.size - 2;
        int ret = memcmp(t1.term + 2, t2.term + 2, min(l1, l2));
        if (ret == 0) {
            return (l1 - l2) < 0;
        } else {
            return ret < 0;
        }
    }

    bool less(const AnnotatedTerm &t1) const {
        return sLess(*this, t1);
    }

    bool greater(const AnnotatedTerm &t1) const {
        int l1 = size - 2;
        int l2 = t1.size - 2;
        int ret = memcmp(term + 2, t1.term + 2, min(l1, l2));
        if (ret == 0) {
            return (l1 - l2) > 0;
        } else {
            return ret > 0;
        }
    }

    void readFrom(const int id, DiskLZ4Reader *reader) {
        term = reader->readString(id, size);

        char b = reader->readByte(id);
        if (b >> 1 != 0) {
            tripleIdAndPosition = reader->readLong(id);
            if (b & 1) {
                useHashes = true;
                hashT1 = reader->readLong(id);
                hashT2 = reader->readLong(id);
            } else {
                useHashes = false;
            }
        } else {
            tripleIdAndPosition = -1;
            useHashes = false;
        }
    }

    void readFrom(LZ4Reader *reader) {
        term = reader->parseString(size);

        char b = reader->parseByte();
        if (b >> 1 != 0) {
            tripleIdAndPosition = reader->parseLong();
            if (b & 1) {
                useHashes = true;
                hashT1 = reader->parseLong();
                hashT2 = reader->parseLong();
            } else {
                useHashes = false;
            }
        } else {
            tripleIdAndPosition = -1;
            useHashes = false;
        }
    }

    void writeTo(LZ4Writer *writer) {
        writer->writeString(term, size);

        if (useHashes) {
            writer->writeByte(3);
            writer->writeLong(tripleIdAndPosition);
            writer->writeLong(hashT1);
            writer->writeLong(hashT2);
        } else {
            if (tripleIdAndPosition == -1) {
                writer->writeByte(0);
            } else {
                writer->writeByte(2);
                writer->writeLong(tripleIdAndPosition);
            }

        }
    }

    void writeTo(const int id, DiskLZ4Writer *writer) {
        writer->writeString(id, term, size);

        if (useHashes) {
            writer->writeByte(id, 3);
            writer->writeLong(id, tripleIdAndPosition);
            writer->writeLong(id, hashT1);
            writer->writeLong(id, hashT2);
        } else {
            if (tripleIdAndPosition == -1) {
                writer->writeByte(id, 0);
            } else {
                writer->writeByte(id, 2);
                writer->writeLong(id, tripleIdAndPosition);
            }
        }
    }

    bool equals(const char *el) {
        int l = Utils::decode_short(el);
        if (l == size - 2) {
            return memcmp(term + 2, el + 2, l) == 0;
        }
        return false;
    }

    bool equals(const char *el, int size) {
        if (size == this->size) {
            return memcmp(term + 2, el + 2, size - 2) == 0;
        }
        return false;
    }
};

struct priorityQueueOrder {
    bool operator()(const std::pair<string, int64_t> &lhs,
                    const std::pair<string, int64_t>&rhs) const {
        return lhs.second > rhs.second;
    }
};

class StringCollection;
class LRUSet;

class Compressor {

private:
    const string input;
    const string kbPath;
    int64_t totalCount;
    int64_t nTerms;
    std::shared_ptr<Hashtable> table1;
    std::shared_ptr<Hashtable> table2;
    std::shared_ptr<Hashtable> table3;

    void do_sample(const int dictPartitions, const int sampleArg,
                   const int sampleArg2,
                   const int maxReadingThreads, bool copyHashes,
                   const int parallelProcesses,
                   SchemaExtractor *extractors, vector<FileInfo> *files,
                   GStringToNumberMap *commonTermsMaps);

    void do_mcgs();

    void do_countmin(const int dictPartitions, const int sampleArg,
                     const int parallelProcesses, const int maxReadingThreads,
                     const bool copyHashes, SchemaExtractor *extractors,
                     vector<FileInfo> *files,
                     GStringToNumberMap *commonTermsMaps, bool usemisgra,
                     bool ignorePredicates);

    void do_countmin_secondpass(const int dictPartitions,
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
                                bool ignorePredicates);

    uint64_t getThresholdForUncommon(
        const int parallelProcesses,
        const uint64_t sizeHashTable,
        const int sampleArg,
        int64_t *distinctValues,
        Hashtable **tables1,
        Hashtable **tables2,
        Hashtable **tables3);

    static void concatenateFiles_seq(int part, std::vector<std::string> *rangeFiles);

    static void concatenateFiles(std::vector<std::string> &rangeFiles,
                                 int parallelProcesses,
                                 int maxReadingThreads);

    static std::vector<string> getPartitionBoundaries(const string kbdir,
            const int partitions);

    static void rangePartitionFiles(int readingThreads,
                                    int nthreads,
				    const std::string dirPrefix,
                                    const std::string prefixInputFiles,
                                    const std::vector<string> &boundaries);

    static void sortRangePartitionedTuples(DiskLZ4Reader *reader,
                                           int idxReader,
					   const std::string dirPrefix,
                                           const std::string outputFile,
                                           const std::vector<string> *boundaries);

    static void sortPartitionsAndAssignCounters(const std::string dirPrefix,
	    std::string prefixInputFile,
	    std::string dictfile, string outputfile, int partitions,
            int64_t &counter, int parallelProcesses, int maxReadingThreads);

    static void assignCountersAndPartByTripleID(int64_t startCounter,
            DiskLZ4Reader *reader, int idReader,
            MultiDiskLZ4Writer **writers,
            std::mutex *locks,
            int parallelProcesses,
            int maxReadingThreads);

protected:
    static bool isSplittable(string path);

    string getKBPath() {
        return kbPath;
    }

    void sampleTerm(const char *term, int sizeTerm, int sampleArg,
                    int dictPartitions, GStringToNumberMap * map/*,
                    LRUSet *duplicateCache, LZ4Writer **dictFile*/);

    void uncompressTriples(ParamsUncompressTriples params);

#ifdef COUNTSKETCH
    void uncompressTriplesForMGCS(vector<FileInfo> &files, MG * heap, CountSketch * cs, string outFile,
                                  SchemaExtractor * extractor, int64_t * distinctValues);

    void extractTermsForMGCS(ParamsExtractCommonTermProcedure params, const set<string>& freq,
                             const CountSketch * cs);

    void extractTermForMGCS(const char *term, const int sizeTerm, uint64_t & countFreq, uint64_t & countInfrequent,
                            const int dictPartition, const bool copyHashes, const int64_t tripleId, const int pos,
                            char **prevEntries, int *sPrevEntries, LZ4Writer **dictFile, LZ4Writer **udictFile,
                            const set<string>& freq, const CountSketch * cs);
#endif

    void uncompressAndSampleTriples(vector<FileInfo> &files, string outFile,
                                    string * dictFileName, int dictPartitions,
                                    int sampleArg,
                                    GStringToNumberMap * map,
                                    SchemaExtractor * extractor);

    void extractUncommonTerm(const char *term, const int sizeTerm,
                             ByteArrayToNumberMap * map,
                             const int idwriter,
                             DiskLZ4Writer *writer,
                             const int64_t tripleId,
                             const int pos,
                             const int dictPartitions,
                             const bool copyHashes,
                             char **prevEntries, int *sPrevEntries);

    void extractCommonTerm(const char* term, const int sizeTerm, int64_t & countFrequent,
                           const int64_t thresholdForUncommon, Hashtable * table1,
                           Hashtable * table2, Hashtable * table3, const int dictPartitions,
                           int64_t & minValueToBeAdded,
                           const uint64_t maxMapSize, GStringToNumberMap * map,
                           std::priority_queue<std::pair<string, int64_t>,
                           std::vector<std::pair<string, int64_t> >, priorityQueueOrder> &queue);

    void extractCommonTerms(ParamsExtractCommonTermProcedure params);

    void extractUncommonTerms(const int dictPartitions, DiskLZ4Reader *inputFile,
                              const int inputFileId,
                              const bool copyHashes, const int idProcess,
                              const int parallelProcesses,
                              DiskLZ4Writer *writer,
                              const bool ignorePredicates);

    void mergeCommonTermsMaps(ByteArrayToNumberMap * finalMap,
                              GStringToNumberMap * maps, int nmaps);

    void mergeNotPopularEntries(string prefixInputFile,
                                string globalDictOutput, string outputFile2,
                                int64_t * startCounter, int increment,
                                int parallelProcesses,
                                int maxReadingThreads);

    void assignNumbersToCommonTermsMap(ByteArrayToNumberMap * finalMap,
                                       int64_t * counters, LZ4Writer **writers,
                                       LZ4Writer **invWriters, int ndictionaries,
                                       bool preserveMapping);

    static bool areFilesToCompress(int parallelProcesses, string * tmpFileNames);

    static void sortAndDumpToFile(vector<SimplifiedAnnotatedTerm> &vector,
                                  string outputFile,
				  int nthreads,
                                  bool removeDuplicates);

    static void sortAndDumpToFile(vector<SimplifiedAnnotatedTerm> &vector,
                                  DiskLZ4Writer *writer,
                                  const int id);

    static void sortAndDumpToFile2(vector<TriplePair> &pairs, string outputFile);

    static void sortByTripleID(MultiDiskLZ4Reader *reader,
                        //vector<string> *inputFiles,
                        DiskLZ4Writer *writer,
                        const int idWriter,
                        string tmpfileprefix,
                        const uint64_t maxMemory);

    static void immemorysort(string **inputFiles,
                             int maxReadingThreads,
                             int parallelProcesses,
                             string outputFile,
                             //int *noutputFiles,
                             bool removeDuplicates,
                             const int64_t maxSizeToSort,
                             int sampleInterval,
                             bool sample);

    static void inmemorysort_seq(DiskLZ4Reader *reader,
                                 DiskLZ4Writer *writer,
                                 MultiDiskLZ4Writer *sampleWriter,
                                 const int idReader,
                                 int idx,
                                 const uint64_t maxMemPerThread,
                                 bool removeDuplicates,
                                 int sampleInterval,
                                 bool sample);

    static uint64_t calculateSizeHashmapCompression();

    static uint64_t calculateMaxEntriesHashmapCompression();

public:
	KLIBEXP Compressor(string input, string kbPath);

	KLIBEXP static void addPermutation(const int permutation, int &output);

	KLIBEXP static void parsePermutationSignature(int signature, int *output);

	KLIBEXP uint64_t getEstimatedFrequency(const string & el) const;

	KLIBEXP static vector<FileInfo> *splitInputInChunks(const string & input, int nchunks, string prefix = "");

    /*void parse(int dictPartitions, int sampleMethod, int sampleArg, int sampleArg2,
               int parallelProcesses, int maxReadingThreads, bool copyHashes,
               SchemaExtractor * extractor, const bool splitUncommonByHash) {
        parse(dictPartitions, sampleMethod, sampleArg, sampleArg2,
              parallelProcesses, maxReadingThreads, copyHashes, extractor,
              splitUncommonByHash, false, false);
    }*/

	KLIBEXP void parse(int dictPartitions, int sampleMethod, int sampleArg, int sampleArg2,
               int parallelProcesses, int maxReadingThreads, bool copyHashes,
               SchemaExtractor * extractor, bool onlySample, bool ignorePredicates);

	KLIBEXP virtual void compress(string * permDirs, int nperms, int signaturePerms,
                          string * dictionaries, int ndicts,
                          int parallelProcesses,
                          int maxReadingThreads,
                          const bool ignorePredicates);

    string **dictFileNames;
    string **uncommonDictFileNames;
    string *tmpFileNames;
    StringCollection *poolForMap;
    ByteArrayToNumberMap *finalMap;

    int64_t getTotalCount() {
        return totalCount;
    }

    int64_t getEstimateNumberTerms() {
        return nTerms;
    }

    void cleanup() {
        table1 = std::shared_ptr<Hashtable>();
        table2 = std::shared_ptr<Hashtable>();
        table3 = std::shared_ptr<Hashtable>();
    }

	KLIBEXP static std::vector<string> getAllDictFiles(string prefixDict);

	KLIBEXP ~Compressor();

    //I make it public only for testing purposes

    static void sortPartition(ParamsSortPartition params);

    static void sortDictionaryEntriesByText(string **input, const int ndicts,
                                            const int maxReadingThreads,
                                            const int parallelProcesses,
                                            string * prefixOutputFiles,
                                            ByteArrayToNumberMap * map,
                                            bool filterDuplicates,
                                            int sampleInterval,
                                            bool sample);

    static void sortFilesByTripleSource(string kbPath,
                                        const int maxReadingThreads,
                                        const int parallelProcesses,
                                        const int ndicts,
                                        vector<string> uncommonFiles,
                                        vector<string> &finalUncommonFiles);

    static void compressTriples(const int maxReadingThreads,
                         const int parallelProcesses,
                         const int ndicts,
                         string * permDirs,
                         int nperms, int signaturePerms,
                         vector<string> &notSoUncommonFiles,
                         vector<string> &finalUncommonFiles, string * tmpFileNames,
                         StringCollection * poolForMap,
                         ByteArrayToNumberMap * finalMap,
                         const bool ignorePredicates);

    static void newCompressTriples(ParamsNewCompressProcedure params);
};

#endif /* COMPRESSOR_H_ */
