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
#include <kognac/filemerger.h>
#include <kognac/disklz4reader.h>
#include <kognac/logs.h>
#include <kognac/consts.h>
#include <zstr/zstr.hpp>
#include <algorithm>
#include <string>
#include <mutex>

Kognac::Kognac(string inputPath, string outputPath, const int maxPatternLength)
    : inputPath(inputPath), outputPath(outputPath),
    frequentPatterns(NULL), maxPatternLength(maxPatternLength) {
        if (!Utils::exists(outputPath)) {
            Utils::create_directories(outputPath);
        }

        compr = std::unique_ptr<Compressor>(new Compressor(inputPath, outputPath));
    }

void Kognac::sample(const int sampleMethod, const int sampleArg1,
        const int sampleArg2, const int parallelThreads,
        const int maxConcurrentThreads) {
    //1- Sample the graph
    compr->parse(1, sampleMethod, sampleArg1, sampleArg2, parallelThreads,
            maxConcurrentThreads, true, &extractor, true, false);

    //2- Store in the internal list the most frequent terms
    for (ByteArrayToNumberMap::iterator itr = compr->finalMap->begin();
            itr != compr->finalMap->end(); ++itr) {
        const int sizeString = Utils::decode_short(itr->first);
        if (itr->second > 0) {
            mostFrequentTerms.push_back(
                    make_pair(string(itr->first + 2, sizeString), itr->second));
        }
    }
    LOG(INFOL) << "Detected " << (uint64_t)mostFrequentTerms.size() <<
        " frequent terms";

    //3- Get the temporary files if they exists
    if (compr->tmpFileNames != NULL) {
        for (int i = 0; i < parallelThreads; ++i) {
            if (Utils::exists(compr->tmpFileNames[i])) {
                splittedInput.push_back(compr->tmpFileNames[i]);
            }
        }
    }

    compr->cleanup();
}

bool Kognac_sortByFreqDesc(const std::pair<string, uint64_t> &p1,
        const std::pair<string, uint64_t> &p2) {
    return p1.second > p2.second;
}

/*std::vector<std::pair<string, uint64_t>> Kognac::getTermFrequencies(
  const std::set<string> &elements) const {
  std::vector<std::pair<string, uint64_t>> out;
  for (std::set<string>::const_iterator itr = elements.begin();
  itr != elements.end(); ++itr) {
  uint64_t freq = compr.getEstimatedFrequency(*itr);
  out.push_back(make_pair(*itr, freq));
  }
  sort(out.begin(), out.end(), Kognac_sortByFreqDesc);
  return out;
  }*/

void Kognac::compress(const int nthreads,
        const int nReadingThreads,
        const bool useFP,
        const int minSupport,
        const bool serializeTaxonomy,
        int64_t startCounter) {

    LOG(DEBUGL) << "Used memory at this moment " <<
        Utils::getUsedMemory();

    // Create the output
    int64_t counter = startCounter;
    zstr::ofstream out(outputPath + DIR_SEP + string("dict.gz"), ios_base::binary);
    {
        //Assign the number to the popular terms and copy them in a fast
        //hashmap
        LOG(INFOL) << "Assign an ID to all popular terms ...";
        StringCollection stringPoolForMap(10 * 1024 * 1024);
        ByteArrayToNumberMap frequentTermsMap;
        sort(mostFrequentTerms.begin(), mostFrequentTerms.end(),
                &Kognac_sortByFreqDesc);
        assignIdsToMostPopularTerms(stringPoolForMap, frequentTermsMap,
                counter, out);

        //Calculate the taxonomy
        LOG(INFOL) << "Create a taxonomy of classes ...";
        extractor.prepare();
        std::set<string> allClasses = extractor.getAllClasses();
        std::vector<uint64_t> hashedTerms;
        for (auto &cl : allClasses) {
            uint64_t hashClass = Hashes::murmur3_56(cl.c_str(),
                    static_cast<int>(cl.size()));
            hashedTerms.push_back(hashClass);
            if (!classesWithFrequency.count(hashClass)) {
                classesWithFrequency.insert(make_pair(hashClass, 0));
                classesHash.insert(make_pair(cl, hashClass));
                classesHash2.insert(make_pair(hashClass, cl));
            } else {
                throw 10; //It means multiple classes get the same hash.
                //This is very unlikely, I'll fix if indeed it happens.
            }
        }

        //Annotate each term with a class ID
        LOG(INFOL) << "Annotate the terms with class info ...[threads = "
            << nthreads << "]";
        string tmpDir = outputPath + DIR_SEP + string("extractedTerms");
        Utils::create_directories(tmpDir);
        extractAllTermsWithClassIDs(nthreads, nReadingThreads,
                useFP, tmpDir, frequentTermsMap,
                classesWithFrequency);

        LOG(INFOL) << "Sort and merge the terms by text ... [threads = "
            << nthreads << "]";

        if (useFP) {
            //Sort all terms by the textual ID. Get the frequent patterns
            frequentPatterns = std::shared_ptr <
                FPTree<uint64_t >>
                (new FPTree<uint64_t>(0, hashedTerms, 1000));
            mergeAllTermsWithClassIDs(nthreads, tmpDir);

            //Extract frequent patterns
            throw 10;
    } else {
        mergeAllTermsWithClassIDs(nthreads, tmpDir);
    }

    //For each term, pick the smallest class ID
    LOG(INFOL) << "Pick smallest class IDs ... [threads = " << nthreads << "]";
    pickSmallestClassID(nthreads, tmpDir, useFP);

    //Re-sort the terms by class ID
    LOG(INFOL) << "Sort and merge the terms by class ID...";
    string tmpDir2 = outputPath + DIR_SEP + string("sortedByClass");
    Utils::create_directories(tmpDir2);
    sortTermsByClassId(tmpDir, tmpDir2);
    Utils::remove_all(tmpDir);

    //Assign IDs
    LOG(INFOL) << "Assign an ID to all terms ...";
    assignIdsToAllTerms(tmpDir2, counter, out);
    Utils::remove_all(tmpDir2);
}

if (serializeTaxonomy) {
    LOG(INFOL) << "Serializing the taxonomy ...";
    string path = outputPath + DIR_SEP + string("taxonomy.gz");
    extractor.serialize(path);
}

compr = std::unique_ptr<Compressor>();
}

void Kognac::loadDictionaryMap(std::istream &in,
        CompressedByteArrayToNumberMap &map,
        StringCollection &supportDictionaryMap) {

    const uint64_t maxMem = static_cast<uint64_t>(Utils::getSystemMemory() * 0.8);
    LOG(DEBUGL) << "Max memory to use during the loading: " <<
        maxMem;
    char supportTerm[MAX_TERM_SIZE];
    int64_t counter = 0;
    for (std::string line; std::getline(in, line); ) {
        //Line contains the ID, and the size of the string, and the string
        int64_t idTerm, sizeString;
        stringstream stream(line);
        stream >> idTerm;
        stream >> sizeString;
        stream.get(); //blank space
        stream.read(supportTerm + 2, sizeString);
        Utils::encode_short(supportTerm, static_cast<int>(sizeString));
        const char* term = supportDictionaryMap.addNew(supportTerm,
			static_cast<int>(sizeString + 2));
        map.insert(make_pair(term, idTerm));

        uint64_t memEstimate = supportDictionaryMap.occupiedBytes() +
            map.size() * 20;
        if (counter++ % 1000000 == 0) {
            LOG(DEBUGL) << "Added " << (counter - 1) <<
                " records. Memory so far " <<
                Utils::getUsedMemory() <<
                " my estimate is " << memEstimate ;
        }

        if (memEstimate > maxMem) {
            break;
        }
    }
    LOG(DEBUGL) << "The hashmap contains " <<
        (uint64_t)map.size() << " terms";
}

int64_t Kognac::getIDOrText(DiskLZ4Reader *reader, int idReader, int &size, char *text,
        const CompressedByteArrayToNumberMap &map) {
    char flag = reader->readByte(idReader);
    int64_t term = -1;
    if (flag) {
        term = reader->readLong(idReader);
    } else {
        //try to compress it
        const char *tmpPointer = reader->readString(idReader, size);
        if (map.count(tmpPointer)) {
            term = map.find(tmpPointer)->second;
        } else {
            memcpy(text, tmpPointer, size);
        }
    }
    return term;
}

void Kognac::compressGraph_seq(DiskLZ4Reader *reader, const int idReader,
        string outputUncompressed,
        const bool firstPass,
        CompressedByteArrayToNumberMap *map,
        int64_t *countCompressedTriples,
        LZ4Writer *finalWriter) {
    char supportBuffer1[MAX_TERM_SIZE];
    char supportBuffer2[MAX_TERM_SIZE];
    char supportBuffer3[MAX_TERM_SIZE];
    int s1, s2, s3;

    int64_t c = 0;
    bool nonempty = false;
    //Compress it
    {
        LZ4Writer tmpWriter(outputUncompressed);
        int64_t counter = 0;
        while (!reader->isEOF(idReader)) {
            const int64_t s = getIDOrText(reader, idReader, s1, supportBuffer1, *map);
            const int64_t p = getIDOrText(reader, idReader, s2, supportBuffer2, *map);
            const int64_t o = getIDOrText(reader, idReader, s3, supportBuffer3, *map);
            if (s >= 0 && p >= 0 && o >= 0) {
                c++;
                //triple is fully compressed
                Triple t;
                t.s = s;
                t.p = p;
                t.o = o;
                t.writeTo(finalWriter);
            } else {
                nonempty = true;
                //Must wait the second round
                if (s == -1) {
                    tmpWriter.writeByte(0);
                    tmpWriter.writeString(supportBuffer1, s1);
                } else {
                    tmpWriter.writeByte(1);
                    tmpWriter.writeLong(s);
                }
                if (p == -1) {
                    tmpWriter.writeByte(0);
                    tmpWriter.writeString(supportBuffer2, s2);
                } else {
                    tmpWriter.writeByte(1);
                    tmpWriter.writeLong(p);
                }
                if (o == -1) {
                    tmpWriter.writeByte(0);
                    tmpWriter.writeString(supportBuffer3, s3);
                } else {
                    tmpWriter.writeByte(1);
                    tmpWriter.writeLong(o);
                }
            }

            if ((++counter % 1000000) == 0) {
                LOG(DEBUGL) << "Passed " <<
                    (counter) << " triples";
            }
        }
    }
    *countCompressedTriples += c;
    if (!nonempty) {
        Utils::remove(outputUncompressed);
    }
}

void Kognac::compressGraph(const int nthreads, const int nReadingThreads) {

    LOG(DEBUGL) << "Used memory at this moment " <<
        Utils::getUsedMemory();

    //The dictionary
    zstr::ifstream in(outputPath + DIR_SEP + string("dict.gz"), ios_base::in);
    //Create a temporary directory where to store the partially compr. files
    string workingDir = outputPath + DIR_SEP + "tmp_graph_compr";
    Utils::create_directories(workingDir);
    {
        //Create a large hash map
        CompressedByteArrayToNumberMap map;
        map.set_deleted_key(DELETED_KEY);
        StringCollection col(10 * 1024 * 1024);

        // Write where to write the sorted triples.
        std::vector<LZ4Writer*> finalWriters;
        std::unique_ptr<int64_t[]> counters(new int64_t[nthreads]);
        for (int i = 0; i < nthreads; ++i) {
            counters[i] = 0;
            finalWriters.push_back(new LZ4Writer(
                        workingDir + DIR_SEP + "triples_unsorted" +
                        to_string(i)));
        }
        bool firstPass = true;
        int incr = 0;
        while (true) {
            std::vector<string> inputFiles;
            if (firstPass) {
                //Read the files created during the sampling procedure
                inputFiles = splittedInput;
            } else {
                //Read the files in the working directory
                inputFiles = Utils::getFiles(workingDir);
            }

            //Load the hashmap
            col.clear();
            map.clear();
            loadDictionaryMap(in, map, col);
            std::vector<string> filesToProcess;
            for (const auto el : inputFiles) {
                if (Utils::filename(el).find("triples_unsorted") != string::npos)
                    continue;
                filesToProcess.push_back(el);
            }

            if (filesToProcess.size() > nthreads) {
                LOG(INFOL) << "There should not be more files than available threads";
                throw 10;
            } else if (filesToProcess.size() == 0) {
                break;
            } else if (filesToProcess.size() != nReadingThreads) {
                LOG(ERRORL) << "The number of files should be equal to the number of reading threads";
                throw 10;
            }


            //Set up the input
            DiskLZ4Reader **readers = new DiskLZ4Reader*[nReadingThreads];
            for (int i = 0; i < nReadingThreads; ++i) {
                readers[i] = new DiskLZ4Reader(inputFiles[i],
                        nthreads / nReadingThreads, 3);
            }

            std::thread *threads = new std::thread[nthreads];
            for (int idxThread = 0; idxThread < nthreads; ++idxThread) {
                threads[idxThread] = std::thread(
                        std::bind(
                            &Kognac::compressGraph_seq,
                            this,
                            readers[idxThread % nReadingThreads],
                            idxThread / nReadingThreads,
                            workingDir + DIR_SEP + "file" +
                            to_string(incr++),
                            firstPass,
                            &map,
                            counters.get() + idxThread,
                            finalWriters[idxThread]));
            }

            //Join the threads
            for (int i = 0; i < nthreads; ++i) {
                threads[i].join();
            }
            delete[] threads;
            if (firstPass) {
                firstPass = false;
            }

            for (int i = 0; i < nReadingThreads; ++i) {
                delete readers[i];
            }
            delete[] readers;
        }
        int64_t countCompressedTriples = 0;
        for (int i = 0; i < nthreads; ++i) {
            delete finalWriters[i];
            countCompressedTriples += counters[i];
        }
        LOG(DEBUGL) << "Compressed triples so far " <<
            countCompressedTriples;
        map.clear();
        col.clear();
        col.deallocate();
    }

    //Sort and remove the duplicates
    LOG(DEBUGL) << "Used memory at this moment " <<
        Utils::getUsedMemory();
    sortCompressedGraph(workingDir, outputPath + DIR_SEP + string("triples.gz"));

    //Remove the tmp dir
    Utils::remove_all(workingDir);
}

void Kognac::sortCompressedGraph(string inputDir, string outputFile, int v) {
    //The output, compressed file
    LOG(INFOL) << "Sorting and removing duplicates ...";
    string diroutput = Utils::parentDir(outputFile);
    char tmpString[1024];
    {
        zstr::ofstream out(outputFile, ios_base::binary);

        //Sort and remove the duplicates
        uint64_t maxTriplesPerSegment = static_cast<uint64_t>(Utils::getSystemMemory() * 0.50 / sizeof(Triple));
        std::vector<Triple> inmemorytriples;
        std::vector<string> outputFiles;
        int id = 0;
        int64_t countTriples = 0;
        std::vector<std::string> childrenFiles = Utils::getFiles(inputDir);
        for (int idx = 0; idx < childrenFiles.size(); ++idx) {
            string fn = childrenFiles[idx];
            //string fn = string(inputDir + "/" + string(dir->d_name));
            if (!Utils::isFile(fn)) {
                continue;
            }
            LZ4Reader reader(fn.c_str());
            if (v == 1)
                reader.parseByte(); //ignore it is a quad
            while (!reader.isEof()) {
                if (v == 0) {
                    Triple t;
                    t.readFrom(&reader);
                    inmemorytriples.push_back(t);
                } else { // v == 1
                    int64_t s = reader.parseLong();
                    int64_t p = reader.parseLong();
                    int64_t o = reader.parseLong();
                    inmemorytriples.push_back(Triple(s, p, o));
                }

                if (inmemorytriples.size() >= maxTriplesPerSegment) {
                    LOG(DEBUGL) << "Writing tmp file " <<
                        to_string(id);
                    cmp c;
                    std::sort(inmemorytriples.begin(), inmemorytriples.end(), c);
                    //Dump the triples in a file
                    string tmpfile = diroutput + DIR_SEP + string("tmp-") + to_string(id);
                    {
                        LZ4Writer writer(tmpfile);
                        for (std::vector<Triple>::iterator
                                itr = inmemorytriples.begin();
                                itr != inmemorytriples.end(); ++itr) {
                            itr->writeTo(&writer);
                        }
                    }
                    outputFiles.push_back(tmpfile);
                    id++;
                    inmemorytriples.clear();
                }
            }
        }

        if (outputFiles.size() > 0) {
            //Dump current file
            if (inmemorytriples.size() > 0) {
                cmp c;
                std::sort(inmemorytriples.begin(), inmemorytriples.end(), c);
                //Dump the triples in a file
                string tmpfile = diroutput + DIR_SEP + string("tmp-") + to_string(id);
                {
                    LZ4Writer writer(tmpfile);
                    for (std::vector<Triple>::iterator itr = inmemorytriples.begin();
                            itr != inmemorytriples.end(); ++itr) {
                        itr->writeTo(&writer);
                    }
                }
                outputFiles.push_back(tmpfile);
                inmemorytriples.clear();
            }

            //Merge sort
            LOG(DEBUGL) << "Merging all segments ... ";
            FileMerger<Triple> merger(outputFiles);
            int64_t prevs = -1;
            int64_t prevp = -1;
            int64_t prevo = -1;
            while (!merger.isEmpty()) {
                Triple t = merger.get();
                if (t.s != prevs || t.p != prevp || t.o != prevo) {
                    sprintf_s(tmpString, 1024, "%lld %lld %lld", t.s, t.p, t.o);
                    out << tmpString << '\n';
                    countTriples++;
                }
                prevs = t.s;
                prevp = t.p;
                prevo = t.o;
            }
        } else {
            cmp c;
            std::sort(inmemorytriples.begin(), inmemorytriples.end(), c);
            LOG(DEBUGL) << "inmemorytriples = " << (uint64_t)inmemorytriples.size();
            int64_t prevs = -1;
            int64_t prevp = -1;
            int64_t prevo = -1;
            for (std::vector<Triple>::iterator itr = inmemorytriples.begin();
                    itr != inmemorytriples.end(); ++itr) {
                if (itr->s != prevs || itr->p != prevp || itr->o != prevo) {
                    sprintf_s(tmpString, 1024, "%lld %lld %lld", itr->s, itr->p, itr->o);
                    out << tmpString << '\n';
                    countTriples++;
                }
                prevs = itr->s;
                prevp = itr->p;
                prevo = itr->o;
            }

        }
        LOG(INFOL) << "Wrote " << countTriples << " triples";
    }
}

void Kognac::sortTermsByClassId(string inputdir, string outputdir) {
    {
        std::vector<string> files = Utils::getFiles(inputdir);
        StringCollection col(10 * 1024 * 1024);
        const uint64_t maxMem = std::max((uint64_t)(10 * 1024 * 1024),
                (uint64_t) (Utils::getSystemMemory() * 0.7));
        int idx = 0;
        std::vector<Kognac_TextClassID> elements;
        for (std::vector<string>::iterator itr = files.begin();
                itr != files.end(); ++itr) {
            //Load each file and sort segments in main memory
            LZ4Reader reader(*itr);
            while (!reader.isEof()) {
                Kognac_TextClassID el;
                el.readFrom(&reader);

                //Copy it in main mem.
                const char *term = col.addNew(el.term, static_cast<int>(el.size));
                el.term = term;
                elements.push_back(el);

                if (sizeof(Kognac_TextClassID)*elements.size() +
                        col.allocatedBytes() > maxMem) {
                    std::sort(elements.begin(), elements.end(),
                            &Kognac_TextClassID::lessClassIDFirst);
                    LZ4Writer writer(outputdir + DIR_SEP + to_string(idx++));
                    for (std::vector<Kognac_TextClassID>::iterator itr =
                            elements.begin(); itr != elements.end(); ++itr) {
                        itr->writeTo(&writer);
                    }
                    elements.clear();
                    col.clear();
                }
            }
        }

        if (elements.size() > 0) {
            std::sort(elements.begin(), elements.end(),
                    &Kognac_TextClassID::lessClassIDFirst);
            LZ4Writer writer(outputdir + DIR_SEP + to_string(idx++));
            for (std::vector<Kognac_TextClassID>::iterator itr =
                    elements.begin(); itr != elements.end(); ++itr) {
                itr->writeTo(&writer);
            }
        }

        //Merge all fragments
        std::vector<string> sortedFiles = Utils::getFiles(outputdir);
        while (sortedFiles.size() > 1) {
            string file1 = sortedFiles.back();
            sortedFiles.pop_back();
            string file2 = sortedFiles.back();
            sortedFiles.pop_back();

            string outputFile = file1 + string("-new");
            {
                LZ4Writer writer(outputFile);
                //2-way merging
                Kognac_TwoWayMerger<Kognac_TextClassID,
                    Kognac_TextClassID::lessClassIDFirst>
                        merger(file1, file2);
                char supportBuffer[MAX_TERM_SIZE + 2];
                size_t sSupportBuffer = 0;
                while (!merger.isEmpty()) {
                    Kognac_TextClassID el = merger.get();
                    if (!el.eqText(supportBuffer, sSupportBuffer)) {
                        el.writeTo(&writer);
                        memcpy(supportBuffer, el.term, el.size);
                        sSupportBuffer = el.size;
                    }
                    merger.next();
                }
            }
            Utils::remove(file1);
            Utils::remove(file2);
            if (Utils::exists(outputFile))
                Utils::rename(outputFile, file1);
            sortedFiles.insert(sortedFiles.begin(), file1);
        }
    }
}

void Kognac::assignIdsToAllTerms(string inputdir, int64_t & counter,
        std::ostream & out) {
    std::vector<string> files = Utils::getFiles(inputdir);
    assert(files.size() == 1);
    LZ4Reader reader(files.front());

    int64_t classId = -1;
    int64_t prevCounter = 0;

    while (!reader.isEof()) {
        Kognac_TextClassID el;
        el.readFrom(&reader);
        if (el.classID != classId) {
            LOG(DEBUGL) << "ClassID: " << el.classID <<
                " first count " << counter;
            //Update an internal data structure
            if (classId != -1) {
                extractor.addClassesBeginEndRange(classId, prevCounter, counter);
                prevCounter = counter;
            }
            classId = el.classID;
        }
        assert(el.classID >= classId);
        out << to_string(counter) << " " << to_string(el.size) << " ";
        out.write(el.term, el.size);
        out << '\n';
        counter++;
    }
}

void Kognac::mergeAllTermsWithClassIDs(const int npartitions,
        string inputDir) {
    std::vector<std::vector<string>> filesPerPartition;
    std::vector<string> allFiles = Utils::getFiles(inputDir);
    for (int i = 0; i < npartitions; ++i) {
        std::vector<string> files;
        for (std::vector<string>::iterator itr = allFiles.begin();
                itr != allFiles.end(); ++itr) {
            string sfilename = *itr;
            sfilename = sfilename.substr(0, sfilename.rfind("."));
            if (Utils::ends_with(sfilename, string(".") +
                        to_string(i))) {
                files.push_back(*itr);
            }
        }
        filesPerPartition.push_back(files);
    }

    std::unique_ptr<std::thread[]> threads = std::unique_ptr <
        std::thread[] > (new std::thread[npartitions - 1]);
    for (int i = 1; i < npartitions; ++i) {
        threads[i - 1] =  std::thread(
                std::bind(
                    &Kognac::mergeAllTermsWithClassIDsPart,
                    this, filesPerPartition[i]));
    }
    mergeAllTermsWithClassIDsPart(filesPerPartition[0]);
    for (int i = 1; i < npartitions; ++i) {
        threads[i - 1].join();
    }
}

void Kognac::pickSmallestClassID(const int npartitions, string inputDir,
        const bool useFP) {
    std::vector<std::vector<string>> filesPerPartition;
    std::vector<string> allFiles = Utils::getFiles(inputDir);
    for (int i = 0; i < npartitions; ++i) {
        std::vector<string> files;
        for (std::vector<string>::iterator itr = allFiles.begin();
                itr != allFiles.end(); ++itr) {
            string sfilename = *itr;
            sfilename = sfilename.substr(0, sfilename.rfind("."));
            if (Utils::ends_with(sfilename, string(".") +
                        to_string(i))) {
                files.push_back(*itr);
            }
        }
        assert(files.size() == 1);
        filesPerPartition.push_back(files);
    }

    std::unique_ptr<std::thread[]> threads = std::unique_ptr <
        std::thread[] > (new std::thread[npartitions - 1]);
    for (int i = 1; i < npartitions; ++i) {
        threads[i - 1] =  std::thread(
                std::bind(
                    &Kognac::pickSmallestClassIDPart,
                    this, filesPerPartition[i][0], useFP));
    }
    pickSmallestClassIDPart(filesPerPartition[0][0], useFP);
    for (int i = 1; i < npartitions; ++i) {
        threads[i - 1].join();
    }

}

void Kognac::pickSmallestClassIDPart(string inputFile, const bool useFP) {
    //Read the file
	{
		LZ4Reader reader(inputFile);
		LZ4Writer writer(inputFile + ".min");

		if (useFP) {
			char supportBuffer[MAX_TERM_SIZE + 2];
			size_t sSupportBuffer = 0;
			int64_t minClass = INT64_MAX;
			const std::vector<int64_t> *taxonomyClasses;

			bool first = true;
			while (!reader.isEof()) {
				Kognac_TextClassID el;
				el.readFrom(&reader);

				if (first) {
					first = false;
					memcpy(supportBuffer, el.term, el.size);
					sSupportBuffer = el.size;
				}
				else if (!el.eqText(supportBuffer, sSupportBuffer)) {
					Kognac_TextClassID lastEl;
					lastEl.term = supportBuffer;
					lastEl.size = static_cast<int>(sSupportBuffer);
					lastEl.classID = minClass;
					lastEl.classID2 = 0;
					lastEl.writeTo(&writer);
					memcpy(supportBuffer, el.term, el.size);
					sSupportBuffer = el.size;
					minClass = INT64_MAX;
				}

				extractor.retrieveInstances(el.classID, &taxonomyClasses);
				if (taxonomyClasses && taxonomyClasses->at(0) < minClass) {
					minClass = taxonomyClasses->at(0);
				}
			}

			//write last element
			Kognac_TextClassID lastEl;
			lastEl.term = supportBuffer;
			lastEl.size = static_cast<int>(sSupportBuffer);
			lastEl.classID = minClass;
			lastEl.classID2 = 0;
			lastEl.writeTo(&writer);
		}
		else { //No FP support
			char supportBuffer[MAX_TERM_SIZE + 2];
			size_t sSupportBuffer = 0;
			int64_t minClass = INT64_MAX;
			const std::vector<int64_t> *taxonomyClasses;
			bool first = true;
			while (!reader.isEof()) {
				Kognac_TextClassID el;
				el.readFrom(&reader);

				if (first) {
					first = false;
					memcpy(supportBuffer, el.term, el.size);
					sSupportBuffer = el.size;
					minClass = el.classID;
				}
				else if (!el.eqText(supportBuffer, sSupportBuffer)) {
					Kognac_TextClassID lastEl;
					lastEl.term = supportBuffer;
					lastEl.size = static_cast<int>(sSupportBuffer);
					lastEl.classID = minClass;
					lastEl.classID2 = 0;
					lastEl.writeTo(&writer);
					memcpy(supportBuffer, el.term, el.size);
					sSupportBuffer = el.size;
					minClass = el.classID;
				}
				extractor.retrieveInstances(el.classID, &taxonomyClasses);
				if (taxonomyClasses && taxonomyClasses->at(0) < minClass) {
					minClass = taxonomyClasses->at(0);
				}
			}

			//write last element
			Kognac_TextClassID lastEl;
			lastEl.term = supportBuffer;
			lastEl.size = static_cast<int>(sSupportBuffer);
			lastEl.classID = minClass;
			lastEl.classID2 = 0;
			lastEl.writeTo(&writer);
		}
	}

    //Remove input file
    Utils::remove(inputFile);
}

void Kognac::mergeAllTermsWithClassIDsPart(std::vector<string> inputFiles) {
    assert(inputFiles.size() > 0);
    while (inputFiles.size() > 1) {
        string file1 = inputFiles.back();
        inputFiles.pop_back();
        string file2 = inputFiles.back();
        inputFiles.pop_back();

        string outputFile = Utils::parentDir(file1) + DIR_SEP + string("n-")
            + Utils::filename(file1);
        {
            LZ4Writer writer(outputFile);
            //2-way merging
            Kognac_TwoWayMerger<Kognac_TextClassID,
                Kognac_TextClassID::lessTextFirst> merger(
                        file1, file2);

            char supportBuffer[MAX_TERM_SIZE + 2];
            size_t sizeSupportBuffer = 0;
            int64_t classID = 0, classID2 = 0;

            while (!merger.isEmpty()) {
                Kognac_TextClassID el = merger.get();

                //Different than the previous?
                if (el.size != sizeSupportBuffer ||
                        memcmp(el.term, supportBuffer,
                            sizeSupportBuffer) != 0 ||
                        el.classID != classID || el.classID2 != classID2) {
                    classID = el.classID;
                    classID2 = el.classID2;
                    sizeSupportBuffer = el.size;
                    memcpy(supportBuffer, el.term, sizeSupportBuffer);
                    el.writeTo(&writer);
                }
                merger.next();
            }
        }
        Utils::remove(file1);
        Utils::remove(file2);
        if (Utils::exists(outputFile))
            Utils::rename(outputFile, file1);
        inputFiles.insert(inputFiles.begin(), file1);
    }


    //Re-read the file and get the frequent patterns
    if (frequentPatterns != NULL) { //If it is NULL then we disabled FP mining
        std::vector<std::pair<uint64_t, uint64_t>> classes;
        char supportBuffer[MAX_TERM_SIZE + 2];
        size_t sSupportBuffer = 0;
        LZ4Reader reader(inputFiles[0]);
        while (!reader.isEof()) {
            Kognac_TextClassID el;
            el.readFrom(&reader);

            if (!el.eqText(supportBuffer, sSupportBuffer)) {
                if (classes.size() > 0) {
                    addTransactionToFrequentPatterns(classes);
                    classes.clear();
                }
                memcpy(supportBuffer, el.term, el.size);
                sSupportBuffer = el.size;
            }
            if (el.classID != INT64_MAX) {
                //Get the frequency
                uint64_t freq = 0;
                if (classesWithFrequency.count(el.classID)) {
                    freq = classesWithFrequency.find(el.classID)->second;
                } else {
                    assert(false); //This is strange. Check out why it's happening
                }
                classes.push_back(make_pair(freq, el.classID));
            }
            if (el.classID2 != INT64_MAX) {
                uint64_t freq = 0;
                if (classesWithFrequency.count(el.classID2)) {
                    freq = classesWithFrequency.find(el.classID2)->second;
                } else {
                    assert(false); //This is strange. Check out why it's happening
                }
                classes.push_back(make_pair(freq, el.classID2));
            }
        }
        if (classes.size() > 0) {
            addTransactionToFrequentPatterns(classes);
        }
    }
}

bool Kognac_sortPairsByFirstDesc(
        const std::pair<uint64_t, uint64_t> &el1,
        const std::pair<uint64_t, uint64_t> &el2) {
    return el1.first > el2.first ||
        (el1.first == el2.first && el1.second > el2.second);
}

void Kognac::addTransactionToFrequentPatterns(
        std::vector<std::pair<uint64_t, uint64_t>> &classes) {
    //Sort the classes by frequency
    std::sort(classes.begin(), classes.end(), Kognac_sortPairsByFirstDesc);
    auto it = std::unique(classes.begin(), classes.end());
    classes.resize(std::distance(classes.begin(), it));
    std::vector<uint64_t> onlyClasses;
    for (int i = 0; i < classes.size() && i < maxPatternLength; ++i) {
        onlyClasses.push_back(classes[i].second);
    }

    //Add them to the frequent pattern data structure (with lock)
    mut.lock();
    frequentPatterns->insert(onlyClasses);
    mut.unlock();
}

void Kognac::assignIdsToMostPopularTerms(StringCollection & col,
        ByteArrayToNumberMap & map,
        int64_t & counter,
        std::ostream & out) {
    char tmpBuffer[MAX_TERM_SIZE + 2];
    map.set_empty_key(EMPTY_KEY);
    map.set_deleted_key(DELETED_KEY);
    for (std::vector<std::pair<string, uint64_t>>::iterator itr =
            mostFrequentTerms.begin(); itr != mostFrequentTerms.end();
            ++itr) {
        out << to_string(counter) << " " << to_string(itr->first.size()) << " ";
        out << itr->first;
        out << '\n';

        //Copy it in the hashmap
        memcpy(tmpBuffer + 2, itr->first.c_str(), itr->first.size());
        Utils::encode_short(tmpBuffer, static_cast<int>(itr->first.size()));
        const char* text = col.addNew(tmpBuffer,
			static_cast<int>(itr->first.size() + 2));
        map.insert(make_pair(text, counter));
        counter++;
    }
}

void Kognac::extractAllTermsWithClassIDs(const int nthreads,
        const int nReadingThreads,
        const bool useFP,
        string outputdir,
        ByteArrayToNumberMap & frequentTermsMap,
        std::map<uint64_t, uint64_t> &frequencyClasses) {
    assert(splittedInput.size() == nReadingThreads);
    int64_t maxMem = std::max((int64_t)(10 * 1024 * 1024),
            (int64_t)(Utils::getSystemMemory() * 0.7));
    maxMem = maxMem / nthreads;

    std::map<uint64_t, uint64_t> *localClasses =
        new std::map<uint64_t, uint64_t>[nthreads - 1];

    DiskLZ4Reader **readers = new DiskLZ4Reader*[nReadingThreads];
    for (int i = 0; i < nReadingThreads; ++i) {
        readers[i] = new DiskLZ4Reader(splittedInput[i], nthreads / nReadingThreads, 3);
    }

    std::thread *threads = new std::thread[nthreads - 1];
    for (int i = 1; i < nthreads; ++i) {
        string outputFile = outputdir + DIR_SEP + to_string(i);
        localClasses[i - 1] = frequencyClasses;
        if (useFP) {
            threads[i - 1] = std::thread(
                    std::bind(
                        &Kognac::extractAllTermsWithClassIDs_int,
                        this, maxMem,
                        readers[i % nReadingThreads],
                        i / nReadingThreads,
                        outputFile,
                        &frequentTermsMap,
                        localClasses + i - 1, nthreads));
        } else {
            threads[i - 1] = std::thread(
                    std::bind(
                        &Kognac::extractAllTermsWithClassIDsNOFP_int,
                        this, maxMem,
                        readers[i % nReadingThreads],
                        i / nReadingThreads,
                        outputFile,
                        &frequentTermsMap,
                        localClasses + i - 1, nthreads));

        }
    }

    string outputFile = outputdir + DIR_SEP + to_string(0);
    if (useFP) {
        extractAllTermsWithClassIDs_int(maxMem,
                readers[0],
                0,
                outputFile,
                &frequentTermsMap, &frequencyClasses,
                nthreads);
    } else {
        extractAllTermsWithClassIDsNOFP_int(maxMem,
                readers[0],
                0,
                outputFile,
                &frequentTermsMap, &frequencyClasses,
                nthreads);
    }
    for (int i = 1; i < nthreads; ++i) {
        threads[i - 1].join();
    }

    for (int i = 0; i < nReadingThreads; ++i) {
        delete readers[i];
    }
    delete[] readers;

    if (useFP) {
        //Merge all frequencies
        for (int i = 1; i < nthreads; ++i) {
            for (auto &el : localClasses[i - 1]) {
                if (!frequencyClasses.count(el.first)) {
                    //Every element should always be present
                    throw 10;
                } else {
                    frequencyClasses.find(el.first)->second += el.second;
                }
            }
        }
    }

    delete[] threads;
    delete[] localClasses;
}

void Kognac::extractAllTermsWithClassIDs_int(const int64_t maxMem,
        DiskLZ4Reader * reader,
        const int idReader,
        string outputfile,
        ByteArrayToNumberMap * frequentTermsMap,
        std::map<uint64_t, uint64_t> *frequencyClasses,
        const int nthreads) {

    char tmpS[MAX_TERM_SIZE + 2];
    char tmpP[MAX_TERM_SIZE + 2];
    char tmpO[MAX_TERM_SIZE + 2];

    //Output: I create n outputs, depending on the hash of the terms
    Kognac_TermBufferWriter writer(maxMem, nthreads, outputfile, false);

    while (!reader->isEOF(idReader)) {
        //Read the three fields
        int sizeTerm;
        if (reader->readByte(idReader) != 0) {
            LOG(ERRORL) << "Flag should always be zero!";
            throw 10;
        }
        const char *term = reader->readString(idReader, sizeTerm);
        memcpy(tmpS, term, sizeTerm);
        if (reader->readByte(idReader) != 0) {
            LOG(ERRORL) << "Flag should always be zero!";
            throw 10;
        }
        term = reader->readString(idReader, sizeTerm);
        memcpy(tmpP, term, sizeTerm);
        if (reader->readByte(idReader) != 0) {
            LOG(ERRORL) << "Flag should always be zero!";
            throw 10;
        }
        term = reader->readString(idReader, sizeTerm);
        memcpy(tmpO, term, sizeTerm);


        //I should output the three values with each an associated class ID
        if (frequentTermsMap->find(tmpS) == frequentTermsMap->end()) {
            processTerm(writer, 0, tmpS, tmpP, tmpO, frequencyClasses, true);
        }
        if (frequentTermsMap->find(tmpP) == frequentTermsMap->end()) {
            processTerm(writer, 1, tmpP, tmpO, tmpS, frequencyClasses, true);
        }
        if (frequentTermsMap->find(tmpO) == frequentTermsMap->end()) {
            processTerm(writer, 2, tmpO, tmpS, tmpP, frequencyClasses, true);
        }
    }

    writer.flush();
}

void Kognac::extractAllTermsWithClassIDsNOFP_int(const int64_t maxMem,
        DiskLZ4Reader * reader,
        int idReader,
        string outputfile,
        ByteArrayToNumberMap * frequentTermsMap,
        std::map<uint64_t, uint64_t> *frequencyClasses,
        const int nthreads) {

    char tmpS[MAX_TERM_SIZE + 2];
    char tmpP[MAX_TERM_SIZE + 2];
    char tmpO[MAX_TERM_SIZE + 2];

    //Output: I create n outputs, depending on the hash of the terms
    Kognac_TermBufferWriter writer(maxMem, nthreads, outputfile, false);

    while (!reader->isEOF(idReader)) {
        //Read the three fields
        int sizeTerm;
        if (reader->readByte(idReader) != 0) {
            LOG(ERRORL) << "Flag should always be zero!";
            throw 10;
        }
        const char *term = reader->readString(idReader, sizeTerm);
        memcpy(tmpS, term, sizeTerm);
        if (reader->readByte(idReader) != 0) {
            LOG(ERRORL) << "Flag should always be zero!";
            throw 10;
        }
        term = reader->readString(idReader, sizeTerm);
        memcpy(tmpP, term, sizeTerm);
        if (reader->readByte(idReader) != 0) {
            LOG(ERRORL) << "Flag should always be zero!";
            throw 10;
        }
        term = reader->readString(idReader, sizeTerm);
        memcpy(tmpO, term, sizeTerm);


        //I should output the three values with each an associated class ID
        if (!frequentTermsMap->count(tmpS)) {
            processTerm(writer, 0, tmpS, tmpP, tmpO, frequencyClasses, false);
        }
        if (!frequentTermsMap->count(tmpP)) {
            processTerm(writer, 1, tmpP, tmpO, tmpS, frequencyClasses, false);
        }
        if (!frequentTermsMap->count(tmpO)) {
            processTerm(writer, 2, tmpO, tmpS, tmpP, frequencyClasses, false);
        }
    }

    writer.flush();
}


void Kognac::processTerm(Kognac_TermBufferWriter & writer, const int pos,
        const char* term, const char* otherterm1,
        const char* otherterm2,
        std::map<uint64_t, uint64_t> *freqsClass,
        const bool useFP) const {
    int64_t classID = INT64_MAX;
    int64_t classID2 = INT64_MAX;
    //int64_t pred = INT64_MAX;
    int sizeTerm = Utils::decode_short(term);

    //Determine a potential classID
    if (pos == 0) {
        int64_t hashP = Hashes::murmur3_56(otherterm1 + 2,
                Utils::decode_short(otherterm1));
        if (hashP == SchemaExtractor::HASHTYPE) {
            classID = Hashes::murmur3_56(otherterm2 + 2,
                    Utils::decode_short(otherterm2));

            if (useFP) {
                assert(freqsClass->count(classID));
                freqsClass->find(classID)->second++;
                writer.insertInCache(Hashes::murmur3_56(term + 2,
                            sizeTerm), classID);
            }
        } else {
            if (extractor.hasDomain(hashP)) {
                classID = extractor.getDomain(hashP);

                if (useFP) {
                    assert(freqsClass->count(classID));
                    freqsClass->find(classID)->second++;
                    writer.insertInCache(Hashes::murmur3_56(term + 2,
                                sizeTerm), classID);
                }
            }

            if (useFP) {
                //Can I get classID2?
                int64_t hashO = Hashes::murmur3_56(otherterm2 + 2,
                        Utils::decode_short(otherterm2));
                classID2 = writer.getClassFromCache(hashO);
            }
        }
        //pred = hashP;
    } else if (pos == 2) {
        int64_t hashP = Hashes::murmur3_56(otherterm2 + 2,
                Utils::decode_short(otherterm2));
        if (extractor.hasRange(hashP)) {
            classID = extractor.getRange(hashP);

            if (useFP) {
                assert(freqsClass->count(classID));
                freqsClass->find(classID)->second++;
                writer.insertInCache(Hashes::murmur3_56(term + 2,
                            sizeTerm), classID);
            }
        }

        if (useFP) {
            //Can I get classID2?
            int64_t hashS = Hashes::murmur3_56(otherterm1 + 2,
                    Utils::decode_short(otherterm1));
            classID2 = writer.getClassFromCache(hashS);
        }
    }

    //Write the term in the file
    Kognac_TextClassID pair;
    pair.term = term + 2;
    pair.size = sizeTerm;
    pair.classID2 = classID2;

    if (useFP) {
        pair.classID = classID;
        //pair.pred = pred;
        writer.write(pair);
    } else {
        pair.classID = classID;
        writer.write(pair);
    }
}

Kognac::~Kognac() {
    for (std::vector<string>::iterator itr = splittedInput.begin();
            itr != splittedInput.end(); ++itr) {
        Utils::remove(*itr);
        Utils::remove(*itr + ".idx");
    }
}

Kognac_TermBufferWriter::Kognac_TermBufferWriter(const int64_t maxMem,
        const int npartitions,
        string outputfile, const bool onlyMinClass) :
    npartitions(npartitions),
    outputfile(outputfile),
    onlyMinClass(onlyMinClass) {
        partitionCounters = std::unique_ptr<int[]>(new int[npartitions]);
        stringBuffers =
            std::unique_ptr<std::unique_ptr<StringCollection>[]>(
                    new std::unique_ptr<StringCollection>[npartitions]);
        elementsBuffers = std::unique_ptr<std::vector<Kognac_TextClassID>[]>(
                new std::vector<Kognac_TextClassID>[npartitions]);

        for (int i = 0; i < npartitions; ++i) {
            partitionCounters[i] = 0;
            stringBuffers[i] =
                std::unique_ptr<StringCollection>(
                        new StringCollection(10 * 1024 * 1024));
        }

        //Calculate the main memory
        //memReservedForCache = maxMem / 2;
        //maxMemoryPerBuffer = (maxMem - memReservedForCache) / npartitions;
        maxMemoryPerBuffer = (maxMem) / npartitions;
        LOG(DEBUGL) << "Memory per buffer is " << maxMemoryPerBuffer;
        count = 0;
        countWritten = 0;
    }

void Kognac_TermBufferWriter::write(const Kognac_TextClassID & pair) {
    const int partition = Utils::getPartition(pair.term,
            pair.size,
            npartitions);

    //Add it in the buffer
    const char* text = stringBuffers[partition]->addNew(pair.term, static_cast<int>(pair.size));
    Kognac_TextClassID newclass;
    newclass.term = text;
    newclass.size = pair.size;
    newclass.classID = pair.classID;
    newclass.classID2 = pair.classID2;
    elementsBuffers[partition].push_back(newclass);

    //If the buffer is too big, sort and dump it to a file
    const int64_t stringSize = stringBuffers[partition]->occupiedBytes();
    const int64_t currentSize = sizeof(Kognac_TextClassID) *
        elementsBuffers[partition].size() * 2 +
        stringSize;
    if (currentSize >= maxMemoryPerBuffer) {
        dumpBuffer(partition);
    }
    count++;
}

void Kognac_TermBufferWriter::dumpBuffer(const int partition) {
    //Sort by text and class ID
    sort(elementsBuffers[partition].begin(),
            elementsBuffers[partition].end(), &Kognac_TextClassID::lessTextFirst);

    //Create a file to write
    LZ4Writer writer(outputfile + string(".") + to_string(partition) + "."
            + to_string(partitionCounters[partition]++));

    Kognac_TextClassID *prev = NULL;
    for (std::vector<Kognac_TextClassID>::iterator itr =
            elementsBuffers[partition].begin();
            itr != elementsBuffers[partition].end(); ++itr) {
        if (!prev || !(prev->eqText(*itr)) ||
                (!onlyMinClass &&
                 (!(prev->classID == itr->classID)
                  || !(prev->classID2 == itr->classID2)))) {
            itr->writeTo(&writer);
            prev = &(*itr);
            countWritten++;
        } else {
        }
    }

    elementsBuffers[partition].clear();
    stringBuffers[partition]->clear();

}

void Kognac_TermBufferWriter::flush() {
    for (int i = 0; i < npartitions; ++i) {
        if (elementsBuffers[i].size() > 0)
            dumpBuffer(i);
    }
}

Kognac_TermBufferWriter::~Kognac_TermBufferWriter() {
    flush();
    LOG(DEBUGL) << "Inserted " << count << " tuples. Written " << countWritten << " tuples.";
}

void Kognac_TermBufferWriter::insertInCache(const int64_t key,
        const int64_t hash) {
    //cacheMut.lock();

    //24: size entry in the list, 28 size entry in the map
    if (!cacheClassAssignments.count(key)) {
        //LOG(DEBUGL) << "Write  to cache " << &cacheClassAssignments;
        //
        //J: Before we were trying to store everything in the cache. Now we store only the last 100 elements
        //const int64_t estimatedConsumption = cacheClassAssignments.size() * (24 + 28);
        //if (estimatedConsumption >= memReservedForCache) {

        if (cacheClassAssignments.size() >= 100) {
            //Remove an entry
            const int64_t keyToRemove = queueElements.front();
            queueElements.pop_front();
            cacheClassAssignments.erase(cacheClassAssignments.
                    find(keyToRemove));
        }
        //Add an entry to the cache
        cacheClassAssignments.insert(make_pair(key, hash));
        queueElements.push_back(key);
    }

    //cacheMut.unlock();
    }
