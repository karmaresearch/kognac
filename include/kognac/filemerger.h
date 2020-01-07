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

#ifndef FILEMERGER_H_
#define FILEMERGER_H_

#include <kognac/lz4io.h>
#include <kognac/triple.h>
#include <kognac/utils.h>

#include <string>
#include <queue>
#include <vector>

using namespace std;

template<class K>
struct QueueEl {
    K key;
    int fileIdx;
};

template<class K>
struct QueueElCmp {
    bool operator()(const QueueEl<K> &t1, const QueueEl<K> &t2) const {
        return t1.key.greater(t2.key);
    }
};

template<class K>
class FileMerger {
    protected:
        priority_queue<QueueEl<K>, vector<QueueEl<K> >, QueueElCmp<K> > queue;
        LZ4Reader **files;
        size_t nfiles;
        int nextFileToRead;
        int64_t elementsRead;
        std::vector<int> extensions; //mark the current extension
        std::vector<string> input;

        FileMerger() : deletePreviousExt(false) {}

    private:
        const bool deletePreviousExt;

        //Changes LZ4Reader **files
        int loadFileWithExtension(string prefix, int part, int ext) {
            string filename = prefix + "." + to_string(ext);
            if (Utils::exists(filename)) {
                files[part] = new LZ4Reader(filename);
                return ext;
            } else {
                return -1;
            }
        }

    public:
        FileMerger(vector<string> fn,
                bool considerExtensions = false,
                bool deletePreviousExt = false) :
            deletePreviousExt(deletePreviousExt) {
                //Open the files
                files = new LZ4Reader*[fn.size()];
                nfiles = fn.size();
                elementsRead = 0;
                this->input = fn;

                for (int i = 0; i < fn.size(); ++i) {
                    if (considerExtensions) {
                        const int loadedExt = loadFileWithExtension(fn[i], i, 0);
                        extensions.push_back(loadedExt);
                    } else {
                        files[i] = new LZ4Reader(fn[i]);
                        extensions.push_back(-1);
                    }

                    //Read the first element and put it in the queue
                    if (!files[i]->isEof()) {
                        QueueEl<K> el;
                        el.key.readFrom(files[i]);
                        el.fileIdx = i;
                        queue.push(el);
                        elementsRead++;
                    }
                }
                nextFileToRead = -1;
            }

        bool isEmpty() {
            return queue.empty() && nextFileToRead == -1;
        }

        K get() {
            if (nextFileToRead != -1) {
                QueueEl<K> el;
                el.key.readFrom(files[nextFileToRead]);
                el.fileIdx = nextFileToRead;
                queue.push(el);
                elementsRead++;
            }

            //Get the first triple
            QueueEl<K> el = queue.top();
            queue.pop();
            K out = el.key;

            //Replace the current element with a new one from the same file
            if (!files[el.fileIdx]->isEof()) {
                nextFileToRead = el.fileIdx;
            } else {
                if (extensions[el.fileIdx] != -1) {
                    while (true) {
                        const int currentExt = extensions[el.fileIdx];
                        delete files[el.fileIdx];
                        if (deletePreviousExt) {
                            string filename = input[el.fileIdx] + "." + to_string(currentExt);
                            Utils::remove(filename);
                        }
                        const int nextExt = loadFileWithExtension(input[el.fileIdx], el.fileIdx, currentExt + 1);
                        if (nextExt == -1) {
                            nextFileToRead = -1;
                            extensions[el.fileIdx] = -1;
                            files[el.fileIdx] = NULL;
                            break;
                        } else {
                            nextFileToRead = el.fileIdx;
                            extensions[el.fileIdx] = nextExt;
                            if (!files[el.fileIdx]->isEof()) {
                                break;
                            }
                        }
                    }
                } else {
                    nextFileToRead = -1;
                }
            }

            return out;
        }

        virtual ~FileMerger() {
            for (int i = 0; i < nfiles; ++i) {
                if (files[i]) {
                    delete files[i];
                    if (deletePreviousExt) {
                        string filename = input[i] + "." + to_string(extensions[i]);
                        Utils::remove(filename);
                    }
                }
            }
            if (files != NULL)
                delete[] files;
        }
};

template<int N, class K>
class FastFileMerger {
    protected:
        //priority_queue<QueueEl<K>, vector<QueueEl<K> >, QueueElCmp<K> > queue;
        LZ4Reader *files[N];
        K elements[N];
        int elementsInQueue;
        int nextFileToRead;
        std::vector<int> extensions; //mark the current extension
        std::vector<string> input;

        FastFileMerger() : deletePreviousExt(false) {}

    private:
        const bool deletePreviousExt;

        //Changes LZ4Reader **files
        int loadFileWithExtension(string prefix, int part, int ext) {
            string filename = prefix + "." + to_string(ext);
            if (Utils::exists(filename)) {
                files[part] = new LZ4Reader(filename);
                return ext;
            } else {
                files[part] = NULL;
                return -1;
            }
        }

    public:
        FastFileMerger(vector<string> fn,
                bool considerExtensions = false,
                bool deletePreviousExt = false) :
            deletePreviousExt(deletePreviousExt) {
                //Open the files
                this->input = fn;
                this->elementsInQueue = 0;

                for (int i = 0; i < fn.size(); ++i) {
                    if (considerExtensions) {
                        const int loadedExt = loadFileWithExtension(fn[i], i, 0);
                        extensions.push_back(loadedExt);
                    } else {
                        files[i] = new LZ4Reader(fn[i]);
                        extensions.push_back(-1);
                    }

                    //Read the first element and put it in the queue
                    if (!files[i]->isEof()) {
                        elements[i].readFrom(files[i]);
                        elementsInQueue++;
                    } else {
                        elements[i] = K::getMaxEl();
                    }
                }
                nextFileToRead = -1;
            }

        bool isEmpty() {
            return elementsInQueue == 0 && nextFileToRead == -1;
        }

        K get() {
            if (nextFileToRead != -1) {
                elements[nextFileToRead].readFrom(files[nextFileToRead]);
                elementsInQueue++;
            }

            //Get the first triple
            int idxElToPick = 0;
            for(int i = 1; i < N; ++i) {
                if (elements[idxElToPick].greater(elements[i])) {
                    idxElToPick = i;
                }
            }
            K out = elements[idxElToPick];
            elements[idxElToPick] = K::getMaxEl();
            elementsInQueue--;

            //Replace the current element with a new one from the same file
            if (!files[idxElToPick]->isEof()) {
                nextFileToRead = idxElToPick;
            } else {
                if (extensions[idxElToPick] != -1) {
                    while (true) {
                        const int currentExt = extensions[idxElToPick];
                        delete files[idxElToPick];
                        files[idxElToPick] = NULL;
                        if (deletePreviousExt) {
                            string filename = input[idxElToPick] + "." + to_string(currentExt);
                            Utils::remove(filename);
                        }
                        const int nextExt = loadFileWithExtension(input[idxElToPick], idxElToPick, currentExt + 1);
                        if (nextExt == -1) {
                            nextFileToRead = -1;
                            extensions[idxElToPick] = -1;
                            files[idxElToPick] = NULL;
                            break;
                        } else {
                            nextFileToRead = idxElToPick;
                            extensions[idxElToPick] = nextExt;
                            if (!files[idxElToPick]->isEof()) {
                                break;
                            }
                        }
                    }
                } else {
                    nextFileToRead = -1;
                }
            }
            return out;
        }

        virtual ~FastFileMerger() {
            for (int i = 0; i < N; ++i) {
                if (files[i]) {
                    delete files[i];
                    if (deletePreviousExt) {
                        string filename = input[i] + "." + to_string(extensions[i]);
                        Utils::remove(filename);
                    }
                }
            }
        }
};



template<class K>
class NoLZ4FileMerger {
    protected:
        priority_queue<QueueEl<K>, vector<QueueEl<K> >, QueueElCmp<K> > queue;
        ifstream **files;

        size_t nfiles;
        int nextFileToRead;
        int64_t elementsRead;
        std::vector<int> extensions; //mark the current extension
        std::vector<string> input;

        NoLZ4FileMerger() : deletePreviousExt(false) {}

    private:
        const bool deletePreviousExt;

        //Changes LZ4Reader **files
        int loadFileWithExtension(string prefix, int part, int ext) {
            string filename = prefix + "." + to_string(ext);
            if (Utils::exists(filename)) {
                files[part] = new ifstream(filename, std::ifstream::binary);
                return ext;
            } else {
                return -1;
            }
        }

    public:
        NoLZ4FileMerger(vector<string> fn,
                bool considerExtensions = false,
                bool deletePreviousExt = false) :
            deletePreviousExt(deletePreviousExt) {
                //Open the files
                files = new ifstream*[fn.size()];
                nfiles = fn.size();
                elementsRead = 0;
                this->input = fn;

                for (int i = 0; i < fn.size(); ++i) {
                    files[i] = NULL;
                    if (considerExtensions) {
                        const int loadedExt = loadFileWithExtension(fn[i], i, 0);
                        extensions.push_back(loadedExt);
                    } else {
                        files[i] = new ifstream(fn[i], std::ifstream::binary);
                        extensions.push_back(-1);
                    }

                    //Read the first element and put it in the queue
                    if (!files[i]->eof()) {
                        QueueEl<K> el;
                        el.key.readFrom(files[i]);
                        el.fileIdx = i;
                        queue.push(el);
                        elementsRead++;
                    }
                }
                nextFileToRead = -1;
            }

        bool isEmpty() {
            return queue.empty() && nextFileToRead == -1;
        }

        K get() {
            if (nextFileToRead != -1) {
                QueueEl<K> el;
                el.key.readFrom(files[nextFileToRead]);
                el.fileIdx = nextFileToRead;
                queue.push(el);
                elementsRead++;
            }

            //Get the first triple
            QueueEl<K> el = queue.top();
            queue.pop();
            K out = el.key;

            //Replace the current element with a new one from the same file
            if (!files[el.fileIdx]->eof()) {
                nextFileToRead = el.fileIdx;
            } else {
                if (extensions[el.fileIdx] != -1) {
                    while (true) {
                        const int currentExt = extensions[el.fileIdx];
                        files[el.fileIdx]->close();
                        delete files[el.fileIdx];
                        if (deletePreviousExt) {
                            string filename = input[el.fileIdx] + "." + to_string(currentExt);
                            Utils::remove(filename);
                        }
                        const int nextExt = loadFileWithExtension(input[el.fileIdx], el.fileIdx, currentExt + 1);
                        if (nextExt == -1) {
                            nextFileToRead = -1;
                            extensions[el.fileIdx] = -1;
                            files[el.fileIdx] = NULL;
                            break;
                        } else {
                            nextFileToRead = el.fileIdx;
                            extensions[el.fileIdx] = nextExt;
                            if (!files[el.fileIdx]->eof()) {
                                break;
                            }
                        }
                    }
                } else {
                    nextFileToRead = -1;
                }
            }

            return out;
        }

        virtual ~NoLZ4FileMerger() {
            for (int i = 0; i < nfiles; ++i) {
                if (files[i]) {
                    files[i]->close();
                    delete files[i];
                    if (deletePreviousExt) {
                        string filename = input[i] + "." + to_string(extensions[i]);
                        Utils::remove(filename);
                    }
                }
            }
            if (files != NULL)
                delete[] files;
        }
};



#endif /* FILEMERGER_H_ */
