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

#include <kognac/filereader.h>
#include <kognac/consts.h>
#include <kognac/logs.h>
#include <kognac/utils.h>
#include <zstr/zstr.hpp>

#include <fstream>
#include <iostream>
#include <climits>
#include <chrono>

string GZIP_EXTENSION = string(".gz");

FileReader::FileReader(char *buffer, size_t sizebuffer, bool gzipped) :
    byteArray(true), rawByteArray(buffer),
    sizeByteArray(sizebuffer), compressed(gzipped) {
    rawFile = NULL;
    if (compressed) {
        //Decompress the stream
	std::string b(buffer, buffer + sizebuffer);
	std::istringstream bytestream(b);
	zstr::istream stream(bytestream);
	std::vector<char> contents((std::istreambuf_iterator<char>(stream)), std::istreambuf_iterator<char>());
	uncompressedByteArray.swap(contents);
    }
    currentIdx = 0;
}

FileReader::FileReader(FileInfo sFile) :
    byteArray(false), compressed(!sFile.splittable) {
    //First check the extension to identify what kind of file it is.
    if (Utils::hasExtension(sFile.path) && Utils::extension(sFile.path) == GZIP_EXTENSION) {
	rawFile = new zstr::ifstream(sFile.path);
    } else {
	rawFile = new std::ifstream(sFile.path, ios_base::in | ios_base::binary);
    }

    if (sFile.splittable) {
        end = sFile.start + sFile.size;
    } else {
        end = LONG_MAX;
    }

    //If start != 0 then move to first '\n'
    if (sFile.start > 0) {
        rawFile->seekg(sFile.start);
//Seek to the first '\n'
        while (!rawFile->eof() && rawFile->get() != '\n') {
        };
    }
    countBytes = rawFile->tellg();
    tripleValid = false;

    startS = startP = startO = NULL;
    lengthS = lengthP = lengthO = 0;
}

bool FileReader::parseTriple() {
    bool ok = false;
    if (byteArray) {
        if (compressed) {
            if (currentIdx < uncompressedByteArray.size()) {
                size_t e = currentIdx + 1;
                while (e < uncompressedByteArray.size()
                        && uncompressedByteArray[e] != '\n') {
                    e++;
                }
                if (e == currentIdx + 1 || uncompressedByteArray[currentIdx] == '#') {
                    currentIdx = e + 1;
                    return parseTriple();
                }
                tripleValid = parseLine(&uncompressedByteArray[currentIdx],
                                        e - currentIdx);
                currentIdx = e + 1;
                return true;
            } else {
                tripleValid = false;
                return false;
            }
        } else {
            if (currentIdx < sizeByteArray) {
                //read a line
                size_t e = currentIdx + 1;
                while (e < sizeByteArray && rawByteArray[e] != '\n') {
                    e++;
                }
                if (e == currentIdx + 1 || rawByteArray[currentIdx] == '#') {
                    currentIdx = e + 1;
                    return parseTriple();
                }
                tripleValid = parseLine(rawByteArray + currentIdx, e - currentIdx);
                currentIdx = e + 1;
                return true;
            } else {
                tripleValid = false;
                return false;
            }
        }
    } else {
        if (compressed) {
	    ok = (bool) std::getline(*rawFile, currentLine);
        } else {
            ok = countBytes <= end && std::getline(*rawFile, currentLine);
            if (ok) {
                countBytes = rawFile->tellg();
            }
        }

        if (ok) {
            if (currentLine.size() == 0 || currentLine.at(0) == '#') {
                return parseTriple();
            }
            tripleValid = parseLine(currentLine.c_str(), (int)currentLine.size());
            return true;
        }
        tripleValid = false;
        return false;
    }
}

const char *FileReader::getCurrentS(int &length) {
    length = lengthS;
    return startS;
}

const char *FileReader::getCurrentP(int &length) {
    length = lengthP;
    return startP;
}

const char *FileReader::getCurrentO(int &length) {
    length = lengthO;
    return startO;
}

bool FileReader::isTripleValid() {
    return tripleValid;
}

void FileReader::checkRange(const char *pointer, const char* start,
                            const char *end) {
    if (pointer == NULL || pointer <= (start + 1) || pointer > end) {
        throw ex;
    }
}

bool FileReader::parseLine(const char *line, const int sizeLine) {

    const char* endLine = line + sizeLine;
    const char *endS;
    const char *endO = NULL;
    try {
        // Parse subject
        startS = line;
        if (line[0] == '<') {
            endS = strchr(line, '>') + 1;
        } else { // Is a bnode
            endS = strchr(line, ' ');
        }
        checkRange(endS, startS, endLine);
        lengthS = (int)(endS - startS);

        //Parse predicate. Skip one space
        startP = line + lengthS + 1;
        const char *endP = strchr(startP, '>');
        checkRange(endP, startP, endLine);
        lengthP = (int)(endP + 1 - startP);

        // Parse object
        startO = startP + lengthP + 1;
        if (startO[0] == '<') { // URI
            endO = strchr(startO, '>') + 1;
        } else if (startO[0] == '"') { // Literal
            //Copy until the end of the string and remove character
            endO = endLine;
            while (*endO != '.' && endO >  startO) {
                endO--;
            }
            endO--;
        } else { // Bnode
            endO = strchr(startO, ' ');
        }
        checkRange(endO, startO, endLine);
        lengthO = (int)(endO - startO);

        if (lengthS > 0 && lengthS < (MAX_TERM_SIZE - 1) && lengthP > 0
                && lengthP < (MAX_TERM_SIZE - 1) && lengthO > 0
                && lengthO < (MAX_TERM_SIZE - 1)) {
            return true;
        } else {
            //LOG(ERROR) << "The triple was not parsed correctly: " << lengthS << " " << lengthP << " " << lengthO;
            return false;
        }

    } catch (std::exception &e) {
        LOG(ERROR) << "Failed parsing line: " + string(line, sizeLine);
	abort();
    }
    return false;
}
