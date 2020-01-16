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

#ifndef FILEREADER_H_
#define FILEREADER_H_

#include <kognac/consts.h>

#include <fstream>
#include <iostream>
#include <string>
#include <exception>
#include <vector>
#include <zlib.h>

using namespace std;

class ParseException: public exception {
    public:
        virtual const char* what() const throw () {
            return "Line is not parsed correctly";
        }
};

typedef struct FileInfo {
    uint64_t size;
    uint64_t start;
    bool splittable;
    string path;
} FileInfo;

typedef struct Membuf : std::streambuf
{
    Membuf() {
    }

    Membuf(char* begin, char* end) {
        this->setg(begin, begin, end);
    }
} Membuf;

class FileReader {
    private:
        //Params used if we read from a byte array
        const bool byteArray;
	std::vector<char> uncompressedByteArray;
        char *rawByteArray;
        size_t sizeByteArray;
        size_t currentIdx;

        const bool compressed;
        Membuf sbuf;
        istream *rawFile;
        string currentLine;
        bool tripleValid;
        int64_t end;
        int64_t countBytes;

        const char *startS;
        int lengthS;
        const char* startP;
        int lengthP;
        const char *startO;
        int lengthO;

        ParseException ex;
        bool parseLine(const char *input, const int sizeInput);

        char nextChar(const char *start, const char *end);

        const char *readUnicodeEscape(const char *start, const char *end);

        const char *readIRI(const char *start, const char *end);

        const char *readResource(const char *start, const char *end);

        const char *skipSpaces(const char *start, const char *end);

        const char *readLiteral(const char *start, const char *end);

    public:
		KLIBEXP FileReader(FileInfo file);

		KLIBEXP FileReader(char *buffer, size_t sizebuffer, bool gzipped);

		KLIBEXP bool parseTriple();

		KLIBEXP bool isTripleValid();

		KLIBEXP const char *getCurrentS(int &length);

		KLIBEXP const char *getCurrentP(int &length);

		KLIBEXP const char *getCurrentO(int &length);

        ~FileReader() {
            if (rawFile != NULL) {
                delete rawFile;
            }
        }
};

#endif /* FILEREADER_H_ */
