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

#include <ctype.h>

#include <fstream>
#include <iostream>
#include <climits>
#include <chrono>

string GZIP_EXTENSION = string(".gz");

#define THRESHOLD_SIZE	(1024*1024)

FileReader::FileReader(char *buffer, size_t sizebuffer, bool gzipped) :
    byteArray(! gzipped || sizebuffer < THRESHOLD_SIZE), rawByteArray(buffer),
    sizeByteArray(sizebuffer), compressed(gzipped) {
        if (compressed) {
            //Decompress the stream
            // std::string b(buffer, buffer + sizebuffer);
            // std::istringstream bytestream(b);
            // Avoid copy...
	    Membuf buf(buffer, buffer + sizebuffer);
	    if (byteArray) {
		zstr::istream stream(&buf);
		std::vector<char> contents((std::istreambuf_iterator<char>(stream)), std::istreambuf_iterator<char>());
		uncompressedByteArray.swap(contents);
		rawFile = NULL;
	    } else {
		this->sbuf = buf;
		rawFile = new zstr::istream(&this->sbuf);
	    }
        } else {
	    rawFile = NULL;
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
            end = INT64_MAX;
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
                        static_cast<int>(e - currentIdx));
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
                tripleValid = parseLine(rawByteArray + currentIdx, static_cast<int>(e - currentIdx));
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

char FileReader::nextChar(const char *start, const char *end) {
    if (start >= end) {
        throw ex;
    }
    return start[0];
}

const char *FileReader::readIRI(const char *start, const char *end) {
    assert(start < end && start[0] == '<');
    start++;
    char c = nextChar(start++, end);
    while (c != '>') {
        // Removed ` from the string below, since it actually is used in Claros
        if ((c & 0377) <= 0x20 /* || strchr("<\"{}|^", c) != NULL Commented out; slow. */) {
            LOG(ERRORL) << "Illegal character in IRI";
            throw ex;
        }
        switch(c) {
        case '<':
        case '"':
        case '{':
        case '}':
        case '|':
        case '^':
            LOG(ERRORL) << "Illegal character in IRI";
            throw ex;
        case '\\':
            if (start[0] == 'u' || start[0] == 'U') {
                start = readUnicodeEscape(start-1, end);
            }
            // Otherwise allow \, it actually is used in Claros.
            break;
        }
        c = nextChar(start++, end);
    }
    return start;
}

const char *FileReader::readUnicodeEscape(const char *start, const char *end) {
    assert(start < end && *start == '\\');
    char c = nextChar(start+1, end);
    start += 2;
    int count;
    if (c == 'u') {
        count = 4;
    } else if (c == 'U') {
        count = 8;
    } else {
        LOG(ERRORL) << "Illegal escape sequence";
        throw ex;
    }
    for (int i = 0; i < count; i++) {
        c = nextChar(start++, end);
        if (! isxdigit(c)) {
            LOG(ERRORL) << "Illegal unicode escape";
            throw ex;
        }
    }
    return start;
}

const char *FileReader::skipSpaces(const char *start, const char *end) {
    while (start < end && isspace(start[0])) {
        start++;
    }
    return start;
}

const char *FileReader::readResource(const char *start, const char *end) {
    char c = nextChar(start, end);
    if (c == '_') {
        start++;
        c = nextChar(start++, end);
        if (c == ':') {
            c = nextChar(start, end);
            while (c == '*'	// Actually used in btc2012, but not in the N-Triples standard
		    || c == '-' || c == '_' || c == ':' || isalnum(c)) {
                start++;
                if (start >= end) {
                    return start;
                }
                c = nextChar(start, end);
            }
            return start;
        } else {
            LOG(ERRORL) << "Illegal blank node";
            throw ex;
        }
    } else if (c == '<') {
        return readIRI(start, end);
    } else {
        LOG(ERRORL) << "blank node or URI expected";
        throw ex;
    }
}

const char *FileReader::readLiteral(const char *start, const char *end) {
    assert(start < end && *start == '"');
    start++;
    char c = nextChar(start++, end);
    while (c != '"') {
        if (c == '\\') {
            c = nextChar(start, end);
            if (strchr("tbnrf\"'\\", c) != NULL) {
                start++;
            } else if (c == 'u' || c == 'U') {
                start = readUnicodeEscape(start-1, end);
            } else {
                LOG(ERRORL) << "Illegal escape in string";
                throw ex;
            }
        }
        c = nextChar(start++, end);
    }
    if (start < end) {
        if (*start == '@') {
            // [a-zA-Z]+ ('-' [a-zA-Z0-9]+)*
            start++;
            c = nextChar(start++, end);
            if (! isalpha(c)) {
                LOG(ERRORL) << "illegal character in language specification";
                throw ex;
            }
            while (isalpha(c)) {
                if (start == end) {
                    return start;
                }
                c = nextChar(start++, end);
            }
            while (c == '-') {
                c = nextChar(start++, end);
                if (! isalnum(c)) {
                    LOG(ERRORL) << "illegal character in language specification";
                    throw ex;
                }
                while (isalnum(c)) {
                    if (start == end) {
                        return start;
                    }
                    c = nextChar(start++, end);
                }
            }
        } else if (*start == '^') {
            start++;
            c = nextChar(start++, end);
            if (c != '^') {
                LOG(ERRORL) << "Expected ^ in type";
                throw ex;
            }
            c = nextChar(start, end);
            if (c != '<') {
                LOG(ERRORL) << "Expected IRI in type";
                throw ex;
            }
            start = readIRI(start, end);
        }
    }
    return start;
}

bool FileReader::parseLine(const char *line, const int sizeLine) {

    const char* endLine = line + sizeLine;
    try {
        // Parse subject
        startS = skipSpaces(line, endLine);
        const char *endS = readResource(startS, endLine);
        lengthS = (int)(endS - startS);
        LOG(TRACEL) << "S = " << std::string(startS, lengthS) << ".";

        //Parse predicate
        startP = skipSpaces(endS, endLine);
	if (*startP != '<') {
	    LOG(ERRORL) << "Expected start of IRI";
	    throw ex;
	}
        const char *endP = readIRI(startP, endLine);
        lengthP = (int)(endP - startP);
        LOG(TRACEL) << "P = " << std::string(startP, lengthP) << ".";

        // Parse object
        startO = skipSpaces(endP, endLine);
        const char *endO;
        if (startO[0] == '"') { // Literal
            endO = readLiteral(startO, endLine);
        } else {
            endO = readResource(startO, endLine);
        }
        lengthO = (int)(endO - startO);
        LOG(TRACEL) << "O = " << std::string(startO, lengthO) << ".";

        if (endO < endLine) {
            const char *p = skipSpaces(endO, endLine);
            if (p < endLine) {
                if (*p != '.') {
                    LOG(ERRORL) << "'.' expected at end of triple";
                    throw ex;
                }
                p = skipSpaces(p+1, endLine);
                if (p != endLine) {
                    LOG(ERRORL) << "garbage at end of triple";
                    throw ex;
                }
            }
        }

        if (lengthS > 0 && lengthS < (MAX_TERM_SIZE - 1) && lengthP > 0
                && lengthP < (MAX_TERM_SIZE - 1) && lengthO > 0
                && lengthO < (MAX_TERM_SIZE - 1)) {
            return true;
        } else {
            //LOG(ERRORL) << "The triple was not parsed correctly: " << lengthS << " " << lengthP << " " << lengthO;
            return false;
        }

    } catch (std::exception e) {
        LOG(ERRORL) << "Failed parsing line: " + string(line, sizeLine);
        //abort();
    }
    return false;
}
