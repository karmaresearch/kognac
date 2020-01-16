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

#ifndef LZ4IO_H_
#define LZ4IO_H_

#include <kognac/utils.h>
#include <kognac/consts.h>
#include <kognac/logs.h>
#include <lz4.h>
#include <iostream>
#include <fstream>
#include <string>
#include <cstring>

#define SIZE_SEG 512*1024
#define SIZE_COMPRESSED_SEG (512+64)*1024

using namespace std;

class LZ4Writer {
    private:
        string path;
        std::ofstream os;
        char *compressedBuffer = new char[SIZE_COMPRESSED_SEG];
        char *uncompressedBuffer = new char[SIZE_SEG];
        int uncompressedBufferLen;

        void compressAndWriteBuffer();
    public:

        LZ4Writer(string file) :
            os(file, std::ofstream::binary) {
                this->path = file;
                if (!os.good()) {
                    LOG(ERRORL) << "Failed to open the file " << file;
                }

                uncompressedBufferLen = 0;
                memset(compressedBuffer, 0, sizeof(char) * SIZE_COMPRESSED_SEG);
#if defined(WIN32)
                strcpy_s(compressedBuffer, strlen("LZOBLOCK") + 1, "LZOBLOCK");
#else
                strncpy(compressedBuffer, "LZOBLOCK", strlen("LZOBLOCK") + 1);
#endif
            }

        KLIBEXP void writeByte(char i);

        KLIBEXP void writeLong(int64_t n);

        KLIBEXP void writeShort(short n);

        KLIBEXP void writeVLong(uint64_t n);

        //  void writeString(const char *el);

        KLIBEXP void writeRawArray(const char *bytes, int length);

        KLIBEXP void writeString(const char *rawStr, int length);

        KLIBEXP ~LZ4Writer();
};

class LZ4Reader {

    private:
        char supportBuffer[MAX_TERM_SIZE + 2];
        char *compressedBuffer = new char[SIZE_COMPRESSED_SEG];
        char *uncompressedBuffer = new char[SIZE_SEG];
        int uncompressedBufferLen;
        int currentOffset;

        std::ifstream is;
        string file;

        int uncompressBuffer() {
            //The total header size is 21 bytes
            char header[21];
            is.read(header, 21);
            if (is.eof()) {
                currentOffset = 0;
                return 0;
            }

            if (!is.good() || is.gcount() != 21) {
                LOG(ERRORL) << "Problems reading from the file. Only " << (int64_t)is.gcount() << " out of 21 were being read";
            }

            int token = header[8] & 0xFF;
            int compressionMethod = token & 0xF0;

            //First 8 bytes is a fixed string (LZ4Block). Then there is one token byte.
            int compressedLen = Utils::decode_intLE(header, 9);
            int uncompressedLen = Utils::decode_intLE(header, 13);
            switch (compressionMethod) {
                case 16:
                    is.read(uncompressedBuffer, uncompressedLen);
                    if (!is.good()) {
                        LOG(ERRORL) << "Problems reading from the file. Only " << (int64_t)is.gcount() << " out of " << uncompressedLen << " were being read";
                    }
                    break;
                case 32:
                    is.read(compressedBuffer, compressedLen);
                    if (!is.good()) {
                        LOG(ERRORL) << "Problems reading from the file. Only " << (int64_t)is.gcount() << " out of " << compressedLen << " were being read";
                    }

                    if (!LZ4_decompress_safe(compressedBuffer, uncompressedBuffer,
                                compressedLen, uncompressedLen)) {
                        LOG(ERRORL) << "Error in the decompression.";
                    }
                    break;
                default:
                    LOG(ERRORL) << "Unrecognized block format. This should not happen. File " << file << " is broken.";
                    exit(1);
            }
            currentOffset = 0;
            return uncompressedLen;
        }

    public:
        LZ4Reader(string file) :
            is(file, std::ifstream::binary) {

                if (!is.good()) {
                    LOG(ERRORL) << "Failed to open the file " << file;
                }

                uncompressedBufferLen = 0;
                currentOffset = 0;
                this->file = file;
            }

        bool isEof() {
            if (currentOffset == uncompressedBufferLen) {
                if (is.good()) {
                    uncompressedBufferLen = uncompressBuffer();
                    return uncompressedBufferLen == 0;
                } else {
                    return true;
                }
            }
            return false;
        }

        KLIBEXP int64_t parseLong();

        KLIBEXP int64_t parseVLong();

        KLIBEXP int parseInt();

        KLIBEXP char parseByte();

        void parseRawArray(char *buffer, const int size) {
            if (currentOffset + size <= uncompressedBufferLen) {
                memcpy(buffer, uncompressedBuffer + currentOffset, size);
                currentOffset += size;
            } else {
                int remSize = uncompressedBufferLen - currentOffset;
                memcpy(buffer, uncompressedBuffer + currentOffset, remSize);
                currentOffset += remSize;
                isEof(); //Load the next buffer
                memcpy(buffer + remSize, uncompressedBuffer, size - remSize);
                currentOffset += size - remSize;
            }
        }

        const char *parseString(int &size) {
            size = static_cast<int>(parseVLong());

            if (currentOffset + size <= uncompressedBufferLen) {
                memcpy(supportBuffer, uncompressedBuffer + currentOffset, size);
                currentOffset += size;
            } else {
                int remSize = uncompressedBufferLen - currentOffset;
                memcpy(supportBuffer, uncompressedBuffer + currentOffset, remSize);
                currentOffset += remSize;
                isEof(); //Load the next buffer
                memcpy(supportBuffer + remSize, uncompressedBuffer, size - remSize);
                currentOffset += size - remSize;
            }
            supportBuffer[size] = '\0';

            return supportBuffer;
            //
            //
            //      for (size = 0; !isEof() && uncompressedBuffer[currentOffset] != '\0';
            //              currentOffset++) {
            //          supportBuffer[size++] = uncompressedBuffer[currentOffset];
            //      }
            //      supportBuffer[size] = '\0';
            //      currentOffset++;
            //      return supportBuffer;
        }

        ~LZ4Reader() {
            is.close();
	    delete[] uncompressedBuffer;
	    delete[] compressedBuffer;
        }
};

#endif /* LZ4IO_H_ */
