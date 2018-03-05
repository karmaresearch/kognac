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

#ifndef UTILS_H_
#define UTILS_H_

#include <kognac/consts.h>
#include <kognac/triple.h>
#include <kognac/hashfunctions.h>

#include <vector>
#include <string>
#include <iostream>
#include <stdint.h>

using namespace std;

class Utils {
    private:
    public:
        //String utils
        static bool starts_with(const string s, const string prefix);
        static bool ends_with(const string s, const string suffix);
        static bool contains(const string s, const string substr);
        //End string utils

        //File utils
        static string getFullPathExec();
        static bool hasExtension(const string &file);
        static string extension(const string &file);
        static string removeExtension(string file);
        static string removeLastExtension(string file);
        static bool isDirectory(string dirname);
        static bool isFile(string dirname);
        static vector<string> getFilesWithPrefix(string dir, string prefix);
        static vector<string> getFilesWithSuffix(string dir, string suffix);
        static vector<string> getFiles(string dir, bool ignoreExtension = false);
        static vector<string> getSubdirs(string dir);
        static uint64_t getNBytes(std::string input);
        static bool isCompressed(std::string input);
        static bool exists(std::string file);
        static uint64_t fileSize(string file);
        static void create_directories(string newdir);
        static void remove(string file);
        static void remove_all(string path);
        static void rename(string oldfile, string newfile);
        static string parentDir(string file);
        static string filename(string path);
        static bool isEmpty(string dir);
        static void resizeFile(string file, uint64_t newsize);
        static void linkdir(string source, string dest);
        static void rmlink(string link);
        //End file utils

        static int numberOfLeadingZeros(uint32_t number) {
            if (number == 0)
                return 32;
            unsigned int n = 1;
            if (number >> 16 == 0) {
                n += 16;
                number <<= 16;
            }
            if (number >> 24 == 0) {
                n += 8;
                number <<= 8;
            }
            if (number >> 28 == 0) {
                n += 4;
                number <<= 4;
            }
            if (number >> 30 == 0) {
                n += 2;
                number <<= 2;
            }
            n -= number >> 31;
            return n;
        }

        static int numberOfLeadingZeros(uint64_t i) {
            if (i == 0)
                return 64;
            int n = 1;
            unsigned int x = (int) (i >> 32);
            if (x == 0) {
                n += 32;
                x = (int) i;
            }
            if (x >> 16 == 0) {
                n += 16;
                x <<= 16;
            }
            if (x >> 24 == 0) {
                n += 8;
                x <<= 8;
            }
            if (x >> 28 == 0) {
                n += 4;
                x <<= 4;
            }
            if (x >> 30 == 0) {
                n += 2;
                x <<= 2;
            }
            n -= x >> 31;
            return n;
        }

        static short decode_short(const char* buffer, int offset);

        static short decode_short(const char* buffer) {
            return (short) (((buffer[0] & 0xFF) << 8) + (buffer[1] & 0xFF));
        }

        static void encode_short(char* buffer, int offset, int n);
        static void encode_short(char* buffer, int n);

        static int decode_int(char* buffer, int offset);
        static int decode_int(const char* buffer);
        static void encode_int(char* buffer, int offset, int n);
        static void encode_int(char* buffer, int n);
        static int decode_intLE(char* buffer, int offset);
        static void encode_intLE(char* buffer, int offset, int n);

        static int64_t decode_long(char* buffer, int offset);
        static int64_t decode_long(const char* buffer);
        static int64_t decode_longFixedBytes(const char* buffer, const uint8_t nbytes);

        static void encode_long(char* buffer, int offset, int64_t n);
        static void encode_long(char* buffer, int64_t n);
        static void encode_longNBytes(char* buffer, const uint8_t nbytes,
                const uint64_t n);

        static int64_t decode_longWithHeader(char* buffer);
        static void encode_longWithHeader0(char* buffer, int64_t n);
        static void encode_longWithHeader1(char* buffer, int64_t n);

        static int64_t decode_vlong(char* buffer, int *offset);
        static int encode_vlong(char* buffer, int offset, int64_t n);
        static uint16_t encode_vlong(char* buffer, int64_t n);
        static int numBytes(int64_t number);

        static int numBytesFixedLength(int64_t number);

        static int decode_vint2(char* buffer, int *offset);
        static int encode_vint2(char* buffer, int offset, int n);

        static int64_t decode_vlong2(const char* buffer, int *offset);

        static int encode_vlong2_fast(uint8_t *out, uint64_t x);
        static uint64_t decode_vlong2_fast(uint8_t *out);

        static void encode_vlong2_fixedLen(char* buffer, int64_t n, const uint8_t len);
        static int encode_vlong2(char* buffer, int offset, int64_t n);
        static uint16_t encode_vlong2(char* buffer, int64_t n);
        static int numBytes2(int64_t number);

        static int64_t decode_vlongWithHeader0(char* buffer, const int end, int *pos);
        static int64_t decode_vlongWithHeader1(char* buffer, const int end, int *pos);
        static int encode_vlongWithHeader0(char* buffer, int64_t n);
        static int encode_vlongWithHeader1(char* buffer, int64_t n);

        static int compare(const char* string1, int s1, int e1, const char* string2,
                int s2, int e2);

        static int compare(const char* o1, const int l1, const char* o2,
                const int l2) {
            for (int i = 0; i < l1 && i < l2; i++) {
                if (o1[i] != o2[i]) {
                    return (o1[i] & 0xff) - (o2[i] & 0xff);
                }
            }
            return l1 - l2;
        }

        static int prefixEquals(char* string1, int len, char* string2);

        static int prefixEquals(char* o1, int len1, char* o2, int len2);

        static int commonPrefix(tTerm *o1, int s1, int e1, tTerm *o2, int s2,
                int e2);

        static double get_max_mem();

        static uint64_t getSystemMemory();

        static uint64_t getUsedMemory();

        static uint64_t getIOReadBytes();

        static uint64_t getIOReadChars();

        static int getNumberPhysicalCores();

        static int64_t quickSelect(int64_t *vector, int size, int k);

        static uint64_t getCPUCounter();

        static int getPartition(const char *key, const int size,
                const int partitions) {
            return abs(Hashes::dbj2s(key, size) % partitions);
        }
};
#endif /* UTILS_H_ */
