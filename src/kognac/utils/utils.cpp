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

#include <kognac/utils.h>

/**** MEMORY STATISTICS ****/
#if defined(_WIN32)
#include <io.h>
#include <fcntl.h>
#include <windows.h>
#include <psapi.h>
#include <tchar.h>
#include <direct.h>
#elif defined(__unix__) || defined(__unix) || defined(unix) || (defined(__APPLE__) && defined(__MACH__))
#include <unistd.h>
#include <sys/resource.h>
#include <dirent.h>

#if defined(__APPLE__) && defined(__MACH__)
#include <mach/mach.h>
#include <sys/types.h>
#include <sys/sysctl.h>
#include <dirent.h>

#elif (defined(_AIX) || defined(__TOS__AIX__)) || (defined(__sun__) || defined(__sun) || defined(sun) && (defined(__SVR4) || defined(__svr4__)))
#include <fcntl.h>
#include <procfs.h>

#elif defined(__linux__) || defined(__linux) || defined(linux) || defined(__gnu_linux__)
#include <stdio.h>
#endif
#else
#error "I don't know which OS it is being used. Cannot optimize the code..."
#endif

#include <sys/stat.h>

#include <kognac/lz4io.h>
#include <kognac/logs.h>

#include <algorithm>
#include <vector>
#include <string>
#include <set>
#include <assert.h>
#include <stdio.h>

using namespace std;

/**** FILE UTILS ****/
//Return full path of the exec/library
string Utils::getFullPathExec() {
#if defined(_WIN32)
    char ownPth[MAX_PATH];
    // When NULL is passed to GetModuleHandle, the handle of the exe itself is returned
    HMODULE hModule = GetModuleHandle(NULL);
    if (hModule != NULL) {
        // Use GetModuleFileName() with module handle to get the path
        GetModuleFileName(hModule, ownPth, (sizeof(ownPth)));
        return string(ownPth);
    } else {
        return string("");
    }
#elif defined(__unix__) || defined(__unix) || defined(unix) || (defined(__APPLE__) && defined(__MACH__))
    char buff[FILENAME_MAX];
    getcwd(buff, FILENAME_MAX);
    std::string current_working_dir(buff);
    return current_working_dir;
#endif
}

//Return only the files or the entire path?
vector<string> Utils::getFilesWithPrefix(string dirname, string prefix) {
    vector<string> files;
    vector<string> allfiles = Utils::getFiles(dirname);
    for (uint64_t i = 0; i < allfiles.size(); ++i) {
        string fn = filename(allfiles[i]);
        if (Utils::starts_with(fn, prefix))
            files.push_back(allfiles[i]);
    }
    return files;
}

vector<string> Utils::getSubdirs(string dirname) {
    vector<string> files;
#if defined(_WIN32)
    WIN32_FIND_DATA ffd;
    TCHAR szDir[MAX_PATH];
    HANDLE hFind = INVALID_HANDLE_VALUE;
    DWORD dwError = 0;
	std::string toSearch = dirname + DIR_SEP + "*";
    hFind = FindFirstFile(toSearch.c_str(), &ffd);
    if (INVALID_HANDLE_VALUE == hFind) {
        return std::vector<string>();
    }
    // List all the files in the directory with some info about them.
    do {
        if (ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
			if (string(ffd.cFileName) == "." || string(ffd.cFileName) == "..")
				continue;
            string fileFullPath = dirname + DIR_SEP + ffd.cFileName;
            files.push_back(fileFullPath);
        }
    } while (FindNextFile(hFind, &ffd) != 0);
#else
    DIR *d = opendir(dirname.c_str());
    struct dirent *dir;
    if (d) {
        while ((dir = readdir(d)) != NULL) {
            string path = dirname + DIR_SEP + string(dir->d_name);
            if (isDirectory(path) && dir->d_name[0] != '.') {
                files.push_back(path);
            }
        }
        closedir(d);
    }
#endif
    return files;
}

vector<string> Utils::getFiles(string dirname, bool ignoreExtension) {
    std::set<string> sfiles;
#if defined(_WIN32)
    WIN32_FIND_DATA ffd;
    TCHAR szDir[MAX_PATH];
    HANDLE hFind = INVALID_HANDLE_VALUE;
    DWORD dwError = 0;
	std::string toSearch = dirname + DIR_SEP + "*";
    hFind = FindFirstFile(toSearch.c_str(), &ffd);
    if (INVALID_HANDLE_VALUE == hFind) {
        return std::vector<string>();
    }
    // List all the files in the directory with some info about them.
    do {
        if (ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
            //ignore the subdirectories
        }
        else if (ffd.cFileName[0] != '.') {
            string fileFullPath = dirname + DIR_SEP + ffd.cFileName;
            if (ignoreExtension) {
                sfiles.insert(Utils::removeExtension(fileFullPath));
            }
            else {
                sfiles.insert(fileFullPath);
            }
        }
    } while (FindNextFile(hFind, &ffd) != 0);
#else
    DIR *d = opendir(dirname.c_str());
    struct dirent *dir;
    if (d) {
        while ((dir = readdir(d)) != NULL) {
            if (dir->d_name[0] != '.') {
                string fileFullPath = dirname + DIR_SEP + string(dir->d_name);
                if (ignoreExtension) {
                    sfiles.insert(Utils::removeExtension(fileFullPath));
                }
                else {
                    sfiles.insert(fileFullPath);
                }
            }
        }
        closedir(d);
    }
#endif
    std::vector<string> files;
    for(auto s : sfiles) {
        files.push_back(s);
    }
    return files;
}
vector<string> Utils::getFilesWithSuffix(string dirname, string suffix) {
    vector<string> files;
    vector<string> allfiles = Utils::getFiles(dirname);
    for (uint64_t i = 0; i < allfiles.size(); ++i) {
        string f = allfiles[i];
        if (Utils::ends_with(f, suffix)) {
            files.push_back(f);
        }
    }
    return files;
}
bool Utils::hasExtension(const string &file){
    string fn = filename(file);
    return (fn.find('.') != std::string::npos);
}
string Utils::extension(const string &file) {
    string fn = filename(file);
    auto pos = fn.find_last_of('.');
    return fn.substr(pos, fn.size() - pos); //must return also '.'
}
string Utils::removeExtension(string file) {
    string fn = filename(file);
    auto pos = fn.find('.');
    if (pos != std::string::npos) {
	pos += file.size() - fn.size();
        return file.substr(0, pos);
    } else {
        return file;
    }
}
string Utils::removeLastExtension(string file) {
    string fn = filename(file);
    auto pos = fn.find_last_of('.');
    if (pos != std::string::npos) {
	pos += file.size() - fn.size();
        return file.substr(0, pos);
    } else {
        return file;
    }
}
bool Utils::isDirectory(string dirname) {
#if defined(_WIN32)
	DWORD d = GetFileAttributes(dirname.c_str());
	if (d &FILE_ATTRIBUTE_DIRECTORY) {
		return true;
	} else {
		return false;
	}
#else
	struct stat st;
	auto resp = stat(dirname.c_str(), &st);
	if (resp == 0) {
		if ((st.st_mode & S_IFDIR) != 0)
			return true;
	}
    return false;
#endif
}
bool Utils::isFile(string dirname) {
#if defined(_WIN32)
	DWORD d = GetFileAttributes(dirname.c_str());
	if (!(d & FILE_ATTRIBUTE_DIRECTORY)) {
		return true;
	}
	else {
		return false;
	}
#else
	struct stat st;
	if (stat(dirname.c_str(), &st) == 0)
		if ((st.st_mode & S_IFREG) != 0)
			return true;
	return false;
#endif
}
uint64_t Utils::fileSize(string file) {
#if defined(_WIN32)
	LARGE_INTEGER size;
	HANDLE fd = CreateFile(file.c_str(), GENERIC_READ,
		FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_EXISTING,
		FILE_ATTRIBUTE_NORMAL, NULL);
	bool res = GetFileSizeEx(fd, &size);
	CloseHandle(fd);
	if (!res) {
		throw 10;
	}
	return size.QuadPart;
#else
    struct stat stat_buf;
    int rc = stat(file.c_str(), &stat_buf);
    if (rc == 0) {
        return stat_buf.st_size;
    } else {
        throw 10;
    }
#endif
}
void Utils::create_directories(string newdir) {
    string pd = parentDir(newdir);
    if (pd != newdir && !exists(pd)) {
        create_directories(pd);
    }
    if (!Utils::exists(newdir)) {
#if defined(_WIN32)
        _mkdir(newdir.c_str());
#else
        if (mkdir(newdir.c_str(), 0777) != 0) {
            LOG(ERRORL) << "Error creating dir " << newdir;
            throw 10;
        }
#endif
    } else if (!Utils::isDirectory(newdir)) {
        LOG(ERRORL) << "Directory " << newdir << " is already existing but it is not a dir";
        abort();
    }
}
void Utils::remove(string file) {
    if (isDirectory(file)) {
#if defined(_WIN32)
		auto resp = RemoveDirectory(file.c_str());
		if (resp == 0) {
			DWORD errorMessageID = ::GetLastError();
			LPSTR messageBuffer = nullptr;
			size_t size = FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
				NULL, errorMessageID, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPSTR)&messageBuffer, 0, NULL);
			std::string message(messageBuffer, size);
			LocalFree(messageBuffer);
			LOG(ERRORL) << "Error deleting directory " << file << " " << message;
			abort();
		}
#else
        if (rmdir(file.c_str()) != 0) {
            LOG(ERRORL) << "Error removing dir " << file;
            abort();
        }
#endif
	} else {
#if defined(_WIN32)
		auto resp = DeleteFile(file.c_str());
		if (resp == 0) {
			DWORD errorMessageID = ::GetLastError();
			LPSTR messageBuffer = nullptr;
			size_t size = FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
				NULL, errorMessageID, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPSTR)&messageBuffer, 0, NULL);
			std::string message(messageBuffer, size);
			LocalFree(messageBuffer);
			LOG(ERRORL) << "Error deleting file " << file << " " << message;
			abort();
		}
#else
		int resp = ::remove(file.c_str());
		if (resp != 0) {
			LOG(ERRORL) << "Error deleting file " << file;
			abort();
		}
#endif
    }
}
void Utils::remove_all(string path) {
    if (isDirectory(path)) {
        std::vector<std::string> filechildren = Utils::getFiles(path);
        for (uint64_t i = 0; i < filechildren.size(); ++i) {
            remove(filechildren[i]);
        }
        std::vector<std::string> subdirs = Utils::getSubdirs(path);
        for (uint64_t i = 0; i < subdirs.size(); ++i) {
            remove_all(subdirs[i]);
        }
        remove(path);
    } else {
        remove(path);
    }
}
void Utils::rename(string oldfile, string newfile) {
    if(std::rename(oldfile.c_str(), newfile.c_str()) != 0 )
        LOG(ERRORL) << "Error renaming file " << oldfile;
}
string Utils::parentDir(string path) {
    auto pos = path.rfind(CDIR_SEP);
    if (pos != std::string::npos) {
        return path.substr(0, pos);
    } else {
        return path;
    }
}
string Utils::filename(string path) {
    auto pos = path.rfind(CDIR_SEP);
    if (pos != std::string::npos) {
        return path.substr(pos + 1, path.size() - pos);
    } else {
        return path;
    }
}
bool Utils::exists(std::string file) {
    struct stat buffer;
    return (stat(file.c_str(), &buffer) == 0);
}
bool Utils::isEmpty(string dir) {
    if (!Utils::isDirectory(dir)) {
        LOG(ERRORL) << dir << " is not a directory!";
        throw 10;
    }
    return Utils::getFiles(dir).empty() && Utils::getSubdirs(dir).empty();
}
void Utils::resizeFile(string file, uint64_t newsize) {
    if (!Utils::exists(file)) {
        LOG(ERRORL) << file << " does not exist";
        throw 10;
    }
    uint64_t oldsize = Utils::fileSize(file);
    if (oldsize != newsize) {
#if defined(_WIN32)
        int fd = _sopen_s(&fd, file.c_str(), _O_RDWR | _O_CREAT, _SH_DENYNO,
                _S_IREAD | _S_IWRITE);
        _chsize(fd, newsize);
#else
        truncate(file.c_str(), newsize);
#endif
    }
}
/**** END FILE UTILS ****/
/**** START STRING UTILS ****/
bool Utils::starts_with(const string s, const string prefix) {
    int m = min(s.size(), prefix.size());
    return strncmp(s.c_str(), prefix.c_str(), m) == 0;
}
bool Utils::ends_with(const string s, const string suffix) {
    return s.size() >= suffix.size() &&
        s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}
bool Utils::contains(const string s, const string substr) {
    return s.find(substr) != string::npos;
}
/**** END STRING UTILS ****/

int Utils::numBytes(int64_t number) {
    int64_t max = 32;
    if (number < 0) {
        LOG(ERRORL) << "Negative number " << number;
    }
    for (int i = 1; i <= 8; i++) {
        if (number < max) {
            return i;
        }
        max *= 256;
    }
    LOG(ERRORL) << "Number is too large: " << number;
    return -1;
}

int Utils::numBytesFixedLength(int64_t number) {
    uint8_t bytes = 0;
    do {
        bytes++;
        number = number >> 8;
    } while (number > 0);
    return bytes;
}

int Utils::numBytes2(int64_t number) {
    int nbytes = 0;
    do {
        number >>= 7;
        nbytes++;
    } while (number > 0);
    return nbytes;
}

int Utils::decode_int(char* buffer, int offset) {
    int n = (buffer[offset++] & 0xFF) << 24;
    n += (buffer[offset++] & 0xFF) << 16;
    n += (buffer[offset++] & 0xFF) << 8;
    n += buffer[offset] & 0xFF;
    return n;
}

int Utils::decode_int(const char* buffer) {
    int n = (buffer[0] & 0xFF) << 24;
    n += (buffer[1] & 0xFF) << 16;
    n += (buffer[2] & 0xFF) << 8;
    n += buffer[3] & 0xFF;
    return n;
}

void Utils::encode_int(char* buffer, int offset, int n) {
    buffer[offset++] = (n >> 24) & 0xFF;
    buffer[offset++] = (n >> 16) & 0xFF;
    buffer[offset++] = (n >> 8) & 0xFF;
    buffer[offset++] = n & 0xFF;
}

void Utils::encode_int(char* buffer, int n) {
    buffer[0] = (n >> 24) & 0xFF;
    buffer[1] = (n >> 16) & 0xFF;
    buffer[2] = (n >> 8) & 0xFF;
    buffer[3] = n & 0xFF;
}

int Utils::decode_intLE(char* buffer, int offset) {
    int n = buffer[offset++] & 0xFF;
    n += (buffer[offset++] & 0xFF) << 8;
    n += (buffer[offset++] & 0xFF) << 16;
    n += (buffer[offset] & 0xFF) << 24;
    return n;
}

void Utils::encode_intLE(char* buffer, int offset, int n) {
    buffer[offset++] = n & 0xFF;
    buffer[offset++] = (n >> 8) & 0xFF;
    buffer[offset++] = (n >> 16) & 0xFF;
    buffer[offset++] = (n >> 24) & 0xFF;
}

int64_t Utils::decode_long(char* buffer, int offset) {
    int64_t n = (int64_t) (buffer[offset++]) << 56;
    n += (int64_t) (buffer[offset++] & 0xFF) << 48;
    n += (int64_t) (buffer[offset++] & 0xFF) << 40;
    n += (int64_t) (buffer[offset++] & 0xFF) << 32;
    n += (int64_t) (buffer[offset++] & 0xFF) << 24;
    n += (buffer[offset++] & 0xFF) << 16;
    n += (buffer[offset++] & 0xFF) << 8;
    n += buffer[offset] & 0xFF;
    return n;
}

int64_t Utils::decode_longFixedBytes(const char* buffer, const uint8_t nbytes) {
    uint8_t offset = 0;
    int64_t n = 0;
    switch (nbytes) {
        case 1:
            n += buffer[offset] & 0xFF;
            break;
        case 2:
            n += (buffer[offset++] & 0xFF) << 8;
            n += buffer[offset] & 0xFF;
            break;
        case 3:
            n += (buffer[offset++] & 0xFF) << 16;
            n += (buffer[offset++] & 0xFF) << 8;
            n += buffer[offset] & 0xFF;
            break;
        case 4:
            n += (int64_t) (buffer[offset++] & 0xFF) << 24;
            n += (buffer[offset++] & 0xFF) << 16;
            n += (buffer[offset++] & 0xFF) << 8;
            n += buffer[offset] & 0xFF;
            break;
        case 5:
            n += (int64_t) (buffer[offset++] & 0xFF) << 32;
            n += (int64_t) (buffer[offset++] & 0xFF) << 24;
            n += (buffer[offset++] & 0xFF) << 16;
            n += (buffer[offset++] & 0xFF) << 8;
            n += buffer[offset] & 0xFF;
            break;
        case 6:
            n += (int64_t) (buffer[offset++] & 0xFF) << 40;
            n += (int64_t) (buffer[offset++] & 0xFF) << 32;
            n += (int64_t) (buffer[offset++] & 0xFF) << 24;
            n += (buffer[offset++] & 0xFF) << 16;
            n += (buffer[offset++] & 0xFF) << 8;
            n += buffer[offset] & 0xFF;
            break;
        case 7:
            n += (int64_t) (buffer[offset++] & 0xFF) << 48;
            n += (int64_t) (buffer[offset++] & 0xFF) << 40;
            n += (int64_t) (buffer[offset++] & 0xFF) << 32;
            n += (int64_t) (buffer[offset++] & 0xFF) << 24;
            n += (buffer[offset++] & 0xFF) << 16;
            n += (buffer[offset++] & 0xFF) << 8;
            n += buffer[offset] & 0xFF;
            break;
        case 8:
            n += (int64_t) (buffer[offset++]) << 56;
            n += (int64_t) (buffer[offset++] & 0xFF) << 48;
            n += (int64_t) (buffer[offset++] & 0xFF) << 40;
            n += (int64_t) (buffer[offset++] & 0xFF) << 32;
            n += (int64_t) (buffer[offset++] & 0xFF) << 24;
            n += (buffer[offset++] & 0xFF) << 16;
            n += (buffer[offset++] & 0xFF) << 8;
            n += buffer[offset] & 0xFF;
            break;
    }
    return n;
}

int64_t Utils::decode_long(const char* buffer) {
    int64_t n = (int64_t) (buffer[0]) << 56;
    n += (int64_t) (buffer[1] & 0xFF) << 48;
    n += (int64_t) (buffer[2] & 0xFF) << 40;
    n += (int64_t) (buffer[3] & 0xFF) << 32;
    n += (int64_t) (buffer[4] & 0xFF) << 24;
    n += (buffer[5] & 0xFF) << 16;
    n += (buffer[6] & 0xFF) << 8;
    n += buffer[7] & 0xFF;
    return n;
}

void Utils::encode_long(char* buffer, int offset, int64_t n) {
    buffer[offset++] = (n >> 56) & 0xFF;
    buffer[offset++] = (n >> 48) & 0xFF;
    buffer[offset++] = (n >> 40) & 0xFF;
    buffer[offset++] = (n >> 32) & 0xFF;
    buffer[offset++] = (n >> 24) & 0xFF;
    buffer[offset++] = (n >> 16) & 0xFF;
    buffer[offset++] = (n >> 8) & 0xFF;
    buffer[offset++] = n & 0xFF;
}

void Utils::encode_long(char* buffer, int64_t n) {
    buffer[0] = (n >> 56) & 0xFF;
    buffer[1] = (n >> 48) & 0xFF;
    buffer[2] = (n >> 40) & 0xFF;
    buffer[3] = (n >> 32) & 0xFF;
    buffer[4] = (n >> 24) & 0xFF;
    buffer[5] = (n >> 16) & 0xFF;
    buffer[6] = (n >> 8) & 0xFF;
    buffer[7] = n & 0xFF;
}

void Utils::encode_longNBytes(char* buffer, const uint8_t nbytes,
        const uint64_t n) {
    uint8_t offset = 0;
    switch (nbytes) {
        case 1:
            buffer[offset] = n & 0xFF;
            break;
        case 2:
            buffer[offset++] = (n >> 8) & 0xFF;
            buffer[offset++] = n & 0xFF;
            break;
        case 3:
            buffer[offset++] = (n >> 16) & 0xFF;
            buffer[offset++] = (n >> 8) & 0xFF;
            buffer[offset++] = n & 0xFF;
            break;
        case 4:
            buffer[offset++] = (n >> 24) & 0xFF;
            buffer[offset++] = (n >> 16) & 0xFF;
            buffer[offset++] = (n >> 8) & 0xFF;
            buffer[offset++] = n & 0xFF;
            break;
        case 5:
            buffer[offset++] = (n >> 32) & 0xFF;
            buffer[offset++] = (n >> 24) & 0xFF;
            buffer[offset++] = (n >> 16) & 0xFF;
            buffer[offset++] = (n >> 8) & 0xFF;
            buffer[offset++] = n & 0xFF;
            break;
        case 6:
            buffer[offset++] = (n >> 40) & 0xFF;
            buffer[offset++] = (n >> 32) & 0xFF;
            buffer[offset++] = (n >> 24) & 0xFF;
            buffer[offset++] = (n >> 16) & 0xFF;
            buffer[offset++] = (n >> 8) & 0xFF;
            buffer[offset++] = n & 0xFF;
            break;
        case 7:
            buffer[offset++] = (n >> 48) & 0xFF;
            buffer[offset++] = (n >> 40) & 0xFF;
            buffer[offset++] = (n >> 32) & 0xFF;
            buffer[offset++] = (n >> 24) & 0xFF;
            buffer[offset++] = (n >> 16) & 0xFF;
            buffer[offset++] = (n >> 8) & 0xFF;
            buffer[offset++] = n & 0xFF;
            break;
        case 8:
            buffer[offset++] = (n >> 56) & 0xFF;
            buffer[offset++] = (n >> 48) & 0xFF;
            buffer[offset++] = (n >> 40) & 0xFF;
            buffer[offset++] = (n >> 32) & 0xFF;
            buffer[offset++] = (n >> 24) & 0xFF;
            buffer[offset++] = (n >> 16) & 0xFF;
            buffer[offset++] = (n >> 8) & 0xFF;
            buffer[offset++] = n & 0xFF;
            break;
        default:
            throw 10;
    }
}

int64_t Utils::decode_longWithHeader(char* buffer) {
    int64_t n = (int64_t) (buffer[0] & 0x7F) << 49;
    n += (int64_t) (buffer[1] & 0x7F) << 42;
    n += (int64_t) (buffer[2] & 0x7F) << 35;
    n += (int64_t) (buffer[3] & 0x7F) << 28;
    n += (int64_t) (buffer[4] & 0x7F) << 21;
    n += (buffer[5] & 0x7F) << 14;
    n += (buffer[6] & 0x7F) << 7;
    n += buffer[7] & 0x7F;
    return n;
}

void Utils::encode_longWithHeader0(char* buffer, int64_t n) {

    if (n < 0) {
        LOG(ERRORL) << "Number is negative";
        exit(1);
    }

    buffer[0] = (n >> 49) & 0x7F;
    buffer[1] = (n >> 42) & 0x7F;
    buffer[2] = (n >> 35) & 0x7F;
    buffer[3] = (n >> 28) & 0x7F;
    buffer[4] = (n >> 21) & 0x7F;
    buffer[5] = (n >> 14) & 0x7F;
    buffer[6] = (n >> 7) & 0x7F;
    buffer[7] = n & 0x7F;
}

void Utils::encode_longWithHeader1(char* buffer, int64_t n) {

    if (n < 0) {
        LOG(ERRORL) << "Number is negative";
        exit(1);
    }

    buffer[0] = ((n >> 49) | 0x80) & 0xFF;
    buffer[1] = ((n >> 42) | 0x80) & 0xFF;
    buffer[2] = ((n >> 35) | 0x80) & 0xFF;
    buffer[3] = ((n >> 28) | 0x80) & 0xFF;
    buffer[4] = ((n >> 21) | 0x80) & 0xFF;
    buffer[5] = ((n >> 14) | 0x80) & 0xFF;
    buffer[6] = ((n >> 7) | 0x80) & 0xFF;
    buffer[7] = (n | 0x80) & 0xFF;
}

//long Utils::decode_long(char* buffer, int offset, const char nbytes) {
//  long n = 0;
//  for (int i = nbytes - 1; i >= 0; i--) {
//      n += (long) (buffer[offset++] & 0xFF) << i * 8;
//  }
//  return n;
//}

short Utils::decode_short(const char* buffer, int offset) {
    return (short) (((buffer[offset] & 0xFF) << 8) + (buffer[offset + 1] & 0xFF));
}

void Utils::encode_short(char* buffer, int offset, int n) {
    buffer[offset++] = (n >> 8) & 0xFF;
    buffer[offset++] = n & 0xFF;
}

void Utils::encode_short(char* buffer, int n) {
    buffer[0] = (n >> 8) & 0xFF;
    buffer[1] = n & 0xFF;
}

int64_t Utils::decode_vlong(char* buffer, int *offset) {
    int pos = *offset;
    int first = buffer[pos++];
    int nbytes = ((first & 255) >> 5) + 1;
    int64_t retval = (first & 31);

    switch (nbytes) {
        case 2:
            retval += (buffer[pos++] & 255) << 5;
            break;
        case 3:
            retval += (buffer[pos++] & 255) << 5;
            retval += (buffer[pos++] & 255) << 13;
            break;
        case 4:
            retval += (buffer[pos++] & 255) << 5;
            retval += (buffer[pos++] & 255) << 13;
            retval += (buffer[pos++] & 255) << 21;
            break;
        case 5:
            retval += (buffer[pos++] & 255) << 5;
            retval += (buffer[pos++] & 255) << 13;
            retval += (buffer[pos++] & 255) << 21;
            retval += (int64_t) (buffer[pos++] & 255) << 29;
            break;
        case 6:
            retval += (buffer[pos++] & 255) << 5;
            retval += (buffer[pos++] & 255) << 13;
            retval += (buffer[pos++] & 255) << 21;
            retval += (int64_t) (buffer[pos++] & 255) << 29;
            retval += (int64_t) (buffer[pos++] & 255) << 37;
            break;
        case 7:
            retval += (buffer[pos++] & 255) << 5;
            retval += (buffer[pos++] & 255) << 13;
            retval += (buffer[pos++] & 255) << 21;
            retval += (int64_t) (buffer[pos++] & 255) << 29;
            retval += (int64_t) (buffer[pos++] & 255) << 37;
            retval += (int64_t) (buffer[pos++] & 255) << 45;
            break;
        case 8:
            retval += (buffer[pos++] & 255) << 5;
            retval += (buffer[pos++] & 255) << 13;
            retval += (buffer[pos++] & 255) << 21;
            retval += (int64_t) (buffer[pos++] & 255) << 29;
            retval += (int64_t) (buffer[pos++] & 255) << 37;
            retval += (int64_t) (buffer[pos++] & 255) << 45;
            retval += (int64_t) (buffer[pos++] & 255) << 53;
            break;
    }
    *offset = pos;
    return retval;
}

int Utils::encode_vlong(char* buffer, int offset, int64_t n) {
    int nbytes = numBytes(n);
    buffer[offset++] = (((nbytes - 1) << 5) + ((int) n & 31));
    n >>= 5;
    for (int i = 1; i < nbytes; i++) {
        buffer[offset++] = ((int) n & 255);
        n >>= 8;
    }
    return offset;
}

uint16_t Utils::encode_vlong(char* buffer, int64_t n) {
    int nbytes = numBytes(n);
    int offset = 0;
    buffer[offset++] = (((nbytes - 1) << 5) + ((int) n & 31));
    n >>= 5;
    for (int i = 1; i < nbytes; i++) {
        buffer[offset++] = ((int) n & 255);
        n >>= 8;
    }
    return nbytes;
}
//short Utils::decode_vshort(char* buffer, int *offset) {
//  char n = buffer[(*offset)++];
//  if (n < 0) {
//      short return_value = (short) ((n & 127) << 8);
//      return_value += buffer[(*offset)++] & 255;
//      return return_value;
//  } else {
//      return n;
//  }
//}

//int Utils::decode_vint(char* buffer, int *offset) {
//  int n = 0;
//  int pos = *offset;
//  int b = buffer[pos++];
//  n = b & 63;
//  int nbytes = (b >> 6) & 3;
//  switch (nbytes) {
//  case 1:
//      n += (buffer[pos++] & 255) << 6;
//      break;
//  case 2:
//      n += (buffer[pos++] & 255) << 6;
//      n += (buffer[pos++] & 255) << 14;
//      break;
//  case 3:
//      n += (buffer[pos++] & 255) << 6;
//      n += (buffer[pos++] & 255) << 14;
//      n += (buffer[pos++] & 255) << 22;
//      break;
//  }
//  *offset = pos;
//  return n;
//}

int Utils::decode_vint2(char* buffer, int *offset) {
    int pos = *offset;
    int number = buffer[pos++];
    if (number < 0) {
        int longNumber = number & 127;
        int shiftBytes = 7;
        do {
            number = buffer[pos++];
            longNumber += ((number & 127) << shiftBytes);
            shiftBytes += 7;
        } while (number < 0);
        *offset = pos;
        return longNumber;
    } else {
        *offset = pos;
        return number;
    }
}

int Utils::encode_vlong2(char* buffer, int offset, int64_t n) {
    if (n < 0) {
        LOG(ERRORL) << "Number is negative. This is not allowed with vlong2";
        throw 10;
    }

    if (n < 128) { // One byte is enough
        buffer[offset++] = n;
        return offset;
    } else {
        int bytesToStore = 64 - numberOfLeadingZeros((uint64_t)n);
        while (bytesToStore > 7) {
            buffer[offset++] = ((n & 127) + 128);
            n >>= 7;
            bytesToStore -= 7;
        }
        buffer[offset++] = n & 127;
    }
    return offset;
}

uint16_t Utils::encode_vlong2(char* buffer, int64_t n) {
    if (n < 0) {
        LOG(ERRORL) << "Number is negative. This is not allowed with vlong2";
        throw 10;
    }

    if (n < 128) { // One byte is enough
        buffer[0] = n;
        return 1;
    } else {
        int bytesToStore = 64 - numberOfLeadingZeros((uint64_t)n);
        uint16_t offset = 0;
        while (bytesToStore > 7) {
            buffer[offset++] = ((n & 127) + 128);
            n >>= 7;
            bytesToStore -= 7;
        }
        buffer[offset++] = n & 127;
        return offset;
    }
}

void Utils::encode_vlong2_fixedLen(char* buffer, int64_t n, const uint8_t len) {
    char *beginbuffer = buffer;

    if (n < 128) { // One byte is enough
        *buffer = n;
    } else {
        int neededBits = 64 - numberOfLeadingZeros((uint64_t)n);
        while (neededBits > 7) {
            *buffer = ((n & 127) + 128);
            n >>= 7;
            buffer++;
            neededBits -= 7;
        }
        *buffer = n & 127;
    }
    buffer++;
    int remBytes = len - (buffer - beginbuffer);
    while (--remBytes >= 0) {
        *buffer = (char)128;
        buffer++;
    }
    assert((buffer - beginbuffer) == len);

}


int Utils::encode_vlong2_fast(uint8_t *out, uint64_t x) {
    int i, j;
    for (i = 9; i > 0; i--) {
        if (x & 127ULL << i * 7) break;
    }
    for (j = 0; j <= i; j++)
        out[j] = ((x >> ((i - j) * 7)) & 127) | 128;

    out[i] ^= 128;
    return i;
}

uint64_t Utils::decode_vlong2_fast(uint8_t *in) {
    uint64_t r = 0;

    do {
        r = (r << 7) | (uint64_t)(*in & 127);
    } while (*in++ & 128);

    return r;
}

int64_t Utils::decode_vlong2(const char* buffer, int *offset) {
    int pos = *offset;
    int shift = 7;
    int64_t n = buffer[pos] & 127;
    while (buffer[pos++] < 0) {
        n += (int64_t) (buffer[pos] & 127) << shift;
        shift += 7;
    }
    *offset = pos;
    return n;
}

int64_t Utils::decode_vlongWithHeader0(char* buffer, const int end, int *p) {
    int pos = 0;
    int shift = 7;
    int64_t n = buffer[pos++] & 127;
    while (pos < end && ((buffer[pos] & 128) == 0)) {
        n += (int64_t) (buffer[pos++] & 127) << shift;
        shift += 7;
    }
    *p = pos;
    return n;
}

int64_t Utils::decode_vlongWithHeader1(char* buffer, const int end, int *p) {
    int pos = 0;
    int shift = 7;
    int64_t n = buffer[pos++] & 127;
    while (pos < end && buffer[pos] < 0) {
        n += (int64_t) (buffer[pos++] & 127) << shift;
        shift += 7;
    }
    *p = pos;
    return n;
}

int Utils::encode_vlongWithHeader0(char* buffer, int64_t n) {
    if (n < 0) {
        LOG(ERRORL) << "Number is negative";
        return -1;
    }

    int i = 0;
    do {
        buffer[i++] = n & 127;
        n >>= 7;
    } while (n > 0);
    return i;
}

int Utils::encode_vlongWithHeader1(char* buffer, int64_t n) {
    if (n < 0) {
        LOG(ERRORL) << "Number is negative";
        return -1;
    }

    int i = 0;
    do {
        buffer[i++] = ( n | 128 ) & 0xFF;
        n >>= 7;
    } while (n > 0);
    return i;
}

int Utils::encode_vint2(char* buffer, int offset, int n) {
    if (n < 128) { // One byte is enough
        buffer[offset++] = n;
        return offset;
    } else {
        int bytesToStore = 32 - numberOfLeadingZeros((unsigned int) n);
        while (bytesToStore > 7) {
            buffer[offset++] = ((n & 127) + 128);
            n >>= 7;
            bytesToStore -= 7;
        }
        buffer[offset++] = (n & 127);
    }
    return offset;
}

int Utils::commonPrefix(tTerm *o1, int s1, int e1, tTerm *o2, int s2, int e2) {
    int len = 0;
    for (int i = s1, j = s2; i < e1 && j < e2; i++, j++) {
        if (o1[i] != o2[j]) {
            return len;
        }
        ++len;
    }
    return len;
}

int Utils::compare(const char* o1, int s1, int e1, const char* o2, int s2,
        int e2) {
    for (int i = s1, j = s2; i < e1 && j < e2; i++, j++) {
        if (o1[i] != o2[j]) {
            return ((int)o1[i] & 0xff) - ((int) o2[j] & 0xff);
        }
    }
    return (e1 - s1) - (e2 - s2);
}

int Utils::prefixEquals(char* o1, int len1, char* o2, int len2) {
    int i = 0;
    for (; i < len1 && i < len2; i++) {
        if (o1[i] != o2[i]) {
            return ((int) o1[i] & 0xff) - ((int) o2[i] & 0xff);
        }
    }
    if (i == len1) {
        return 0;
    } else {
        return 1;
    }
}

int Utils::prefixEquals(char* o1, int len, char* o2) {
    int i = 0;
    for (; i < len && o2[i] != '\0'; i++) {
        if (o1[i] != o2[i]) {
            return ((int) o1[i] & 0xff) - ((int) o2[i] & 0xff);
        }
    }
    if (i == len) {
        return 0;
    } else {
        return 1;
    }
}

double Utils::get_max_mem() {
    double memory = 0.0;

#if defined(__APPLE__) && defined(__MACH__)
    struct rusage rusage;
    getrusage(RUSAGE_SELF, &rusage);
    memory = (double) rusage.ru_maxrss / 1024 / 1024;
#elif defined(__unix__) || defined(__unix) || defined(unix)
    struct rusage rusage;
    getrusage(RUSAGE_SELF, &rusage);
    memory = (double)rusage.ru_maxrss / 1024;
#endif
#if defined(_WIN32)
    HANDLE hProcess;
    PROCESS_MEMORY_COUNTERS pmc;
    bool result = GetProcessMemoryInfo(GetCurrentProcess(),
            &pmc, sizeof(PPROCESS_MEMORY_COUNTERS));
    memory = pmc.PeakWorkingSetSize / 1024 / 1024;
#endif
    return memory;
}

uint64_t Utils::getSystemMemory() {
#if defined(__APPLE__) && defined(__MACH__)
    int mib[2];
    mib[0] = CTL_HW;
    mib[1] = HW_MEMSIZE;
    int64_t size = 0;
    size_t len = sizeof(size);
    sysctl(mib, 2, &size, &len, NULL, 0);
    return size;
#elif defined(__unix__) || defined(__unix) || defined(unix)
    uint64_t pages = sysconf(_SC_PHYS_PAGES);
    uint64_t page_size = sysconf(_SC_PAGE_SIZE);
    return pages * page_size;
#elif defined(_WIN32)
    MEMORYSTATUSEX memInfo;
    memInfo.dwLength = sizeof(MEMORYSTATUSEX);
    GlobalMemoryStatusEx(&memInfo);
    return memInfo.ullTotalPhys;
#endif
}

uint64_t Utils::getUsedMemory() {
#if defined(_WIN32)
    /* Windows -------------------------------------------------- */
    PROCESS_MEMORY_COUNTERS info;
    GetProcessMemoryInfo( GetCurrentProcess( ), &info, sizeof(info) );
    return (size_t)info.WorkingSetSize;

#elif defined(__APPLE__) && defined(__MACH__)
    /* OSX ------------------------------------------------------ */
    struct mach_task_basic_info info;
    mach_msg_type_number_t infoCount = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info( mach_task_self( ), MACH_TASK_BASIC_INFO, (task_info_t) &info,
                &infoCount) != KERN_SUCCESS)
        return (size_t) 0L; /* Can't access? */
    return (size_t) info.resident_size;

#elif defined(__linux__) || defined(__linux) || defined(linux) || defined(__gnu_linux__)
    /* Linux ---------------------------------------------------- */
    uint64_t rss = 0L;
    FILE* fp = NULL;
    if ( (fp = fopen( "/proc/self/statm", "r" )) == NULL )
        return (size_t)0L; /* Can't open? */
    if ( fscanf( fp, "%*s%ld", &rss ) != 1 ) {
        fclose( fp );
        return (size_t)0L; /* Can't read? */
    }
    fclose( fp );
    return (size_t)rss * (size_t)sysconf( _SC_PAGESIZE);

#else
    /* AIX, BSD, Solaris, and Unknown OS ------------------------ */
    return (size_t)0L; /* Unsupported. */
#endif
}

uint64_t Utils::getIOReadChars() {
#if defined(__linux__) || defined(__linux) || defined(linux) || defined(__gnu_linux__)
    std::ifstream file("/proc/self/io");
    std::string line;
    while (std::getline(file, line)) {
        string::size_type loc = line.find("rchar", 0);
        if (loc != string::npos) {
            string number = line.substr(7);
            return stol(number);
        }
    }
    return (size_t)0L;
#else
    return (size_t)0L; /* Unsupported. */
#endif
}

uint64_t Utils::getIOReadBytes() {
#if defined(__linux__) || defined(__linux) || defined(linux) || defined(__gnu_linux__)
    std::ifstream file("/proc/self/io");
    std::string line;
    while (std::getline(file, line)) {
        string::size_type loc = line.find("read_bytes", 0);
        if (loc != string::npos) {
            string number = line.substr(12);
            return stol(number);
        }
    }
    return (size_t)0L;
#else
    return -1; /* Unsupported. */
#endif
}



uint64_t Utils::getCPUCounter() {
#if defined(__unix__) || defined(__unix) || defined(unix) || (defined(__APPLE__) && defined(__MACH__))
    unsigned a, d;
    __asm__ volatile("rdtsc" : "=a" (a), "=d" (d));
    return ((uint64_t)a) | (((uint64_t)d) << 32);
#else
    return 0; //unsupported
#endif
}

int Utils::getNumberPhysicalCores() {
#if defined(__unix__) || defined(__unix) || defined(unix) || (defined(__APPLE__) && defined(__MACH__))
    return sysconf( _SC_NPROCESSORS_ONLN);
#else
    SYSTEM_INFO sysinfo;
    GetSystemInfo(&sysinfo);
    return sysinfo.dwNumberOfProcessors;
#endif
}

int64_t Utils::quickSelect(int64_t *vector, int size, int k) {
    std::vector<int64_t> supportVector(vector, vector + size);
    std::nth_element(supportVector.begin(), supportVector.begin() + k,
            supportVector.end());
    return supportVector[k];
    //  if (start == end)
    //          return vector[start];
    //      int j = partition(vector, start, end);
    //      int length = j - start + 1;
    //      if (length == k)
    //          return vector[j];
    //      else if (k < length)
    //          return quickSelect(vector, start, j - 1, k);
    //      else
    //          return quickSelect(vector, j + 1, end, k - length);
}

uint64_t Utils::getNBytes(std::string input) {
    if (Utils::isDirectory(input)) {
        uint64_t size = 0;
        std::vector<std::string> files = Utils::getFiles(input);
        for (uint64_t i = 0; i < files.size(); ++i) {
            if (isFile(files[i]))
                size += Utils::fileSize(files[i]);
        }
        /*for (fs::directory_iterator itr(input); itr != fs::directory_iterator();
          ++itr) {
          if (fs::is_regular(itr->path())) {
          size += fs::file_size(itr->path());
          }
          }*/
        return size;
    } else {
        return Utils::fileSize(input);
    }
}

bool Utils::isCompressed(std::string input) {
    if (Utils::isDirectory(input)) {
        bool isCompressed = false;
        std::vector<std::string> files = Utils::getFiles(input);
        for (uint64_t i = 0; i < files.size(); ++i) {
            string &f = files[i];
            if (Utils::extension(f) == string(".gz")) {
                isCompressed = true;
            }
        }
        /*for (fs::directory_iterator itr(input); itr != fs::directory_iterator();
          ++itr) {
          if (fs::is_regular(itr->path())) {
          if (itr->path().extension() == string(".gz"))
          isCompressed = true;
          }
          }*/
        return isCompressed;
    } else {
        return Utils::extension(input) == string(".gz");
    }
}

void Utils::linkdir(string source, string dest) {
#if defined(_WIN32)
    LOG(ERRORL) << "LinkDir: Not supported";
    throw 10;
#elif defined(__unix__) || defined(__unix) || defined(unix) || (defined(__APPLE__) && defined(__MACH__))
    symlink(source.c_str(), dest.c_str());
#endif
}

void Utils::rmlink(string link) {
#if defined(_WIN32)
    LOG(ERRORL) << "RmLink: Not supported";
    throw 10;
#elif defined(__unix__) || defined(__unix) || defined(unix) || (defined(__APPLE__) && defined(__MACH__))
    unlink(link.c_str());
#endif
}

//int partition(long* input, int start, int end) {
//  int pivot = input[end];
//
//  while (start < end) {
//      while (input[start] < pivot)
//          start++;
//
//      while (input[end] > pivot)
//          end--;
//
//      if (input[start] == input[end])
//          start++;
//      else if (start < end) {
//          int tmp = input[start];
//          input[start] = input[end];
//          input[end] = tmp;
//      }
//  }
//
//  return end;
//}
