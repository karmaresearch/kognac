#ifndef _DISK_LZ4_READER_H
#define _DISK_LZ4_READER_H

#include <kognac/lz4io.h>

#include <fstream>
#include <list>
#include <vector>
#include <thread>
#include <string>
#include <mutex>
#include <condition_variable>

using namespace std;

class DiskLZ4Reader {
private:
    struct FileInfo {
        char *buffer;
        size_t sizebuffer;
        size_t pivot;
    };

    //Info about the files
    const int nbuffersPerFile;
    string inputfile;
    std::vector<FileInfo> files;
    ifstream reader;
    std::vector<std::vector<int64_t> > beginningBlocks;
    std::vector<int64_t> readBlocks;

    //support buffers for strings
    std::vector<std::unique_ptr<char[]> > supportstringbuffers;

    bool uncompressBuffer(const int id);

    bool getNewCompressedBuffer(std::unique_lock<std::mutex> &lk,
                                const int id);

    void run();

protected:

    struct BlockToRead {
        char *buffer;
        size_t sizebuffer;
        size_t pivot;
    };

    std::mutex m_diskbufferpool;
    std::thread currentthread;

    //Pool of compressed buffers
    std::list<BlockToRead> *compressedbuffers;
    std::vector<int64_t> sCompressedbuffers; //number of elements in each list

    //Larger buffers to read from disk. They are proportional to SIZE_SEG
    std::vector<char*> diskbufferpool;
    std::condition_variable cond_diskbufferpool;
    std::chrono::duration<double> time_diskbufferpool;
    std::chrono::duration<double> time_rawreading;

    std::mutex *m_files;
    std::condition_variable *cond_files;
    std::chrono::duration<double> *time_files;

public:
	KLIBEXP DiskLZ4Reader(string inputfile, int npartitions, int nbuffersPerFile);

	KLIBEXP DiskLZ4Reader(int npartitions, int nbuffersPerFile);

	KLIBEXP bool isEOF(const int id);

	KLIBEXP int readByte(const int id);

	KLIBEXP int64_t readVLong(const int id);

	KLIBEXP int64_t readLong(const int id);

	KLIBEXP const char* readString(const int id, int &sizeTerm);

	KLIBEXP bool availableDiskBuffer();

    virtual bool areNewBuffers(const int id);

    virtual ~DiskLZ4Reader();
};

#endif
