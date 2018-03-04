#ifndef _DISK_READER_H
#define _DISK_READER_H

#include <kognac/filereader.h>

#include <mutex>
#include <condition_variable>
#include <vector>

#define DISKREADER_MAX_SIZE 256 * 1024 * 1024

class DiskReader {
    public:
        struct Buffer {
            size_t size;
            size_t maxsize;
            char *b;
            bool gzipped;
        };

    private:
        std::mutex mutex1;
        std::condition_variable cv1;
        std::mutex mutex2;
        std::condition_variable cv2;

        std::vector<FileInfo> *files;
        std::vector<FileInfo>::iterator itr;

        std::vector<Buffer> availablebuffers;
        std::vector<Buffer> readybuffers;

        bool finished;

        std::chrono::duration<double> waitingTime;

        int64_t maxsize;

    public:
        DiskReader(int nbuffers, std::vector<FileInfo> *files);

        //char *getfile(size_t &size, bool &gzipped);

        Buffer getfile();

        //void releasefile(char *file);

        void releasefile(Buffer buffer);

        bool isReady();

        bool isAvailable();

        void run();

        ~DiskReader();
};

#endif
