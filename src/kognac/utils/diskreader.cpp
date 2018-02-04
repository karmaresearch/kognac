#include <assert.h>
#include <kognac/diskreader.h>
#include <kognac/logs.h>
#include <kognac/utils.h>

DiskReader::DiskReader(int nbuffers, std::vector<FileInfo> *files) {
    this->files = files;
    itr = files->begin();
    finished = false;
    maxsize = 0;
    for (int i = 0; i < files->size(); ++i) {
        if (files->at(i).size > maxsize)
            maxsize = files->at(i).size;
    }
    //maxsize += 32 * 1024 + maxsize * 0.1; //max size + add a 10%
    if (maxsize > DISKREADER_MAX_SIZE) { //Limit the max size to 128MB
        maxsize = DISKREADER_MAX_SIZE;
    }
    LOG(DEBUGL) << "Max size=" << maxsize;

    for (int i = 0; i < nbuffers; ++i) {
        Buffer buffer;
        buffer.size = 0;
        buffer.maxsize = maxsize;
        buffer.b = new char[maxsize];
        memset(buffer.b, 0, sizeof(char) * maxsize);
        availablebuffers.push_back(buffer);
    }
    waitingTime = std::chrono::duration<double>::zero();
}

bool DiskReader::isReady() {
    return !this->readybuffers.empty() || finished;
}

bool DiskReader::isAvailable() {
    return !availablebuffers.empty();
}

DiskReader::Buffer DiskReader::getfile() {
    std::unique_lock<std::mutex> lk(mutex1);
    cv1.wait(lk, std::bind(&DiskReader::isReady, this));

    Buffer info;
    bool gotit = false;
    if (!readybuffers.empty()) {
        gotit = true;
        info = readybuffers.back();
        readybuffers.pop_back();
    }

    lk.unlock();
    cv1.notify_one();

    if (gotit) {
        return info;
    } else {
        info.size = 0;
        info.gzipped = false;
        info.b = NULL;
        return info;
    }
}

void DiskReader::releasefile(DiskReader::Buffer buffer) {
    if (buffer.maxsize > maxsize) {
        //This buffer was too large. Reduce it to free some memory
        delete[] buffer.b;
        buffer.b = new char[maxsize];
        buffer.maxsize = maxsize;
        buffer.size = 0;
    }
    std::unique_lock<std::mutex> lk(mutex2);
    availablebuffers.push_back(buffer);
    lk.unlock();
    cv2.notify_one();
}

void DiskReader::run() {
    ifstream ifs;
    size_t count = 0;
    while (itr != files->end()) {
        bool gzipped = false;
        if (Utils::hasExtension(itr->path) && Utils::extension(itr->path) == string(".gz")) {
            gzipped = true;
        }
        LOG(DEBUGL) << "Path is " << itr->path << ", gzipped = " << gzipped;

        //Is there an available buffer that I can use?
        Buffer buffer;
        {
            std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
            std::unique_lock<std::mutex> lk(mutex2);
            cv2.wait(lk, std::bind(&DiskReader::isAvailable, this));
            waitingTime += std::chrono::system_clock::now() - start;
            buffer = availablebuffers.back();
            availablebuffers.pop_back();
            lk.unlock();
        }

        //Read a file and copy it in buffer
        buffer.gzipped = gzipped;
        ifs.open(itr->path);
        long readSize = itr->size;
        if (readSize > buffer.maxsize) {
            //The buffer is too small. Must create a bigger one
            delete[] buffer.b;
            buffer.b = new char[readSize];
            buffer.maxsize = readSize;
        }

        if (!gzipped) {
            if (itr->start > 0) {
                ifs.seekg(itr->start);
                while (!ifs.eof() && ifs.get() != '\n') {
                    readSize--;
                };
                readSize--;
            }
            if (readSize <= 0) {
                //No line was found within the allocated chunk
                readSize = 0;
            } else {
                ifs.read(buffer.b, readSize);
                assert(ifs);
                //Keep reading until the final '\n'
                while (!ifs.eof()) {
                    char b = ifs.get();
                    if (b == -1) {
                        break; //magic value
                    }
                    if (readSize > maxsize) {
                        LOG(ERRORL) << "Buffers are too small. Must fix this";
                        throw 10;
                    }
                    buffer.b[readSize++] = b;
                    if (b == '\n')
                        break;
                };
            }
        } else {
            ifs.read(buffer.b, readSize);
        }
        buffer.size = readSize;
        ifs.close();
        count++;
        {
            std::lock_guard<std::mutex> lk(mutex1);
            readybuffers.push_back(buffer);
        }

        //Alert one waiting thread that there is one new buffer
        cv1.notify_one();
        itr++;
    }

    {
        std::lock_guard<std::mutex> lk(mutex);
        finished = true;
    }
    cv1.notify_all();

}

DiskReader::~DiskReader() {
    for (int i = 0; i < availablebuffers.size(); ++i) {
        delete[] availablebuffers[i].b;
    }
    availablebuffers.clear();
}
