#include <kognac/disklz4reader.h>
#include <kognac/consts.h>

#include <fstream>
#include <assert.h>

DiskLZ4Reader::DiskLZ4Reader(int npartitions, int nbuffersPerFile) : nbuffersPerFile(nbuffersPerFile) {
    //Init data structures
    for (int i = 0; i < npartitions; ++i) {
        for (int j = 0; j < nbuffersPerFile; ++j)
            diskbufferpool.push_back(new char [SIZE_DISK_BUFFER]);
        supportstringbuffers.push_back(std::unique_ptr<char[]>(new char[MAX_TERM_SIZE + 2]));
    }
    for (int i = 0; i < npartitions; ++i) {
        FileInfo inf;
        inf.buffer = new char[SIZE_SEG];
        inf.sizebuffer = 0;
        inf.pivot = 0;
        this->files.push_back(inf);
    }

    compressedbuffers = new std::list<BlockToRead>[files.size()];
    sCompressedbuffers.resize(files.size());

    m_files = new std::mutex[files.size()];
    cond_files = new std::condition_variable[files.size()];
    time_files = new std::chrono::duration<double>[files.size()];
    time_diskbufferpool = std::chrono::duration<double>::zero();
    time_rawreading = std::chrono::duration<double>::zero();
    for (int i = 0; i < files.size(); ++i) {
        time_files[i] = std::chrono::duration<double>::zero();
    }
}

DiskLZ4Reader::DiskLZ4Reader(string inputfile,
        int npartitions,
        int nbuffersPerFile) : nbuffersPerFile(nbuffersPerFile) {

    this->inputfile = inputfile;
    //Init data structures
    for (int i = 0; i < npartitions; ++i) {
        for (int j = 0; j < nbuffersPerFile; ++j)
            diskbufferpool.push_back(new char [SIZE_DISK_BUFFER]);
        supportstringbuffers.push_back(std::unique_ptr<char[]>(new char[MAX_TERM_SIZE + 2]));
    }
    for (int i = 0; i < npartitions; ++i) {
        FileInfo inf;
        inf.buffer = new char[SIZE_SEG];
        inf.sizebuffer = 0;
        inf.pivot = 0;
        this->files.push_back(inf);
    }
    compressedbuffers = new std::list<BlockToRead>[files.size()];
    sCompressedbuffers.resize(files.size());

    m_files = new std::mutex[files.size()];
    cond_files = new std::condition_variable[files.size()];
    time_files = new std::chrono::duration<double>[files.size()];
    time_diskbufferpool = std::chrono::duration<double>::zero();
    time_rawreading = std::chrono::duration<double>::zero();
    for (int i = 0; i < files.size(); ++i) {
        time_files[i] = std::chrono::duration<double>::zero();
    }

    //Open the input file
    reader.open(inputfile, std::ifstream::binary);

    beginningBlocks.resize(npartitions);
    readBlocks.resize(npartitions);
    //Read the index file
    string idxfile = inputfile + ".idx";
    if (!Utils::exists(idxfile)) {
        LOG(ERRORL) << "The file " << idxfile << " does not exist";
        throw 10;
    }

    ifstream idxreader;
    idxreader.open(idxfile, std::ifstream::binary);
    char buffer[8];
    idxreader.read(buffer, 8);
    int64_t n = Utils::decode_long(buffer);
    assert(n == npartitions);
    for (int i = 0; i < n; ++i) {
        readBlocks[i] = 0;
        idxreader.read(buffer, 8);
        int64_t nblocks = Utils::decode_long(buffer);
        for (int j = 0; j < nblocks; ++j) {
            idxreader.read(buffer, 8);
            int64_t pos = Utils::decode_long(buffer);
            beginningBlocks[i].push_back(pos);
        }
    }
    idxreader.close();

    //Launch reading thread
    currentthread = std::thread(std::bind(&DiskLZ4Reader::run, this));
}



bool DiskLZ4Reader::availableDiskBuffer() {
    return !diskbufferpool.empty();
}

bool DiskLZ4Reader::areNewBuffers(const int id) {
    return !compressedbuffers[id].empty() || readBlocks[id] >= static_cast<int>(beginningBlocks[id].size());
}

void DiskLZ4Reader::run() {
    size_t totalsize = 0;
    char tmpbuffer[8];
    int currentFileIdx = 0;

    while (true) {
        //Get a disk buffer
        std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
        std::unique_lock<std::mutex> l(m_diskbufferpool);
        cond_diskbufferpool.wait(l, std::bind(&DiskLZ4Reader::availableDiskBuffer, this));
        time_diskbufferpool += std::chrono::system_clock::now() - start;

        char *buffer = diskbufferpool.back();
        diskbufferpool.pop_back();
        l.unlock();

        //Read the file and put the content in the disk buffer
        start = std::chrono::system_clock::now();




        //Check whether I can get a buffer from the current file. Otherwise keep looking
        /*int skipped = 0;
          while (skipped < files.size() &&
          beginningBlocks[currentFileIdx].size()
          <= readBlocks[currentFileIdx]) {
          currentFileIdx = (currentFileIdx + 1) % files.size();
          skipped++;
          }
          if (skipped == files.size()) {
          diskbufferpool.push_back(buffer);
          break; //It means I read all possible blocks
          }*/

        bool found = false;
        int firstPotentialPart = -1;
        int skipped = 0;
        for(int i = 0; i < files.size(); ++i) {
            if (static_cast<int>(beginningBlocks[currentFileIdx].size()) <= readBlocks[currentFileIdx]) {
                skipped++;
                currentFileIdx = (currentFileIdx + 1) % files.size();
            } else if (sCompressedbuffers[currentFileIdx] >= nbuffersPerFile) {
                firstPotentialPart = currentFileIdx;
                currentFileIdx = (currentFileIdx + 1) % files.size();
            } else {
                found = true;
                break;
            }
        }
        if (skipped == files.size()) {
            LOG(DEBUGL) << "Exiting ...";
            diskbufferpool.push_back(buffer);
            break;
        } else if (!found) {
            if (firstPotentialPart == -1) {
                LOG(ERRORL) << "FirstPotentialPer == -1";
                throw 10;
            }
            currentFileIdx = firstPotentialPart;
        }

        int64_t blocknumber = readBlocks[currentFileIdx];
        assert(blocknumber < static_cast<int>(beginningBlocks[currentFileIdx].size()));
        int64_t position = beginningBlocks[currentFileIdx][blocknumber];
        //LOG(DEBUGL) << "Read block " << blocknumber << " for file " << currentFileIdx << " at position " << position;

        reader.seekg(position);
        reader.read(tmpbuffer, 8);
#ifdef DEBUG
        int fileidx = Utils::decode_int(tmpbuffer);
        assert(fileidx == currentFileIdx);
#endif
        size_t sizeToBeRead = Utils::decode_int(tmpbuffer+4);
        totalsize += sizeToBeRead + 8;
        reader.read(buffer, sizeToBeRead);

        time_rawreading += std::chrono::system_clock::now() - start;

        //Put the content of the disk buffer in the blockToRead container
        assert(sizeToBeRead > 0);
        start = std::chrono::system_clock::now();
        std::unique_lock<std::mutex> lk2(m_files[currentFileIdx]);
        time_files[currentFileIdx] += std::chrono::system_clock::now() - start;

        BlockToRead b;
        b.buffer = buffer;
        b.sizebuffer = sizeToBeRead;
        b.pivot = 0;
        compressedbuffers[currentFileIdx].push_back(b);
        sCompressedbuffers[currentFileIdx]++;

        readBlocks[currentFileIdx]++;
        lk2.unlock();
        cond_files[currentFileIdx].notify_one();

        //Move to the next file/block
        currentFileIdx = (currentFileIdx + 1) % files.size();

        //reader.read(tmpbuffer, 4);
        //currentFileIdx = Utils::decode_int(tmpbuffer);

        //LOG(DEBUGL) << "READING TIME all data from disk " << time_rawreading.count()  << "sec. Last buffer size = " << sizeToBeRead << " Time diskbufferpool " << time_diskbufferpool.count() << "sec.";
    }

    reader.close();
    //LOG(DEBUGL) << "Finished reading the input file";

    //Notify all attached files that might be waiting that there is nothing else to read
    for (int i = 0; i < files.size(); ++i)
        cond_files[i].notify_one();
}

bool DiskLZ4Reader::getNewCompressedBuffer(std::unique_lock<std::mutex> &lk,
        const int id) {
    //Here I have already a lock. First I release the buffer at the front
    if (!compressedbuffers[id].empty()) {
        BlockToRead b = compressedbuffers[id].front();
        compressedbuffers[id].pop_front();
        sCompressedbuffers[id]--;

        std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
        std::unique_lock<std::mutex> lk2(m_diskbufferpool);
        time_diskbufferpool += std::chrono::system_clock::now() - start;

        diskbufferpool.push_back(b.buffer);
        lk2.unlock();
        cond_diskbufferpool.notify_one();
    }

    //Then I wait until a new one is available
    cond_files[id].wait(lk, std::bind(&DiskLZ4Reader::areNewBuffers, this, id));
    return !compressedbuffers[id].empty();
}

bool DiskLZ4Reader::uncompressBuffer(const int id) {
    //Get a lock
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    std::unique_lock<std::mutex> lk(m_files[id]);
    //Make sure you wait until there is a new block
    cond_files[id].wait(lk, std::bind(&DiskLZ4Reader::areNewBuffers, this, id));
    time_files[id] += std::chrono::system_clock::now() - start;

    if (compressedbuffers[id].empty()) {
        return false;
    }

    if (compressedbuffers[id].front().pivot ==
            compressedbuffers[id].front().sizebuffer) {
        if (!getNewCompressedBuffer(lk, id)) {
            return false;
        }
    }

    //Init vars
    size_t sizecomprbuffer = compressedbuffers[id].front().sizebuffer;
    char *comprb = compressedbuffers[id].front().buffer;
    size_t pivot = compressedbuffers[id].front().pivot;
    assert(comprb != NULL);

    //First I need to read the first 21 bytes to read the header
    int token;
    int compressionMethod;
    int compressedLen;
    int uncompressedLen = -1;
    if (pivot + 21 <= sizecomprbuffer) {
        token = comprb[pivot + 8] & 0xFF;
        compressedLen = Utils::decode_intLE(comprb, static_cast<int>(pivot + 9));
        uncompressedLen = Utils::decode_intLE(comprb, static_cast<int>(pivot + 13));
        pivot += 21;
    } else {
        char header[21];
        size_t remsize = sizecomprbuffer - pivot;
        memcpy(header, comprb + pivot, remsize);

        if (!getNewCompressedBuffer(lk, id))
            throw 10;

        sizecomprbuffer = compressedbuffers[id].front().sizebuffer;
        comprb = compressedbuffers[id].front().buffer;
        pivot = 0;

        //Get the remaining
        memcpy(header + remsize, comprb, 21 - remsize);
        pivot += 21 - remsize;
        token = header[8] & 0xFF;
        compressedLen = Utils::decode_intLE(header, 9);
        uncompressedLen = Utils::decode_intLE(header, 13);
    }
    compressionMethod = token & 0xF0;

    //Uncompress chunk
    FileInfo &f = files[id];

    std::unique_ptr<char[]> tmpbuffer;
    char *startb;

    if (pivot + compressedLen <= sizecomprbuffer) {
        startb = comprb + pivot;
        pivot += compressedLen;
    } else {
        tmpbuffer = std::unique_ptr<char[]>(new char[SIZE_COMPRESSED_SEG]);
        size_t copiedSize = sizecomprbuffer - pivot;
        memcpy(tmpbuffer.get(), comprb + pivot, copiedSize);

        //Get a new buffer
        if (!getNewCompressedBuffer(lk, id)) {
            throw 10;
        }

        sizecomprbuffer = compressedbuffers[id].front().sizebuffer;
        comprb = compressedbuffers[id].front().buffer;
        pivot = 0;

        memcpy(tmpbuffer.get() + copiedSize, comprb, compressedLen - copiedSize);
        pivot = compressedLen - copiedSize;
        startb = tmpbuffer.get();

    }
    compressedbuffers[id].front().pivot = pivot;
    lk.unlock();

    switch (compressionMethod) {
        case 16:
            //Not compressed. I just copy the buffer
            memcpy(f.buffer, startb, uncompressedLen);
            break;
        case 32:
            if (LZ4_decompress_safe(startb, f.buffer, compressedLen, uncompressedLen) < 0) {
                LOG(ERRORL) << "Error in the decompression.";
                throw 10;
            }
            break;
        default:
            throw 10;
    }
    f.sizebuffer = uncompressedLen;
    f.pivot = 0;
    return true;
}

bool DiskLZ4Reader::isEOF(const int id) {
    if (files[id].pivot < files[id].sizebuffer)
        return false;
    bool resp = uncompressBuffer(id);
    return !resp;
}

int DiskLZ4Reader::readByte(const int id) {
    assert(id < files.size());
    if (files[id].pivot >= files[id].sizebuffer) {
        uncompressBuffer(id);
    }
    return files[id].buffer[files[id].pivot++];
}

int64_t DiskLZ4Reader::readLong(const int id) {
    if (files[id].pivot + 8 <= files[id].sizebuffer) {
        int64_t n = Utils::decode_long(files[id].buffer + files[id].pivot);
        files[id].pivot += 8;
        return n;
    } else {
        char header[8];
        size_t copiedBytes = files[id].sizebuffer - files[id].pivot;
        // memcpy(header, files[id].buffer + files[id].pivot, copiedBytes);
        for (int i = 0; i < copiedBytes; i++) {
            header[i] = files[id].buffer[files[id].pivot + i];
        }
#ifdef DEBUG
        bool resp = uncompressBuffer(id);
        assert(resp);
#else
        uncompressBuffer(id);
#endif
        // memcpy(header + copiedBytes, files[id].buffer, 8 - copiedBytes);
        for (int i = copiedBytes; i < 8; i++) {
            header[i] = files[id].buffer[i - copiedBytes];
        }
        files[id].pivot = 8 - copiedBytes;
        return Utils::decode_long(header);
    }
}

int64_t DiskLZ4Reader::readVLong(const int id) {
    int b = readByte(id);
    int64_t n = b & 127;
    if ((b & 128) != 0) {
        int shift = 7;
        for (;;) {
            b = readByte(id);
            n += ((int64_t) (b & 127)) << shift;
            if ((b & 128) == 0) {
                break;
            }
            shift += 7;
        }
    }
    return n;
}

const char *DiskLZ4Reader::readString(const int id, int &size) {
    size = static_cast<int>(readVLong(id));

    if (files[id].pivot + size <= files[id].sizebuffer) {
        memcpy(supportstringbuffers[id].get(), files[id].buffer + files[id].pivot, size);
        files[id].pivot += size;
    } else {
        size_t remSize = files[id].sizebuffer - files[id].pivot;
        memcpy(supportstringbuffers[id].get(), files[id].buffer + files[id].pivot, remSize);
#ifdef DEBUG
        bool resp = uncompressBuffer(id);
        assert(resp);
#else
        uncompressBuffer(id);
#endif
        memcpy(supportstringbuffers[id].get() + remSize, files[id].buffer , size - remSize);
        files[id].pivot += size - remSize;
    }
    supportstringbuffers[id][size] = '\0';

    return supportstringbuffers[id].get();
}

DiskLZ4Reader::~DiskLZ4Reader() {
    if (currentthread.joinable()) {
        currentthread.join();
    }

    double avg = 0;
    for (int i = 0; i < files.size(); ++i) {
        avg += time_files[i].count();
    }

    LOG(DEBUGL) << "Time reading all data from disk " <<
        time_rawreading.count()  << "sec. Time waiting lock m_diskbufferpool " <<
        time_diskbufferpool.count() << "sec. Time (avg) waiting locks files " <<
        avg / files.size() << "sec.";

    for (int i = 0; i < files.size(); ++i) {
        m_files[i].lock();
        m_files[i].unlock();
    }

    delete[] compressedbuffers;
    for (int i = 0; i < diskbufferpool.size(); ++i)
        delete[] diskbufferpool[i];
    //delete[] readers;
    for (int i = 0; i < files.size(); ++i)
        delete[] files[i].buffer;

    delete[] m_files;
    delete[] cond_files;
    delete[] time_files;
}
