#include <kognac/disklz4reader.h>
#include <boost/filesystem.hpp>

namespace fs = boost::filesystem;

DiskLZ4Reader::DiskLZ4Reader(std::vector<string> &files, int nbuffersPerFile) {
    //Init data structures
    for (int i = 0; i < files.size(); ++i) {
        for (int j = 0; j < nbuffersPerFile; ++j)
            diskbufferpool.push_back(new char [SIZE_COMPRESSED_SEG * 1000]);
        supportstringbuffers.push_back(std::unique_ptr<char[]>(new char[MAX_TERM_SIZE + 2]));
    }
    for (int i = 0; i < files.size(); ++i) {
        FileInfo inf;
        inf.path = files[i];
        inf.eof = fs::file_size(files[i]) > 0;
        inf.buffer = new char[SIZE_SEG];
        inf.sizebuffer = 0;
        inf.pivot = 0;
        this->files.push_back(inf);
    }
    neofs = 0;
    currentFileIdx = 0;
    compressedbuffers = new std::list<BlockToRead>[files.size()];
    m_files = new std::mutex[files.size()];
    cond_files = new std::condition_variable[files.size()];

    //Open all files
    readers = new ifstream[files.size()];
    for (int i = 0; i < files.size(); ++i) {
        readers[i].open(files[i]);
    }

    //Launch reading thread
    currentthread = std::thread(std::bind(&DiskLZ4Reader::run, this));
}

bool DiskLZ4Reader::availableDiskBuffer() {
    return !diskbufferpool.empty();
}

bool DiskLZ4Reader::areNewBuffers(const int id) {
    return !compressedbuffers[id].empty() || files[id].eof;
}

void DiskLZ4Reader::run() {
    while (true) {
        //Read from each file in a round-robin fashion until all files are read
        if (neofs == files.size()) {
            break;
        }

        //Move to the next file
        currentFileIdx = (currentFileIdx + 1) % files.size();
        if (files[currentFileIdx].eof)
            continue;

        //Get a disk buffer
        std::unique_lock<std::mutex> l(m_diskbufferpool);
        cond_diskbufferpool.wait(l, std::bind(&DiskLZ4Reader::availableDiskBuffer, this));
        char *buffer = diskbufferpool.back();
        diskbufferpool.pop_back();
        l.unlock();

        //Read the file and put the content in the disk buffer
        size_t sizeToBeRead = SIZE_COMPRESSED_SEG * 1000;
        readers[currentFileIdx].read(buffer, sizeToBeRead);
        if (!readers[currentFileIdx]) {
            sizeToBeRead = readers[currentFileIdx].gcount();
            assert(sizeToBeRead <= SIZE_COMPRESSED_SEG * 1000);
            assert(readers[currentFileIdx].eof());
            files[currentFileIdx].eof = true;
            readers[currentFileIdx].close();
            neofs++;
        }

        //Put the content of the disk buffer in the blockToRead container
        assert(sizeToBeRead > 0);
        std::unique_lock<std::mutex> lk2(m_files[currentFileIdx]);
        BlockToRead b;
        b.buffer = buffer;
        b.sizebuffer = sizeToBeRead;
        b.pivot = 0;
        compressedbuffers[currentFileIdx].push_back(b);
        lk2.unlock();
        cond_files[currentFileIdx].notify_one();
    }
}

void DiskLZ4Reader::getNewCompressedBuffer(std::unique_lock<std::mutex> &lk,
        const int id) {
    //Here I have already a lock. First I release the buffer at the front
    if (!compressedbuffers[id].empty()) {
        BlockToRead b = compressedbuffers[id].front();
        compressedbuffers[id].pop_front();
        std::unique_lock<std::mutex> lk2(m_diskbufferpool);
        diskbufferpool.push_back(b.buffer);
        lk2.unlock();
        cond_diskbufferpool.notify_one();
    }

    //Then I wait until a new one is available
    cond_files[id].wait(lk, std::bind(&DiskLZ4Reader::areNewBuffers, this, id));
}

bool DiskLZ4Reader::uncompressBuffer(const int id) {
    //Read the compressed stream and extract an uncompressed block from it.

    //Get a lock
    std::unique_lock<std::mutex> lk(m_files[id]);
    //Make sure you wait until there is a new block
    cond_files[id].wait(lk, std::bind(&DiskLZ4Reader::areNewBuffers, this, id));

    if (compressedbuffers[id].empty())
        return false;

    //Init vars
    size_t sizecomprbuffer = compressedbuffers[id].front().sizebuffer;
    char *comprb = compressedbuffers[id].front().buffer;
    size_t pivot = compressedbuffers[id].front().pivot;

    //First I need to read the first 21 bytes to read the header
    int token;
    int compressionMethod;
    int compressedLen;
    int uncompressedLen;
    if (pivot + 21 <= sizecomprbuffer) {
        token = comprb[pivot + 8] & 0xFF;
        compressedLen = Utils::decode_intLE(comprb, pivot + 9);
        uncompressedLen = Utils::decode_intLE(comprb, pivot + 13);
        pivot += 21;
    } else {
        char header[21];
        int remsize = sizecomprbuffer - pivot;
        memcpy(header, comprb + pivot, remsize);

        getNewCompressedBuffer(lk, id);
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
        tmpbuffer = std::unique_ptr<char[]>(new char[SIZE_SEG]);
        int copiedSize = sizecomprbuffer - pivot;
        memcpy(tmpbuffer.get(), comprb + pivot, copiedSize);

        //Get a new buffer
        getNewCompressedBuffer(lk, id);
        sizecomprbuffer = compressedbuffers[id].front().sizebuffer;
        comprb = compressedbuffers[id].front().buffer;
        pivot = 0;

        memcpy(tmpbuffer.get() + copiedSize, comprb, compressedLen - copiedSize);
        pivot = compressedLen - copiedSize;

    }
    compressedbuffers[id].front().pivot = pivot;
    lk.unlock();

    switch (compressionMethod) {
    case 16:
        //Not compressed. I just copy the buffer
        memcpy(f.buffer, startb, uncompressedLen);
        break;
    case 32:
        if (!LZ4_decompress_fast(tmpbuffer.get(), startb, uncompressedLen)) {
            BOOST_LOG_TRIVIAL(error) << "Error in the decompression.";
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
    if (files[id].pivot >= files[id].sizebuffer) {
        bool resp = uncompressBuffer(id);
        assert(resp);
    }
    return files[id].buffer[files[id].pivot++];
}

long DiskLZ4Reader::readLong(const int id) {
    if (files[id].pivot + 8 <= files[id].sizebuffer) {
        long n = Utils::decode_long(files[id].buffer + files[id].pivot);
        files[id].pivot += 8;
        return n;
    } else {
        char header[8];
        int copiedBytes = files[id].sizebuffer - files[id].pivot;
        memcpy(header, files[id].buffer + files[id].pivot, copiedBytes);
        bool resp = uncompressBuffer(id);
        assert(resp);
        memcpy(header + copiedBytes, files[id].buffer, 8 - copiedBytes);
        files[id].pivot = 8 - copiedBytes;
        return Utils::decode_long(header);
    }
}

long DiskLZ4Reader::readVLong(const int id) {
    int shift = 7;
    char b = readByte(id);
    long n = b & 127;
    while (b < 0) {
        b = readByte(id);
        n += (long) (b & 127) << shift;
        shift += 7;
    }
    return n;
}

const char *DiskLZ4Reader::readString(const int id, int &size) {
    size = readVLong(id);

    if (files[id].pivot + size <= files[id].sizebuffer) {
        memcpy(supportstringbuffers[id].get(), files[id].buffer + files[id].pivot, size);
        files[id].pivot += size;
    } else {
        int remSize = files[id].sizebuffer - files[id].pivot;
        memcpy(supportstringbuffers[id].get(), files[id].buffer + files[id].pivot, remSize);
        bool resp = uncompressBuffer(id);
        assert(resp);
        memcpy(supportstringbuffers[id].get() + remSize, files[id].buffer , size - remSize);
        files[id].pivot += size - remSize;
    }
    supportstringbuffers[id][size] = '\0';

    return supportstringbuffers[id].get();
}

DiskLZ4Reader::~DiskLZ4Reader() {
    currentthread.join();
    delete[] compressedbuffers;
    for (int i = 0; i < diskbufferpool.size(); ++i)
        delete[] diskbufferpool[i];
    delete[] readers;
    for (int i = 0; i < files.size(); ++i)
        delete[] files[i].buffer;
    delete[] m_files;
    delete[] cond_files;
}
