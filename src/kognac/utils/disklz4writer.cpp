#include <kognac/lz4io.h>
#include <kognac/disklz4writer.h>

DiskLZ4Writer::DiskLZ4Writer(int npartitions, int nbuffersPerFile) : npartitions(npartitions) {
    //Create a number of compressed buffers
    for (int i = 0; i < npartitions; i++) {
        //Create 10 buffers for each file
        parentbuffers.push_back(new char[SIZE_DISK_BUFFER * nbuffersPerFile]);
        for (int j = 0; j < nbuffersPerFile; ++j) {
            buffers.push_back(parentbuffers.back() + SIZE_DISK_BUFFER * j);
        }
    }
    fileinfo.resize(npartitions);
    nterminated = 0;
    blocksToWrite = new std::list<BlockToWrite>[npartitions];
    addedBlocksToWrite = 0;
    currentWriteFileID = 0;
    time_rawwriting = std::chrono::duration<double>::zero();
    time_waitingwriting = std::chrono::duration<double>::zero();
    time_waitingbuffer = std::chrono::duration<double>::zero();
    processStarted = false;
}

DiskLZ4Writer::DiskLZ4Writer(string file,
        int npartitions,
        int nbuffersPerFile) :
    DiskLZ4Writer(npartitions, nbuffersPerFile) {
        inputfile = file;
        stream.open(file, std::ofstream::binary);
        currentthread = thread(std::bind(&DiskLZ4Writer::run, this));
        processStarted = true;
        startpositions.resize(npartitions);
    }

void DiskLZ4Writer::writeByte(const int id, const int value) {
    assert(id < npartitions);
    char *buffer = fileinfo[id].buffer;
    if (fileinfo[id].sizebuffer == SIZE_SEG) {
        compressAndQueue(id);
        fileinfo[id].sizebuffer = 0;
    }
    buffer[fileinfo[id].sizebuffer++] = value;
}

void DiskLZ4Writer::writeVLong(const int id, const int64_t value) {
    int64_t n = value;
    // Note: assumes that the value to be written is >= 0.
    while (n >= 128) {
        writeByte(id, ((int) n & 127) + 128);
        n >>= 7;
    }
    writeByte(id, ((int) n) & 127);
    /*
    if (n < 128) { // One byte is enough
        writeByte(id, static_cast<int>(n));
        return;
    } else {
        int bytesToStore = 64 - Utils::numberOfLeadingZeros((uint64_t) n);
        while (bytesToStore > 7) {
            writeByte(id, (n & 127) + 128);
            n >>= 7;
            bytesToStore -= 7;
        }
        writeByte(id, n & 127);
    }
    */
}

void DiskLZ4Writer::writeLong(const int id, const int64_t value) {
    assert(id < npartitions);
    char *buffer = fileinfo[id].buffer;
    if (fileinfo[id].sizebuffer + 8 <= SIZE_SEG) {
        Utils::encode_long(buffer, static_cast<int>(fileinfo[id].sizebuffer), value);
        fileinfo[id].sizebuffer += 8;
    } else {
        char supportBuffer[8];
        Utils::encode_long(supportBuffer, 0, value);
        int i = 0;
        for (; i < 8 && fileinfo[id].sizebuffer < SIZE_SEG; ++i) {
            buffer[fileinfo[id].sizebuffer++] = supportBuffer[i];
        }
        compressAndQueue(id);
        fileinfo[id].sizebuffer = 0;
        for (; i < 8 && fileinfo[id].sizebuffer < SIZE_SEG; ++i) {
            buffer[fileinfo[id].sizebuffer++] = supportBuffer[i];
        }
    }

}

void DiskLZ4Writer::writeString(const int id, const char *bytes,
        const size_t length) {
    writeVLong(id, length);
    writeRawArray(id, bytes, length);
}

void DiskLZ4Writer::writeRawArray(const int id, const char *bytes,
        const size_t length) {
    size_t len = length;
    assert(id < npartitions);
    char *buffer = fileinfo[id].buffer;
    if (fileinfo[id].sizebuffer + len <= SIZE_SEG) {
        memcpy(buffer + fileinfo[id].sizebuffer, bytes, len);
    } else {
        size_t remSize = SIZE_SEG - fileinfo[id].sizebuffer;
        memcpy(buffer + fileinfo[id].sizebuffer, bytes, remSize);
        fileinfo[id].sizebuffer += remSize;
        compressAndQueue(id);
        fileinfo[id].sizebuffer = 0;
        len = len - remSize;
        memcpy(buffer, bytes + remSize, len);
    }
    fileinfo[id].sizebuffer += len;

}

void DiskLZ4Writer::writeShort(const int id, const int value) {
    assert(id < npartitions);
    char *buffer = fileinfo[id].buffer;
    if (fileinfo[id].sizebuffer == SIZE_SEG) {
        compressAndQueue(id);
        fileinfo[id].sizebuffer = 0;
    } else if (fileinfo[id].sizebuffer == SIZE_SEG - 1) {
        char supportBuffer[2];
        Utils::encode_short(supportBuffer, value);
        writeByte(id, supportBuffer[0]);
        writeByte(id, supportBuffer[1]);
        return;
    }
    Utils::encode_short(buffer + fileinfo[id].sizebuffer, value);
    fileinfo[id].sizebuffer += 2;
}

void DiskLZ4Writer::flush(const int id) {
    //Write down the last buffer
    size_t sizebuffer = fileinfo[id].sizebuffer;
    if (sizebuffer > 0) {
        compressAndQueue(id);
        fileinfo[id].sizebuffer = 0;
    }

    //Flush the compressed buffer on disk
    if (fileinfo[id].pivotCompressedBuffer > 0) {
        BlockToWrite b;
        b.buffer = fileinfo[id].compressedbuffer;
        b.sizebuffer = fileinfo[id].pivotCompressedBuffer;
        b.idxfile = fileinfo[id].idxfiletowrite;
        b.idpart = id;

        //Copy in the writing queue
        std::unique_lock<std::mutex> lk2(mutexBlockToWrite);
        blocksToWrite[id].push_back(b);
        addedBlocksToWrite++;
        lk2.unlock();
    }
}

void DiskLZ4Writer::setTerminated(const int id) {
    flush(id);
    mutexTerminated.lock();
    nterminated++;
    mutexTerminated.unlock();
    cvBlockToWrite.notify_one();
}

void DiskLZ4Writer::compressAndQueue(const int id) {
    //Get a compressed buffer
    FileInfo &file = fileinfo[id];
    char *buffer = file.compressedbuffer + file.pivotCompressedBuffer;
    if (file.compressedbuffer == NULL ||
            file.pivotCompressedBuffer +
            SIZE_COMPRESSED_SEG >= SIZE_DISK_BUFFER) {

        //flush current buffer
        if (file.compressedbuffer != NULL) {
            BlockToWrite b;
            b.buffer = file.compressedbuffer;
            b.sizebuffer = file.pivotCompressedBuffer;
            b.idxfile = file.idxfiletowrite;
            b.idpart = id;

            //Copy in the writing queue
            std::unique_lock<std::mutex> lk2(mutexBlockToWrite);
            blocksToWrite[id].push_back(b);
            addedBlocksToWrite++;
            lk2.unlock();
            cvBlockToWrite.notify_one();
        }

        //Get a new buffer
        auto start = std::chrono::system_clock::now();
        std::unique_lock<std::mutex> lk(mutexAvailableBuffer);
        cvAvailableBuffer.wait(lk, std::bind(&DiskLZ4Writer::areAvailableBuffers, this));
        auto sec = std::chrono::system_clock::now() - start;
        time_waitingbuffer += sec;

        assert(buffers.size() > 0);
        char *newbuffer = buffers.back();
        buffers.pop_back();
        lk.unlock();

        file.compressedbuffer = buffer = newbuffer;
        file.pivotCompressedBuffer = 0;
        assert(buffer != NULL);
    }

    //Compress the buffer
    //First 8 bytes is LZOBlock.
    //Then there is a token which has encoded in the 0xF0 bits
    //the type of compression.
    memset(buffer, 0, 21);
#if defined(WIN32)
    strcpy_s(buffer, strlen("LZOBLOCK") + 1, "LZOBLOCK");
#else
    strncpy(buffer, "LZOBLOCK", strlen("LZOBLOCK") + 1);
#endif
    buffer[8] = 32;

    //Then there is the compressed size but I will write it later...
    //... and finally the uncompressed size
    Utils::encode_intLE(buffer, 13, static_cast<int>(file.sizebuffer));
#if LZ4_VERSION_MAJOR > 1 || LZ4_VERSION_MINOR > 2 || (LZ4_VERSION_MINOR == 2 && LZ4_VERSION_RELEASE >= 9)
    // LZ4_compress_default does not exist before lz4 version 129.
    const size_t compressedSize = LZ4_compress_default(file.buffer, buffer + 21, static_cast<int>(file.sizebuffer), SIZE_COMPRESSED_SEG - 21);
#else
    const int compressedSize = LZ4_compress(file.buffer, buffer + 21, file.sizebuffer);
#endif
    if (compressedSize == 0 || compressedSize > SIZE_COMPRESSED_SEG - 21) {
        LOG(ERRORL) << "I could not compress in the given buffer";
        throw 10;
    }
    Utils::encode_intLE(buffer, 9, static_cast<int>(compressedSize));
    file.pivotCompressedBuffer += compressedSize + 21;
}

bool DiskLZ4Writer::areBlocksToWrite() {
    return addedBlocksToWrite > 0 || nterminated == npartitions;
}

bool DiskLZ4Writer::areAvailableBuffers() {
    return !buffers.empty();
}

void DiskLZ4Writer::run() {
    while (true) {
        std::list<BlockToWrite> blocks;

        auto start = std::chrono::system_clock::now();
        std::unique_lock<std::mutex> lk(mutexBlockToWrite);
        cvBlockToWrite.wait(lk, std::bind(&DiskLZ4Writer::areBlocksToWrite, this));
        time_waitingwriting += std::chrono::system_clock::now() - start;

        if (addedBlocksToWrite > 0) {
            //Search the first non-empty file to write
            int nextid = (currentWriteFileID + 1) % npartitions;
            while (blocksToWrite[nextid].empty()) {
                nextid = (nextid + 1) % npartitions;
            }
            currentWriteFileID = nextid;

            blocksToWrite[currentWriteFileID].swap(blocks);
            addedBlocksToWrite -= blocks.size();
            lk.unlock();
        } else { //Exit...
            lk.unlock();
            break;
        }

        start = std::chrono::system_clock::now();
        auto it = blocks.begin();
        while (it != blocks.end()) {
            startpositions[it->idpart].push_back(stream.tellp());
            char el[4];
            Utils::encode_int(el, it->idpart);
            stream.write(el, 4);
            Utils::encode_int(el, static_cast<int>(it->sizebuffer));
            stream.write(el, 4);
            stream.write(it->buffer, it->sizebuffer);
            it++;
        }
        time_rawwriting += std::chrono::system_clock::now() - start;

        //LOG(DEBUGL) << "WRITING TIME " << time_rawwriting.count() << "ec. Waitingwriting " << time_waitingwriting.count() << "sec." << " Waiting buffer " << time_waitingbuffer.count() << "sec.";

        //Return the buffer so that it can be reused
        unique_lock<std::mutex> lk2(mutexAvailableBuffer);
        it = blocks.begin();
        while (it != blocks.end()) {
            buffers.push_back(it->buffer);
            it++;
        }
        lk2.unlock();
        cvAvailableBuffer.notify_one();
    }
    stream.close();

    //write down the beginning of the blocks for each file
    stream.open(inputfile + string(".idx"), std::ofstream::binary);
    char buffer[8];
    Utils::encode_long(buffer, startpositions.size());
    stream.write(buffer, 8);
    for (int i = 0; i < startpositions.size(); ++i) {
        //LOG(DEBUGL) << "The number of blocks in partition "
        //                         << i
        //                         << " is " << startpositions[i].size();
        Utils::encode_long(buffer, startpositions[i].size());
        stream.write(buffer, 8);
        for (int j = 0; j < startpositions[i].size(); ++j) {
            Utils::encode_long(buffer, startpositions[i][j]);
            stream.write(buffer, 8);
        }
    }
    stream.close();
}

DiskLZ4Writer::~DiskLZ4Writer() {
    if (processStarted)
        currentthread.join();
    processStarted = false;

    LOG(DEBUGL) << "Time writing all data from disk " << time_rawwriting.count()  << "sec. Time waiting writing " << time_waitingwriting.count() << "sec. Time waiting buffer " << time_waitingbuffer.count() << "sec.";

    for (int i = 0; i < parentbuffers.size(); ++i)
        delete[] parentbuffers[i];

    delete[] blocksToWrite;
}
