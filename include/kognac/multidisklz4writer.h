#ifndef _MULTI_DISK_LZ4_WRITER_H
#define _MULTI_DISK_LZ4_WRITER_H

#include <kognac/disklz4writer.h>

#include <vector>

class MultiDiskLZ4Writer : public DiskLZ4Writer {
private:
    struct PartFiles {
        std::vector<string> filestowrite;
        int currentopenedfile;
        PartFiles() {
            currentopenedfile = 0;
        }
    };
    std::vector<PartFiles> files;

    ofstream *streams;
    bool *openedstreams;
    int nopenedstreams;
    const int maxopenedstreams;
    std::list<int> historyopenedfiles;

public:
	KLIBEXP MultiDiskLZ4Writer(std::vector<string> files,
                       int nbuffersPerFile,
                       int maxopenedstreams);

	KLIBEXP void addFileToWrite(int idpart, string file);

	KLIBEXP virtual void run();

	KLIBEXP virtual ~MultiDiskLZ4Writer();
};

#endif
