#if defined(_WIN32)
#define LOG_SHARED_LIB
#endif
#include <kognac/logs.h>

int Logger::minLevel = TRACEL;
std::mutex Logger::mutex;
std::unique_ptr<Logger::FileLogger> Logger::file;
