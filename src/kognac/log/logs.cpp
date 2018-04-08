#include <kognac/logs.h>

int Logger::minLevel = TRACEL;
std::mutex Logger::mutex;
std::unique_ptr<Logger::FileLogger> Logger::file;
