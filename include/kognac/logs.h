#ifndef _LOG_H
#define _LOG_H

#include <fstream>
#include <thread>
#include <sstream>
#include <iomanip>
#include <iostream>

#define TRACE 0
#define DEBUG 1
#define INFO 2
#define WARN 3
#define ERROR 4

class Logger {
    private:
        static int minLevel;

        const bool silent;
        const int level;

        Logger(bool silent, int level) : silent(silent), level(level) {
        }

    public:
        static Logger log(int level) {
            return Logger(level < minLevel, level);
        }

        Logger& operator << (const char *msg) {
            if (!silent) {
                auto t = std::time(NULL);
                auto tm = *std::localtime(&t);
                //std::time_t tt = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
                std::stringstream ss;
                ss << "[0x" << std::hex << std::hash<std::thread::id>()(std::this_thread::get_id()) << " ";
                //ss << ctime(&tt) << "] ";
                ss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S") << "] ";
                switch (level) {
                    case TRACE:
                        ss << "TRACE ";
                        break;
                    case DEBUG:
                        ss << "DEBUG ";
                        break;
                    case INFO:
                        ss << "INFO ";
                        break;
                    case WARN:
                        ss << "WARN ";
                        break;
                    case ERROR:
                        ss << "ERROR ";
                        break;
                };
                std::cerr << ss.str() << msg << std::endl;
            }
            return *this;
        }

        Logger& operator << (unsigned long n) {
            std::string s = std::to_string(n);
            return *this << s.c_str();
        }

        Logger& operator << (std::string s) {
            return *this << s.c_str();
        }
};

#define LOG(X) Logger::log(X)

#endif
