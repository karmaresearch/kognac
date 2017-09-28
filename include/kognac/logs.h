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

        std::string toprint;
        bool first;

        Logger(bool silent, int level) : silent(silent), level(level), first(true) {
        }

    public:
        static Logger log(int level) {
            return Logger(level < minLevel, level);
        }

        Logger& operator << (const char *msg) {
            if (!silent) {
                if (first) {
                    auto t = std::time(NULL);
                    auto tm = *std::localtime(&t);
                    std::stringstream ss;
                    ss << "[0x" << std::hex << std::hash<std::thread::id>()(std::this_thread::get_id()) << " ";
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
                    first = false;
                    toprint = ss.str() + " " + std::string(msg);
                } else {
                    toprint += std::string(msg);
                }
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

        ~Logger() {
            if (!silent)
                std::cerr << toprint << std::endl;
        }
};

#define LOG(X) Logger::log(X)

#endif
