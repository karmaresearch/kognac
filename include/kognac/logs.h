#ifndef _LOG_H
#define _LOG_H

#include <fstream>
#include <thread>
#include <sstream>
#include <iomanip>
#include <iostream>

#define TRACEL 0
#define DEBUGL 1
#define INFOL 2
#define WARNL 3
#define ERRORL 4

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

        static void setMinLevel(int level) {
            Logger::minLevel = level;
        }

        Logger& operator << (const char *msg) {
            if (!silent) {
                if (first) {
                    auto t = std::time(NULL);
                    auto tm = *std::localtime(&t);
                    std::stringstream ss;
		    char tmpbuf[128];
		    if(0 < strftime(tmpbuf, sizeof(tmpbuf), "[%T%z %F] ", &tm)) {
			ss << "[0x" << std::hex << std::hash<std::thread::id>()(std::this_thread::get_id()) << " ";
			ss << tmpbuf << "] ";;
		    }
                    switch (level) {
                        case TRACEL:
                            ss << "TRACE ";
                            break;
                        case DEBUGL:
                            ss << "DEBUG ";
                            break;
                        case INFOL:
                            ss << "INFO ";
                            break;
                        case WARNL:
                            ss << "WARN ";
                            break;
                        case ERRORL:
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
