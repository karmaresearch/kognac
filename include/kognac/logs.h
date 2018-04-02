#ifndef _LOG_H
#define _LOG_H

#include <kognac/consts.h>

#include <fstream>
#include <thread>
#include <sstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <time.h>

#define TRACEL 0
#define DEBUGL 1
#define INFOL 2
#define WARNL 3
#define ERRORL 4

class Logger {
    private:
        class FileLogger {
            private:
                std::ofstream ofs;

            public:
                FileLogger(std::string file) {
                    ofs.open(file);
                }

		bool fail() {
		    return ofs.fail();
		}

                void write(std::string &s) {
                    ofs << s << std::endl;
                }

                ~FileLogger() {
                    ofs.close();
                }
        };

        KLIBEXP static int minLevel;
        KLIBEXP static std::mutex mutex;
        KLIBEXP static std::unique_ptr<FileLogger> file;

    private:
        const int level;

        std::string toprint;
        bool first;

        Logger(int level) : level(level), first(true) {
        }

    public:
        static Logger log(int level) {
            return Logger(level);
        }

        static int getMinLevel() {
            return Logger::minLevel;
        }

        static bool check(int level) {
            return level >= Logger::minLevel;
        }

        static void setMinLevel(int level) {
            Logger::minLevel = level;
        }

        static void logToFile(std::string filepath) {
	    if (filepath == "") {
		Logger::file = NULL;
	    } else {
		Logger::file = std::unique_ptr<FileLogger>(new FileLogger(filepath));
		if (Logger::file->fail()) {
		    log(WARNL) << "Could not open " << filepath;
		    Logger::file = NULL;
		}
	    }
        }

        Logger& operator << (const char *msg) {
            if (first) {
                auto t = std::time(NULL);
                struct tm localtm;
#if defined(WIN32)
                localtime_s(&localtm, &t);
#else
                localtime_r(&t, &localtm);
#endif
                std::stringstream ss;
                char tmpbuf[128];
                if(0 < strftime(tmpbuf, sizeof(tmpbuf), "%Y-%m-%d %H:%M:%S", &localtm)) {
                    ss << "[0x" << std::hex << std::hash<std::thread::id>()(std::this_thread::get_id());
                    while (ss.tellp() < 20) {
                        ss << " ";
                    }
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
            return *this;
        }

        Logger& operator << (unsigned long long n) {
            std::string s = std::to_string(n);
            return *this << s.c_str();
        }

        Logger& operator << (unsigned long n) {
            std::string s = std::to_string(n);
            return *this << s.c_str();
        }

        Logger& operator << (unsigned n) {
            std::string s = std::to_string(n);
            return *this << s.c_str();
        }

        Logger& operator << (double n) {
            std::string s = std::to_string(n);
            return *this << s.c_str();
        }

        Logger& operator << (float n) {
            std::string s = std::to_string(n);
            return *this << s.c_str();
        }

        Logger& operator << (long long n) {
            std::string s = std::to_string(n);
            return *this << s.c_str();
        }

        Logger& operator << (long n) {
            std::string s = std::to_string(n);
            return *this << s.c_str();
        }

        Logger& operator << (int n) {
            std::string s = std::to_string(n);
            return *this << s.c_str();
        }

        Logger& operator << (std::string s) {
            return *this << s.c_str();
        }

        ~Logger() {
            std::lock_guard<std::mutex> lock(Logger::mutex);
            if (Logger::file) {
                Logger::file->write(toprint);
            } else {
		std::cerr << toprint << std::endl;
	    }
        }
};

#define LOG(X) if (Logger::check(X)) Logger::log(X)

#endif
