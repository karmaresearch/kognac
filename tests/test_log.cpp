#include <kognac/logs.h>
#include <boost/log/trivial.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>

using namespace std;

int main(int argc, const char** argv) {
    Logger::setMinLevel(DEBUGL);
    Logger::logToFile("file.log");
    LOG(DEBUGL) << "Hi!";
}
