#include <kognac/progargs.h>

#include <algorithm>

template<>
bool ProgramArgs::AbsArg::check<string>(string s) {
    return true;
}
template<>
bool ProgramArgs::AbsArg::check<int>(string s) {
    return !s.empty() && std::find_if(s.begin(),
            s.end(), [](char c) { return !(std::isdigit(c) || c == '-'); }) == s.end();
}
template<>
bool ProgramArgs::AbsArg::check<long>(string s) {
    return !s.empty() && std::find_if(s.begin(),
            s.end(), [](char c) { return !(std::isdigit(c) || c == '-'); }) == s.end();
}
template<>
bool ProgramArgs::AbsArg::check<double>(string s) {
    return !s.empty() && std::find_if(s.begin(),
            s.end(), [](char c) { return !(std::isdigit(c) || c == '-' || c == '.'); }) == s.end();
}
template<>
bool ProgramArgs::AbsArg::check<bool>(string s) {
    if (s != "0" && s != "1" && s != "true" && s != "false" && s != "TRUE" &&
            s != "FALSE")
        return false;
    else
        return true;
}

template<>
void ProgramArgs::AbsArg::convert(string s, bool &v) {
    if (s == "1" || s == "true" || s == "TRUE")
        v = true;
    else
        v = false;
}
template<>
void ProgramArgs::AbsArg::convert(string s, int &v) {
    stringstream ss;
    ss << s;
    ss >> v;
}
template<>
void ProgramArgs::AbsArg::convert(string s, long &v) {
    stringstream ss;
    ss << s;
    ss >> v;
}
template<>
void ProgramArgs::AbsArg::convert(string s, double &v) {
    stringstream ss;
    ss << s;
    ss >> v;
}
template<>
void ProgramArgs::AbsArg::convert(string s, string &v) {
    v = s;
}
