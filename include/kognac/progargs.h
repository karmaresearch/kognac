#ifndef _PROGARGS_H
#define _PROGARGS_H

#include <kognac/consts.h>
#include <kognac/logs.h>

#include <vector>
#include <set>
#include <map>

using namespace std;

class ProgramArgs {
    public:
        class AbsArg {
            private:
                string shortname;
                string name;
                string desc;
                bool isset;
                bool required;
            protected:
                AbsArg(string shortname,
                        string name,
                        string desc,
                        bool required) {
                    this->shortname = shortname;
                    this->name = name;
                    this->desc = desc;
                    this->isset = false;
                    this->required = required;
                }
                void markSet() {
                    isset = true;
                }
                template<class K>
                    bool check(string s);
                template<class K>
                    KLIBEXP void convert(string s, K &v);
            public:
                bool isRequired() {
                    return required;
                }
                bool isSet() {
                    return isset;
                }
                string getName() {
                    return name;
                }
                virtual void set(string value) = 0;
                virtual string tostring(string def) {
                    string out = "";
                    if (shortname == "") {
                        out += "--" + name + " arg ";
                    } else {
                        out += "-" + shortname + " OR --" + name + " arg ";
                    }
                    string padder = " ";
                    while (out.size() + padder.size() < 40) {
                        padder += " ";
                    }
                    string d = "";
                    if (required) {
                        d += "[REQUIRED] ";
                    } else {
                        d += "[OPTIONAL " + def + "] ";
                    }
                    d += desc;
                    string nonformatted = "";
                    if (d.find('\n') != string::npos) {
                        auto pos = d.find('\n');
                        nonformatted = d.substr(pos + 1);
                        d = d.substr(0, pos+1);
                    }
                    if (d.size() < 40) {
                        out += padder + d;
                    } else {
                        int cutpos = 40;
                        string d1 = d.substr(0, cutpos);
                        while (cutpos < d.size() && d[cutpos] != ' ') {
                            d1 += d[cutpos++];
                        }
                        if (cutpos < d.size() && d[cutpos] == ' ')
                            cutpos++;
                        out += padder + d1;
                        d = d.substr(cutpos, d.size() - cutpos);
                        while (d.size() > 40) {
                            out += "\n";
                            for (int i = 0; i < 40; ++i) out += " ";
                            int cutpos = 40;
                            d1 = d.substr(0, cutpos);
                            while (cutpos < d.size() && d[cutpos] != ' ') {
                                d1 += d[cutpos++];
                            }
                            if (cutpos < d.size() && d[cutpos] == ' ')
                                cutpos++;
                            d = d.substr(cutpos, d.size() - cutpos);
                            out += d1;
                        }
                        if (d.size() > 0) {
                            out += "\n";
                            for (int i = 0; i < 40; ++i) out += " ";
                            out += d;
                        }
                    }
                    out += nonformatted;
                    return out;
                }
                string tostring() {
                    return tostring("");
                }
        };
        template <class K>
            class Arg : public AbsArg {
                private:
                    K value;
                public:
                    Arg(K defvalue,
                            string shortname,
                            string name,
                            string desc,
                            bool required) : AbsArg(shortname, name, desc, required) {
                        this->value = defvalue;
                    }
                    void set(string value) {
                        if (isSet()) {
                            LOG(ERRORL) << "Param '" << getName() << "' cannot be set more than once";
                            throw 10;
                        }
                        if (!check<K>(value)) {
                            LOG(ERRORL) << "Value for the param '" << getName() << "' cannot be converted to the appropriate value";
                            throw 10;
                        }
                        convert<K>(value, this->value);
                        markSet();
                    }
                    K get() {
                        return value;
                    }
                    string tostring(string def) {
                        string defaultvalue = "";
                        if (!isRequired()) {
                            std::stringstream ss;
                            ss << value;
                            defaultvalue += " DEFAULT=" + ss.str();
                        }
                        string out = AbsArg::tostring(defaultvalue);
                        return out;
                    }
            };
        class OutVar {
            private:
                std::shared_ptr<AbsArg> v;
            public:
                OutVar(std::shared_ptr<AbsArg> v) : v(v) {}
                template<class K>
                    K as() {
                        return ((Arg<K>*)v.get())->get();
                    }
                bool empty() {
                    return !v->isSet();
                }
        };
        class GroupArgs {
            private:
                string namegroup;
                map<string, std::shared_ptr<AbsArg>> &shortnames;
                map<string, std::shared_ptr<AbsArg>> &names;
                std::vector<std::shared_ptr<AbsArg>> args;

                void addNewName(string sn, string n,
                        std::shared_ptr<AbsArg> arg) {
                    if (sn != "") {
                        if (!shortnames.count(sn)) {
                            shortnames.insert(make_pair(sn, arg));
                        } else {
                            LOG(ERRORL) << "Parameter " << sn << " already defined";
                            throw 10;
                        }
                    }
                    if (!names.count(n)) {
                        names.insert(make_pair(n, arg));
                    } else {
                        LOG(ERRORL) << "Parameter " << n << " already defined";
                        throw 10;
                    }
                }

            public:
                GroupArgs(string name, map<string, std::shared_ptr<AbsArg>> &shortnames,
                        map<string, std::shared_ptr<AbsArg>> &names) : namegroup(name), shortnames(shortnames), names(names) {
                }

                template<class K>
                    GroupArgs& add(string shortname, string name, K defvalue,
                            string desc, bool required) {
                        auto p = std::shared_ptr<AbsArg>(
                                new Arg<K>(defvalue, shortname, name, desc, required));
                        addNewName(shortname, name, p);
                        args.push_back(p);
                        return *this;
                    }
                string tostring() {
                    string out = namegroup + ":\n";
                    for(auto params : args) {
                        out += params->tostring() + "\n";
                    }
                    out += "\n";
                    return out;
                }
        };

    private:
        map<string, std::shared_ptr<AbsArg>> shortnames;
        map<string, std::shared_ptr<AbsArg>> names;
        std::map<string, std::shared_ptr<GroupArgs>> args;
        std::set<string> posArgs;

    public:
        std::shared_ptr<GroupArgs> newGroup(string name) {
            auto ptr = std::shared_ptr<GroupArgs>(new GroupArgs(name, shortnames, names));
            args.insert(make_pair(name, ptr));
            return ptr;
        }
        void parse(int argc, const char** argv) {
            for(int i = 0; i < argc; ++i) {
                if (argv[i][0] == '-' && argv[i][1] == '-') {
                    string nameParam = string(argv[i]).substr(2);
                    if (names.count(nameParam)) {
                        if (i == argc - 1) {
                            LOG(ERRORL) << "Miss value of the param";
                            throw 10;
                        }
                        string value = argv[++i];
                        names[nameParam]->set(value);
                    } else {
                        LOG(ERRORL) << "Param " << nameParam << " not found";
                        throw 10;
                    }
                } else if (argv[i][0] == '-') {
                    string nameParam = string(argv[i]).substr(1);
                    if (shortnames.count(nameParam)) {
                        if (i == argc - 1) {
                            LOG(ERRORL) << "Miss value of the param";
                            throw 10;
                        }
                        string value = argv[++i];
                        shortnames[nameParam]->set(value);
                    } else {
                        LOG(ERRORL) << "Param " << nameParam << " not found";
                        throw 10;
                    }
                } else {
                    posArgs.insert(string(argv[i]));
                }
            }
        }
        KLIBEXP void check() {
            for(auto pair : names) {
                if (pair.second->isRequired() && !pair.second->isSet()) {
                    LOG(ERRORL) << "Param " << pair.second->getName() << " is required but no set";
                    throw 10;
                }
            }
        }
        OutVar operator [](string n) {
            if (!names.count(n)) {
                LOG(ERRORL) << "Param " << n << " not found";
                throw 10;
            }
            return OutVar(names.find(n)->second);
        }
        bool count(string name) {
            return posArgs.count(name) ||
                (names.count(name) && names.find(name)->second->isSet());
        }

        string tostring() const {
            string out = "";
            for (auto pair : args) {
                out += pair.second->tostring();
            }
            return out;
        }
};
#endif
