/*
 * Copyright 2016 Jacopo Urbani
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
**/

#ifndef SCHEMAEXT_H_
#define SCHEMAEXT_H_

#include <kognac/setestimation.h>
#include <kognac/stringscol.h>
#include <kognac/hashmap.h>
#include <kognac/fpgrowth.h>

#include <sparsehash/dense_hash_map>
#include <sparsehash/dense_hash_set>
#include <string>
#include <vector>
#include <set>

using namespace std;

typedef google::dense_hash_map<const char *, vector<int64_t>, hashstr, eqstr> SchemaMap;
typedef google::dense_hash_map<int64_t, vector<int64_t> *> NumericSchemaMap;
typedef google::dense_hash_map<int64_t, vector<int64_t> > NumericNPSchemaMap;
typedef google::dense_hash_set<const char*, hashstr, eqstr> TextualSet;

typedef google::dense_hash_map<int64_t, int64_t> DomainRangeMap;

typedef struct ExtNode {
    const int64_t key;
    ExtNode *child;
    ExtNode *parent;
    ExtNode* sibling;
    int64_t assignedID;
    int depth;
    ExtNode(const int64_t k) : key(k), child(NULL), parent(NULL), sibling(NULL),
        assignedID(0), depth(0) {}
    static bool less(ExtNode *n1, ExtNode *n2) {
        return n1->depth > n2->depth;
    }
} ExtNode;

class SchemaExtractor {

private:
    bool isPresent(const int64_t el, vector<int64_t> &elements);

    void rearrangeTreeByDepth(ExtNode *node);

    bool isReachable(NumericSchemaMap &map, vector<int64_t> &prevEls, int64_t source,
                     int64_t dest);

    bool isDirectSubclass(NumericSchemaMap &map, int64_t subclass, int64_t superclass);

    void addToMap(SchemaMap &map, const char *key, const char *value);

    void addToMap(SchemaMap &map, const char *key, const int64_t value);

    void addToMap(NumericNPSchemaMap &map, const int64_t key, const int64_t value);

    ExtNode *buildTreeFromRoot(NumericNPSchemaMap &map, NumericSchemaMap &subclassMap,
                               const int64_t root);

    void processClasses(SchemaMap &inputMap, NumericNPSchemaMap &outputMap);

    void processExplicitClasses(SchemaMap &inputMap, TextualSet &set);

    void transitiveClosure(NumericNPSchemaMap &map, ExtNode *root);

    void deallocateTree(ExtNode *node);

    void assignID(ExtNode *root, int64_t &counter);

    void printTree(int padding, ExtNode *root);

    void serializeNode(std::ostream &out,
                       ExtNode *node);

    void serializeNodeBeginRange(std::ostream &out,
                                 ExtNode *node);

protected:
    StringCollection supportSubclasses;
    SchemaMap subclasses;

    StringCollection supportExplicitClasses;
    TextualSet explicitClasses;

    NumericNPSchemaMap outputSubclasses;

    DomainRangeMap domains;
    DomainRangeMap ranges;

    map<int64_t, string> hashMappings;
    map<int64_t, std::pair<int64_t, int64_t> > classesRanges;

    /*SetEstimation propertyCardinality;
    google::dense_hash_map<long, long> propertiesID;*/

    ExtNode *root;

public:
    static const int64_t HASHCLASS;
    static const int64_t HASHTYPE;

    SchemaExtractor();

    void extractSchema(char **triple);

    void merge(SchemaExtractor & schema);

    void prepare();

    void printTree() {
        printTree(0, root);
    }

    void serialize(string file);

    void rearrangeWithPatterns(std::map<uint64_t, uint64_t> &classes,
                               std::vector<FPattern<uint64_t> > &patterns);

    int64_t getRankingProperty(const int64_t property);

    bool hasDomain(const int64_t hashProperty) const;

    int64_t getDomain(const int64_t hashProperty) const;

    bool hasRange(const int64_t hashProperty) const;

    int64_t getRange(const int64_t hashProperty) const;

    std::set<string> getAllClasses() const;

    string getText(int64_t id) const;

    void retrieveInstances(const int64_t term, const vector<int64_t> **output) const;

    void addClassesBeginEndRange(const int64_t classId,
                                 const int64_t start,
                                 const int64_t end);

    ~SchemaExtractor();
};

#endif
