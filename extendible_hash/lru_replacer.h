#pragma once
#include <memory>
#include <unordered_map>
#include <mutex>
#include "buffer/replacer.h"
using namespace std;

namespace scudb {

    template <typename T> class LRUReplacer : public Replacer<T> {
        struct Node {
            Node() {};
            Node(T val) : val(val) {};
            T val;
            shared_ptr<Node> prev;
            shared_ptr<Node> next;
        };
    public:
        LRUReplacer();

        ~LRUReplacer(); 

        void Insert(const T& value);

        bool Victim(T& value);

        bool Erase(const T& value);

        size_t Size();

    private:
        shared_ptr<Node> head;
        shared_ptr<Node> tail;
        unordered_map<T, shared_ptr<Node>> map;
        mutable mutex latch;    //在这里添加成员变量
    };

}
