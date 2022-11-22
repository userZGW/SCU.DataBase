#include <list>
#include "hash/extendible_hash.h"
#include "page/page.h"
using namespace std;

namespace scudb {
     template <typename K, typename V>
     ExtendibleHash<K, V>::ExtendibleHash(size_t size) {
     global_depth = 0;
     bucket_size = size;
     bucket_number = 1;
     buckets.push_back(make_shared<Bucket>(0));
     }
    
    template<typename K, typename V>
    ExtendibleHash<K, V>::ExtendibleHash() {
    ExtendibleHash(64);   //固定每个桶的数组大小
    }

    template <typename K, typename V>
    size_t ExtendibleHash<K, V>::HashKey(const K& key) const {
        return hash<K>{}(key);     //帮助函数计算输入键的哈希地址
    }

    template <typename K, typename V>
    int ExtendibleHash<K, V>::GetGlobalDepth() const {
        lock_guard<mutex> lock(latch);
        return global_depth;   //返回哈希表全局深度
    }

    template <typename K, typename V>
    int ExtendibleHash<K, V>::GetGlobalDepth() const {
    lock_guard<mutex> lock(latch_table);
        return global_depth;   //返回该桶的局部深度
    }

    template <typename K, typename V>
    int ExtendibleHash<K, V>::GetNumBuckets() const {
        lock_guard<mutex> lock(latch);
        return bucketNum;      //返回哈希表中桶的当前编号
    }

    template <typename K, typename V>
    bool ExtendibleHash<K, V>::Find(const K& key, V& value) {

        int idx = getIdx(key);
        lock_guard<mutex> lck(buckets[idx]->latch);
        if (buckets[idx]->kmap.find(key) != buckets[idx]->kmap.end()) {
            value = buckets[idx]->kmap[key];       //查找与输入键相关的值
            return true;

        }
        return false;
    }

    template <typename K, typename V>
    int ExtendibleHash<K, V>::getIdx(const K& key) const {
        lock_guard<mutex> lck(latch);
        return HashKey(key) & ((1 << global_depth) - 1);
    }

    template <typename K, typename V>   
    bool ExtendibleHash<K, V>::Remove(const K& key) {
        int idx = getIdx(key);
        lock_guard<mutex> lck(buckets[idx]->latch);
        shared_ptr<Bucket> cur = buckets[idx];
        if (cur->kmap.find(key) == cur->kmap.end()) {
            return false; 
        }
        cur->kmap.erase(key);      //删除哈希表中<key,value>的条目
        return true;
    }

    template <typename K, typename V>
    void ExtendibleHash<K, V>::Insert(const K& key, const V& value) {
        int idx = getIdx(key);
        shared_ptr<Bucket> cur = buckets[idx];
        while (true) {
            lock_guard<mutex> lck(cur->latch);
            if (cur->kmap.find(key) != cur->kmap.end() || cur->kmap.size() < bucket_size) {
                cur->kmap[key] = value;
                break;
            }
            int mask = (1 << (cur->local_depth));
            cur->local_depth++;

            {
                lock_guard<mutex> lck2(latch);
                if (cur->local_depth > global_depth) {

                    size_t length = buckets.size();
                    for (size_t i = 0; i < length; i++) {
                        buckets.push_back(buckets[i]);
                    }
                    global_depth++;

                }
                bucket_num++;       //当有溢出时，拆分并重新分配桶，如有必要增加全局深度
                auto newBuc = make_shared<Bucket>(cur->local_depth);

                typename map<K, V>::iterator it;
                for (it = cur->kmap.begin(); it != cur->kmap.end(); ) {
                    if (HashKey(it->first) & mask) {
                        newBuc->kmap[it->first] = it->second;
                        it = cur->kmap.erase(it);
                    }
                    else it++;
                }
                for (size_t i = 0; i < buckets.size(); i++) {
                    if (buckets[i] == cur && (i & mask))
                        buckets[i] = newBuc;
                }
            }
            idx = getIdx(key);
            cur = buckets[idx];   //在哈希表中插入<key,value>条目
        }
    }

    template class ExtendibleHash<page_id_t, Page*>;
    template class ExtendibleHash<Page*, std::list<Page*>::iterator>;
    template class ExtendibleHash<int, std::string>;
    template class ExtendibleHash<int, std::list<int>::iterator>;
    template class ExtendibleHash<int, int>;
}
