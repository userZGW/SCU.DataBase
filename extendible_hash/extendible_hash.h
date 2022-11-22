#pragma once

#include <cstdlib>
#include <vector>
#include <string>
#include <map>
#include <memory>
#include <mutex>
#include "hash/hash_table.h"
using namespace std;


namespace scudb {


template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
  struct Bucket {
    Bucket(int depth) : local_depth(depth) {};
    int local_depth;
    map<K, V> key_map;
    mutex latch;
  };
public:
  
  ExtendibleHash(size_t size);
  ExtendibleHash();        // 构造器
  
  size_t HashKey(const K &key) const;      // 帮助函数生成哈希地址
 
  bool Find(const K &key, V &value) override;
  bool Remove(const K &key) override;
  void Insert(const K &key,const V &value) override;        // 查找和修改

  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const        //帮助函数获取全局和局部深度
  int getIdx(const K &key) const;

private:
  int global_depth;
  size_t bucket_size;
  int bucket_num;
  vector<shared_ptr<Bucket>> buckets;
  mutable mutex latch;     // 在这里添加成员变量
};
} 
