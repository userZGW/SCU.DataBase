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
  ExtendibleHash();        // ������
  
  size_t HashKey(const K &key) const;      // �����������ɹ�ϣ��ַ
 
  bool Find(const K &key, V &value) override;
  bool Remove(const K &key) override;
  void Insert(const K &key,const V &value) override;        // ���Һ��޸�

  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const        //����������ȡȫ�ֺ;ֲ����
  int getIdx(const K &key) const;

private:
  int global_depth;
  size_t bucket_size;
  int bucket_num;
  vector<shared_ptr<Bucket>> buckets;
  mutable mutex latch;     // ��������ӳ�Ա����
};
} 
