#pragma once
#include <list>
#include <mutex>
#include "buffer/lru_replacer.h"
#include "disk/disk_manager.h"
#include "hash/extendible_hash.h"
#include "logging/log_manager.h"
#include "page/page.h"

namespace scudb {
    class BufferPoolManager {
    public:
        BufferPoolManager(size_t pool_size, DiskManager* disk_manager,
            LogManager* log_manager = nullptr);

        ~BufferPoolManager();

        Page* FetchPage(page_id_t page_id);

        bool UnpinPage(page_id_t page_id, bool is_dirty);

        bool FlushPage(page_id_t page_id);

        Page* NewPage(page_id_t& page_id);

        bool DeletePage(page_id_t page_id);

    private:
        size_t pool_size_; // ������е�ҳ��
        Page* pages_;      // ҳ������
        DiskManager* disk_manager_;
        LogManager* log_manager_;
        HashTable<page_id_t, Page*>* page_table_; // ����ҳ��
        Replacer<Page*>* replacer_;   // ����Ҫ�滻��δ�̶�ҳ
        std::list<Page*>* free_list_; // �ҵ�һ�����е�ҳ������滻
        std::mutex latch_;             // �����������ݽṹ
        Page* GetVictimPage();
    };
} 
