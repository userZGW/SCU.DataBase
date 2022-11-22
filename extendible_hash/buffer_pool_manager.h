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
        size_t pool_size_; // 缓冲池中的页数
        Page* pages_;      // 页面数组
        DiskManager* disk_manager_;
        LogManager* log_manager_;
        HashTable<page_id_t, Page*>* page_table_; // 跟踪页面
        Replacer<Page*>* replacer_;   // 查找要替换的未固定页
        std::list<Page*>* free_list_; // 找到一个空闲的页面进行替换
        std::mutex latch_;             // 保护共享数据结构
        Page* GetVictimPage();
    };
} 
Footer
© 2022 GitHub, Inc.
Footer navigation
Terms
Privacy
