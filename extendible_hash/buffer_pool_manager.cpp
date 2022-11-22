#include "buffer/buffer_pool_manager.h"

namespace scudb {
    BufferPoolManager::BufferPoolManager(size_t pool_size,
        DiskManager* disk_manager,
        LogManager* log_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager),
        log_manager_(log_manager) { 
        pages_ = new Page[pool_size_];
        page_table_ = new ExtendibleHash<page_id_t, Page*>(BUCKET_SIZE);
        replacer_ = new LRUReplacer<Page*>;
        free_list_ = new std::list<Page*>;    // 用于缓冲池的连续内存空间

        for (size_t i = 0; i < pool_size_; ++i) {
            free_list_->push_back(&pages_[i]);   // 把所有的页面放入空闲列表
        }
    }

    BufferPoolManager::~BufferPoolManager() {
        delete[] pages_;
        delete page_table_;
        delete replacer_;
        delete free_list_;
    }

    /**
     * 1. 搜索哈希表
     *  1.1 如果存在，钉住页面并立即返回
     *  1.2 如果不存在，从free list或lru replacer中找到一个替换条目
     * 2. 如果选择替换的条目是脏的，则将其写回磁盘
     * 3. 从散列表中删除旧页面的条目，并为新页面插入条目
     * 4.更新页面元数据，从磁盘文件读取页面内容并返回页面指针
     */
    Page* BufferPoolManager::FetchPage(page_id_t page_id) {
        lock_guard<mutex> lck(latch_);
        
        Page* tar = nullptr;
        if (page_table_->Find(page_id, tar)) { //1.1
            tar->pin_count_++;
            replacer_->Erase(tar);
            return tar;
        }
        //1.2
        tar = GetVictimPage();
        if (tar == nullptr) return tar;
        //2
        if (tar->is_dirty_) {
            disk_manager_->WritePage(tar->GetPageId(), tar->data_);
        }
        //3
        page_table_->Remove(tar->GetPageId());
        page_table_->Insert(page_id, tar);
        //4
        disk_manager_->ReadPage(page_id, tar->data_);
        tar->pin_count_ = 1;
        tar->is_dirty_ = false;
        tar->page_id_ = page_id;

        return tar;
    }

    /*
     *如果引脚计数>为0，则递减它，如果它为0，则将其放回
     *如果在此调用之前引脚计数<=0，则返回false。是否dirty:设置此页面的dirty标志
     */
    bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
        lock_guard<mutex> lck(latch_);
        Page* tar = nullptr;
        page_table_->Find(page_id, tar);
        if (tar == nullptr) {
            return false;
        }
        tar->is_dirty_ = is_dirty;
        if (tar->GetPinCount() <= 0) {
            return false;
        }
        ;
        if (--tar->pin_count_ == 0) {
            replacer_->Insert(tar);
        }
        return true;
    }

    /*
     * 用于将缓冲池的特定页刷新到磁盘。如果页表中没有找到页，是否应该调用磁盘管理器的写页方法，返回false
     */
    bool BufferPoolManager::FlushPage(page_id_t page_id) {
        lock_guard<mutex> lck(latch_);
       
        Page* tar = nullptr;
        page_table_->Find(page_id, tar);
        if (tar == nullptr || tar->page_id_ == INVALID_PAGE_ID) {
            return false;
        }
        if (tar->is_dirty_) {
            disk_manager_->WritePage(page_id, tar->GetData());
            tar->is_dirty_ = false;
        }

        return true;
    }

    /**
     *用户应该调用此方法来删除页面。这个例程将调用磁盘管理器来释放页面。首先，如果在page 
     *表中发现了page，缓冲池管理器应该负责从page表中删除该条目，重置页面元数据并添加回空
     *闲列表。其次，调用磁盘管理器的DeallocatePage()方法从磁盘文件中删除。如果在页表中找
     *到，但引脚计数!= 0，返回false
     */
    bool BufferPoolManager::DeletePage(page_id_t page_id) {
        lock_guard<mutex> lck(latch_);
        
        Page* tar = nullptr;
        
        page_table_->Find(page_id, tar);
        if (tar != nullptr) {
            if (tar->GetPinCount() > 0) {
                return false;
            }
            replacer_->Erase(tar);
            page_table_->Remove(page_id);
            tar->is_dirty_ = false;
            tar->ResetMemory();
            free_list_->push_back(tar);
        }
        disk_manager_->DeallocatePage(page_id);
        return true;
    }

    /**
     *如果需要创建新页面，用户应该调用此方法。这个例程将调用磁盘管理器
     *来分配页面。缓冲池管理器应该负责从空闲列表或lru替换器中选择一个替
     *换页面(注意:总是首先从空闲列表中选择)，更新新页面的元数据，清空内
     *存并在页表中添加相应的条目。如果池中的所有页面都被固定，则返回nullptr
     */
    Page* BufferPoolManager::NewPage(page_id_t& page_id) {
        lock_guard<mutex> lck(latch_);
        Page* tar = nullptr;
        tar = GetVictimPage();
        if (tar == nullptr) return tar;

        page_id = disk_manager_->AllocatePage();
        //2
        if (tar->is_dirty_) {
            disk_manager_->WritePage(tar->GetPageId(), tar->data_);
        }
        //3
        page_table_->Remove(tar->GetPageId());
        page_table_->Insert(page_id, tar);

        //4
        tar->page_id_ = page_id;
        tar->ResetMemory();
        tar->is_dirty_ = false;
        tar->pin_count_ = 1;

        return tar;
    }

    Page* BufferPoolManager::GetVictimPage() {
        Page* tar = nullptr;
        if (free_list_->empty()) {
            if (replacer_->Size() == 0) {
                return nullptr;
            }
            replacer_->Victim(tar);
        }
        else {
            tar = free_list_->front();
            free_list_->pop_front();
            assert(tar->GetPageId() == INVALID_PAGE_ID);
        }
        assert(tar->GetPinCount() == 0);
        return tar;
    }

}
