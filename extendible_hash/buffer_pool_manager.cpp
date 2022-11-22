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
        free_list_ = new std::list<Page*>;    // ���ڻ���ص������ڴ�ռ�

        for (size_t i = 0; i < pool_size_; ++i) {
            free_list_->push_back(&pages_[i]);   // �����е�ҳ���������б�
        }
    }

    BufferPoolManager::~BufferPoolManager() {
        delete[] pages_;
        delete page_table_;
        delete replacer_;
        delete free_list_;
    }

    /**
     * 1. ������ϣ��
     *  1.1 ������ڣ���סҳ�沢��������
     *  1.2 ��������ڣ���free list��lru replacer���ҵ�һ���滻��Ŀ
     * 2. ���ѡ���滻����Ŀ����ģ�����д�ش���
     * 3. ��ɢ�б���ɾ����ҳ�����Ŀ����Ϊ��ҳ�������Ŀ
     * 4.����ҳ��Ԫ���ݣ��Ӵ����ļ���ȡҳ�����ݲ�����ҳ��ָ��
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
     *������ż���>Ϊ0����ݼ����������Ϊ0������Ż�
     *����ڴ˵���֮ǰ���ż���<=0���򷵻�false���Ƿ�dirty:���ô�ҳ���dirty��־
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
     * ���ڽ�����ص��ض�ҳˢ�µ����̡����ҳ����û���ҵ�ҳ���Ƿ�Ӧ�õ��ô��̹�������дҳ����������false
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
     *�û�Ӧ�õ��ô˷�����ɾ��ҳ�档������̽����ô��̹��������ͷ�ҳ�档���ȣ������page 
     *���з�����page������ع�����Ӧ�ø����page����ɾ������Ŀ������ҳ��Ԫ���ݲ���ӻؿ�
     *���б���Σ����ô��̹�������DeallocatePage()�����Ӵ����ļ���ɾ���������ҳ������
     *���������ż���!= 0������false
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
     *�����Ҫ������ҳ�棬�û�Ӧ�õ��ô˷�����������̽����ô��̹�����
     *������ҳ�档����ع�����Ӧ�ø���ӿ����б��lru�滻����ѡ��һ����
     *��ҳ��(ע��:�������ȴӿ����б���ѡ��)��������ҳ���Ԫ���ݣ������
     *�沢��ҳ���������Ӧ����Ŀ��������е�����ҳ�涼���̶����򷵻�nullptr
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
