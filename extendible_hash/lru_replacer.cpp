#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace scudb {

    template <typename T> LRUReplacer<T>::LRUReplacer() {
        head = make_shared<Node>();
        tail = make_shared<Node>();
        head->next = tail;
        tail->prev = head;
    }

    template <typename T> LRUReplacer<T>::~LRUReplacer() {}

    template <typename T> void LRUReplacer<T>::Insert(const T& value) {
        lock_guard<mutex> lck(latch);
        shared_ptr<Node> cur;
        if (map.find(value) != map.end()) {
            cur = map[value];
            shared_ptr<Node> prev = cur->prev;
            shared_ptr<Node> succ = cur->next;
            prev->next = succ;
            succ->prev = prev;
        }
        else {
            cur = make_shared<Node>(value);
        }
        shared_ptr<Node> fir = head->next;
        cur->next = fir;
        fir->prev = cur;
        cur->prev = head;
        head->next = cur;
        map[value] = cur;   //��ֵ����LRU
        return;
    }

    /* 
     *���LRU�ǿգ���LRU����ͷ��Ա��������value�������ҷ���true�����LRUΪ�գ�����false
     */
    template <typename T> bool LRUReplacer<T>::Victim(T& value) {
        lock_guard<mutex> lck(latch);
        if (map.empty()) {
            return false;
        }
        shared_ptr<Node> last = tail->prev;
        tail->prev = last->prev;
        last->prev->next = tail;
        value = last->val;
        map.erase(last->val);
        return true;
    }

    /*
     * ��LRU��ɾ��ֵ������Ƴ��ɹ�������true�����򷵻�false
     */
    template <typename T> bool LRUReplacer<T>::Erase(const T& value) {
        lock_guard<mutex> lck(latch);
        if (map.find(value) != map.end()) {
            shared_ptr<Node> cur = map[value];
            cur->prev->next = cur->next;
            cur->next->prev = cur->prev;
        }
        return map.erase(value);
    }

    template <typename T> size_t LRUReplacer<T>::Size() {
        lock_guard<mutex> lck(latch);
        return map.size();
    }

    template class LRUReplacer<Page*>;
    template class LRUReplacer<int>;

} 
