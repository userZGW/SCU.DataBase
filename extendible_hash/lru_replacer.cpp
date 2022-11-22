#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace scudb {

    template <typename T> LRUReplacer<T>::LRUReplacer() {
        head_node = make_shared<Node>();
        tail_node = make_shared<Node>();
        head_node->next_node = tail_node;
        tail_node->prev_node = head_node;
    }

    template <typename T> LRUReplacer<T>::~LRUReplacer() {}

    template <typename T> void LRUReplacer<T>::Insert(const T& value) {
        lock_guard<mutex> lck(latch);
        
        shared_ptr<Node> cur;
        
        if (map.find(value) != map.end()) {
            cur = map[value];
            shared_ptr<Node> prev = cur->prev;
            shared_ptr<Node> succ = cur->next;
            prev->next_node = succ;
            succ->prev_node = prev;
        }
        else {
            cur = make_shared<Node>(value);
        }
        shared_ptr<Node> fir = head_node->next_node;
        cur_node->next_node = fir_node;
        fir_node->prev_node = cur_node;
        cur_node->prev_node = head_node;
        head_node->next_node = cur_node;
        node_map[value] = cur_node;   //将值插入LRU
        return;
    }

    /* 
     *如果LRU非空，从LRU弹出头成员到参数“value”，并且返回true。如果LRU为空，返回false
     */
    template <typename T> bool LRUReplacer<T>::Victim(T& value) {
        lock_guard<mutex> lck(latch);
        if (map.empty()) {
            return false;
        }
        shared_ptr<Node> last_node = tail_node->prev_node;
        tail_node->prev_node = last_node->prev_node;
        last_node->prev_node->next_node = tail_node;
        value = last_node->val_node;
        node_map.erase(last->val);
        return true;
    }

    /*
     * 从LRU中删除值。如果移除成功，返回true，否则返回false
     */
    template <typename T> bool LRUReplacer<T>::Erase(const T& value) {
        lock_guard<mutex> lck(latch);
        if (map.find(value) != map.end()) {
            shared_ptr<Node> cur_node = node_map[value];
            cur_node->prev_node->next_node = cur_node->next_node;
            cur_node->next_node->prev_node = cur_node->prev_node;
        }
        return node_map.erase(value);
    }

    template <typename T> size_t LRUReplacer<T>::Size() {
        lock_guard<mutex> lck(latch);
        return node_map.size();
    }

    template class LRUReplacer<Page*>;
    
    template class LRUReplacer<int>;

} 
