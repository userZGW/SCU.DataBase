/**
 * b_plus_tree_page.cpp
 */
#include "page/b_plus_tree_page.h"

namespace scudb {


bool BPlusTreePage::IsLeafPage() const { return page_type_ == IndexPageType::LEAF_PAGE; }

bool BPlusTreePage::IsRootPage() const { return parent_page_id_ == INVALID_PAGE_ID; }

void BPlusTreePage::SetPageType(IndexPageType page_type) { page_type_ = page_type; }


int BPlusTreePage::GetSize() const { return size_; }

void BPlusTreePage::SetSize(int size) { size_ = size; }

void BPlusTreePage::IncreaseSize(int amount) { size_ += amount; }


int BPlusTreePage::GetMaxSize() const { return max_size_; }

void BPlusTreePage::SetMaxSize(int size) { max_size_ = size; }


int BPlusTreePage::GetMinSize() const {
  if (IsRootPage()) {
    return IsLeafPage() ? 1
                        : 2;
  }
  return (max_size_ ) / 2;
}


page_id_t BPlusTreePage::GetParentPageId() const { return parent_page_id_; }

void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) { parent_page_id_ = parent_page_id; }

  
page_id_t BPlusTreePage::GetPageId() const { return page_id_; }

void BPlusTreePage::SetPageId(page_id_t page_id) { page_id_ = page_id; }


void BPlusTreePage::SetLSN(lsn_t lsn) { lsn_ = lsn; }

  
bool BPlusTreePage::IsSafe(OpType op) {
  int size = GetSize();
  if (op == OpType::INSERT) {
    return size < GetMaxSize();
  }
  int minSize = GetMinSize() + 1;
  if (op == OpType::DELETE) {
    return (IsLeafPage()) ? size >= minSize : size > minSize;
  }
  assert(false);//invalid area
}

} // namespace scudb
