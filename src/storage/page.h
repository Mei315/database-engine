#ifndef PAGE_H
#define PAGE_H

#include "page_header.h"
#include <cstring>
#include <vector>
#include <stdexcept>

/*
Slotted Page
============================================================
Byte Offset     Component              Description
============================================================
    0           +---------------------+
                |   PAGE HEADER       |  32 bytes
   32           +---------------------+
                |   Slot 0            |  \
                |   Slot 1            |   |  Slot Directory
                |   Slot 2            |   |  (grows downward)
                |   ...               |   |  Each slot: 4 bytes
                |   Slot N-1          |  /
 lower_ptr      +---------------------+
                |                     |
                |                     |
                |    FREE SPACE       |  Available space
                |                     |  (shrinks as data added)
                |                     |
 upper_ptr      +---------------------+
                |   Data Item N-1     |  \
                |   Data Item N-2     |   |
                |   ...               |   |  Data Area (Tuples)
                |   Data Item 1       |   |  (grows upward)
                |   Data Item 0       |  /   Each item: 8 bytes
 4096           +---------------------+
*/

// 槽目录
#pragma pack(push, 1)
struct SlotEntry {
    uint32_t offset;  // 每个tuple对应的offset值
    uint32_t length;  // 每个tuple的长度
};
#pragma pack(pop)

static_assert(sizeof(SlotEntry) == 8, "SlotEntry size mismatch！！！");

// 内部节点
struct InternalNode {
    int key;
    uint32_t child_page_id; 
};

// 叶子节点
struct LeafNode {
    int key;
    int value;
};

class Page {
private:
    uint8_t data_[PAGE_SIZE];  // 4KB 原始数据
    PageHeader header_;        
    bool dirty_;               // 脏页标记
    bool is_pinned_;           // 当前是否有线程正在使用这个页

public:
    Page() : dirty_(false), is_pinned_(false) {
        std::memset(data_, 0, PAGE_SIZE);
        init_header();
    }

    void init_header(uint32_t page_id, uint16_t type) {
    header_.checksum = 0;             // 初始校验和设为0
    header_.magic = 0x50414745;       // "PAGE"
    header_.version = 1;
    header_.page_type = type;         
    header_.lsn = 0;
    header_.page_id = page_id;
    header_.upper_ptr = static_cast<uint16_t>(PAGE_SIZE); 
    header_.lower_ptr = static_cast<uint16_t>(sizeof(PageHeader)); 
    header_.key_count = 0;
    header_.dir_count = 0;            

    serialize_to_buffer();
    finalize_page_checksum(data_); 
}

    // 将页头序列化到缓冲区
    void serialize_to_buffer() {
        serialize_header(header_, data_);
        finalize_page_checksum(data_);
        dirty_ = true;
    }

    // 从缓冲区反序列化页头
    bool deserialize_from_buffer() {
        // 先读取存储的校验和（页头的前 4 字节）
        uint32_t stored_checksum = read_u32_le(data_ + 0);

        // 计算当前页面（跳过 checksum 字段本身）的校验和
        uint32_t calculated_checksum = simple_checksum(data_ + 4, PAGE_SIZE - 4);

        // 验证校验和
        if (stored_checksum != calculated_checksum) {
            return false;  // 校验失败
        }

        // 反序列化页头
        deserialize_header(data_, header_);

        // 验证魔数（与 init_header 中写入的魔数保持一致）
        if (header_.magic != 0x50414745) { // 'PAGE'
            return false;
        }
        
        return true;
    }

    // 获取原始数据指针
    uint8_t* get_data() { return data_; }
    const uint8_t* get_data() const { return data_; }

    // 获取页ID
    uint32_t get_page_id() const { return header_.page_id; }
    void set_page_id(uint32_t page_id) { 
        header_.page_id = page_id; 
        serialize_to_buffer();
    }

    // 是否为叶子节点（使用 PageHeader::page_type）
    bool is_leaf() const { return header_.page_type == LEAF_PAGE; }
    void set_leaf(bool is_leaf) {
        header_.page_type = is_leaf ? LEAF_PAGE : INTERNAL_PAGE;
        serialize_to_buffer();
    }

    // 获取键数量
    uint16_t get_key_count() const { return header_.key_count; }

    // 获取空闲空间大小
    uint16_t get_free_space() const {
        return header_.upper_ptr - header_.lower_ptr;
    }

    // 脏页标记
    bool is_dirty() const { return dirty_; }
    void set_dirty(bool dirty) { dirty_ = dirty; }

    // 钉住页（防止被换出内存）
    void pin() { is_pinned_ = true; }
    void unpin() { is_pinned_ = false; }
    bool is_pinned() const { return is_pinned_; }

    // 获取槽目录项
    SlotEntry* get_slot(uint16_t slot_idx) {
        if (slot_idx >= header_.key_count) {
            return nullptr;
        }
        uint16_t slot_offset = sizeof(PageHeader) + slot_idx * sizeof(SlotEntry);
        return reinterpret_cast<SlotEntry*>(data_ + slot_offset);
    }

    // 插入数据项（通用方法）
    bool insert_item(const void* item_data, uint16_t item_size, uint16_t& slot_idx) {
        // 检查空间是否足够（需要槽目录空间 + 数据空间）
        uint16_t required_space = sizeof(SlotEntry) + item_size;
        if (get_free_space() < required_space) {
            return false;  // 空间不足
        }

        // 分配槽位
        slot_idx = header_.key_count;
        
        // 更新数据区指针（向下增长）
        header_.upper_ptr -= item_size;
        
        // 写入数据
        std::memcpy(data_ + header_.upper_ptr, item_data, item_size);
        
        // 更新槽目录
        SlotEntry* slot = reinterpret_cast<SlotEntry*>(
            data_ + header_.lower_ptr
        );
        slot->offset = header_.upper_ptr;
        slot->length = item_size;
        
        // 更新槽目录指针（向上增长）
        header_.lower_ptr += sizeof(SlotEntry);
        header_.key_count++;
        
        serialize_to_buffer();
        return true;
    }

    // 插入叶子节点条目
    bool insert_leaf_entry(int key, int value) {
        LeafNode entry{key, value};
        uint16_t slot_idx;
        return insert_item(&entry, sizeof(LeafNode), slot_idx);
    }

    // 插入内部节点条目
    bool insert_internal_entry(int key, uint32_t child_page_id) {
        InternalNode entry{key, child_page_id};
        uint16_t slot_idx;
        return insert_item(&entry, sizeof(InternalNode), slot_idx);
    }

    // 获取叶子节点条目
    bool get_leaf_entry(uint16_t slot_idx, LeafNode& entry) const {
        if (slot_idx >= header_.key_count || !is_leaf()) {
            return false;
        }
        
        const SlotEntry* slot = reinterpret_cast<const SlotEntry*>(
            data_ + sizeof(PageHeader) + slot_idx * sizeof(SlotEntry)
        );
        
        std::memcpy(&entry, data_ + slot->offset, sizeof(LeafNode));
        return true;
    }

    // 获取内部节点条目
    bool get_internal_entry(uint16_t slot_idx, InternalNode& entry) const {
        if (slot_idx >= header_.key_count || is_leaf()) {
            return false;
        }
        
        const SlotEntry* slot = reinterpret_cast<const SlotEntry*>(
            data_ + sizeof(PageHeader) + slot_idx * sizeof(SlotEntry)
        );
        
        std::memcpy(&entry, data_ + slot->offset, sizeof(InternalNode));
        return true;
    }

    // 删除条目（通过标记槽为无效，实际空间整理需要额外操作）
    bool delete_item(uint16_t slot_idx) {
        if (slot_idx >= header_.key_count) {
            return false;
        }
        
        SlotEntry* slot = get_slot(slot_idx);
        if (!slot) return false;
        
        // 标记为删除（长度设为0）
        slot->length = 0;
        
        serialize_to_buffer();
        return true;
    }

    // 压缩页空间（整理碎片）
    void compact() {
        std::vector<std::pair<uint16_t, uint16_t>> valid_items;  // <offset, length>
        
        // 收集所有有效条目
        for (uint16_t i = 0; i < header_.key_count; ++i) {
            SlotEntry* slot = get_slot(i);
            if (slot && slot->length > 0) {
                valid_items.push_back({slot->offset, slot->length});
            }
        }
        
        // 重新布局数据区
        uint16_t new_upper = PAGE_SIZE;
        for (size_t i = 0; i < valid_items.size(); ++i) {
            auto [old_offset, length] = valid_items[i];
            new_upper -= length;
            
            // 移动数据
            if (old_offset != new_upper) {
                std::memmove(data_ + new_upper, data_ + old_offset, length);
            }
            
            // 更新槽
            SlotEntry* slot = get_slot(i);
            slot->offset = new_upper;
        }
        
        header_.upper_ptr = new_upper;
        header_.key_count = valid_items.size();
        header_.lower_ptr = sizeof(PageHeader) + header_.key_count * sizeof(SlotEntry);
        
        serialize_to_buffer();
    }

    // 搜索键（二分查找，要求键已排序）
    int search_key(int key) const {
        int left = 0, right = header_.key_count - 1;
        int result = -1;
        
        while (left <= right) {
            int mid = left + (right - left) / 2;
            
            int mid_key;
            if (is_leaf()) {
                LeafEntry entry;
                if (!const_cast<Page*>(this)->get_leaf_entry(mid, entry)) {
                    break;
                }
                mid_key = entry.key;
            } else {
                InternalEntry entry;
                if (!const_cast<Page*>(this)->get_internal_entry(mid, entry)) {
                    break;
                }
                mid_key = entry.key;
            }
            
            if (mid_key == key) {
                return mid;
            } else if (mid_key < key) {
                left = mid + 1;
            } else {
                result = mid;
                right = mid - 1;
            }
        }
        
        return result;  // 返回第一个 >= key 的位置，如果不存在返回-1
    }

    // 线性搜索键（用于未排序的页面）
    int linear_search_key(int key) const {
        for (uint16_t i = 0; i < header_.key_count; ++i) {
            if (is_leaf()) {
                LeafEntry entry;
                if (const_cast<Page*>(this)->get_leaf_entry(i, entry)) {
                    if (entry.key == key) {
                        return i;
                    }
                }
            } else {
                InternalEntry entry;
                if (const_cast<Page*>(this)->get_internal_entry(i, entry)) {
                    if (entry.key == key) {
                        return i;
                    }
                }
            }
        }
        return -1;  // 未找到
    }

    // 更新 LSN（日志序列号）
    void set_lsn(uint64_t lsn) {
        header_.lsn = lsn;
        serialize_to_buffer();
    }

    uint64_t get_lsn() const {
        return header_.lsn;
    }

    // 调试：打印页信息
    void print_info() const {
        std::cout << "=== Page Info ===" << std::endl;
        std::cout << "Page ID: " << header_.page_id << std::endl;
        std::cout << "Is Leaf: " << (is_leaf() ? "Yes" : "No") << std::endl;
        std::cout << "Key Count: " << header_.key_count << std::endl;
        std::cout << "Free Space: " << get_free_space() << " bytes" << std::endl;
        std::cout << "LSN: " << header_.lsn << std::endl;
        std::cout << "Upper Ptr: " << header_.upper_ptr << std::endl;
        std::cout << "Lower Ptr: " << header_.lower_ptr << std::endl;
        std::cout << "=================" << std::endl;
    }

    // 从磁盘加载页
    bool load_from_disk(std::ifstream& file) {
        file.read(reinterpret_cast<char*>(data_), PAGE_SIZE);
        if (!file.good()) {
            return false;
        }
        
        return deserialize_from_buffer();
    }

    // 写入磁盘
    bool write_to_disk(std::ofstream& file) {
        serialize_to_buffer();
        file.write(reinterpret_cast<const char*>(data_), PAGE_SIZE);
        dirty_ = false;
        return file.good();
    }
};

#endif // PAGE_H