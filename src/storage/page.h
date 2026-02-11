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
    uint32_t offset;  // Tuple 的起始位置
    uint32_t length;  // Tuple 的长度
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
    uint8_t data_[PAGE_SIZE];  // 4KB
    PageHeader header_;        
    bool dirty_;               // 脏页标记
    bool is_pinned_;           // 当前是否有线程正在使用这个页

    // 获取槽数组起始偏移量
    uint16_t get_slot_array_offset() const {
        return static_cast<uint16_t>(sizeof(PageHeader));
    }

    // 获取特定槽位的 Key (用于二分查找)
    int get_key_at_slot(uint16_t slot_idx) const {
        const SlotEntry* slot = get_slot_ptr(slot_idx);
        if (header_.page_type == LEAF_PAGE) {
            return reinterpret_cast<const LeafNode*>(data_ + slot->offset)->key;
        } else {
            return reinterpret_cast<const InternalNode*>(data_ + slot->offset)->key;
        }
    }

    // 获取槽位指针（原始位置）
    SlotEntry* get_slot_ptr(uint16_t slot_idx) {
        return reinterpret_cast<SlotEntry*>(data_ + get_slot_array_offset() + (slot_idx * sizeof(SlotEntry)));
    }

    const SlotEntry* get_slot_ptr(uint16_t slot_idx) const {
        return reinterpret_cast<const SlotEntry*>(data_ + get_slot_array_offset() + (slot_idx * sizeof(SlotEntry)));
    }

    // 写入槽位信息
    void write_slot_to_buffer(uint16_t slot_idx, const SlotEntry& se) {
        std::memcpy(get_slot_ptr(slot_idx), &se, sizeof(SlotEntry));
    }

    // 寻找插入点：返回第一个 key >= target_key 的槽位索引 (二分查找)
    uint16_t find_insertion_point(int target_key) const {
        int left = 0, right = header_.key_count - 1;
        while (left <= right) {
            int mid = left + (right - left) / 2;
            if (get_key_at_slot(mid) < target_key) left = mid + 1;
            else right = mid - 1;
        }
        return static_cast<uint16_t>(left);
    }

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

    serialize_to_buffer();
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
            return false;  
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

    // 是否为叶子节点
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

    // 钉住页
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

    bool insert_index_item(const void* item_data, uint16_t item_size, int key, uint16_t& slot_idx) {
    // 1. 获取当前页中 Slot Array 的首地址指针
    /*
    [ PageHeader | SlotEntry | SlotEntry | SlotEntry | ... ]
                 ↑
                 slot_array_offset
    */
    uint8_t* slot_array_ptr = data_ + get_slot_array_offset(); 

    // 2. 确定插入位置
    uint16_t target_idx = find_insertion_point(key);

    // 计算本次操作对连续空闲区的绝对消耗
    // 消耗 = 新槽位 + 数据 + 预留给目录的增长
    uint16_t total_consumption = sizeof(SlotEntry) + item_size + dir_growth;

    // 严格检查：确保中间 Free 区足以容纳
    if (header_.lower_ptr + total_consumption > header_.upper_ptr - current_dir_space) {
        // 问题 4 的对策：如果空间不足，尝试 Purge（物理删除那些标为 dead 的索引槽）
        // if (has_dead_slots()) { purge_index_page(); return insert_index_item(...); }
        return false; 
    }

    // --- 物理落地阶段 ---

    // 4. 目录空间预留落地（解决问题 1 的核心：先挪指针，后写数据）
    if (needs_new_dir) {
        // 注意：PageDir 位于数据区之上，所以我们要压低有效的 upper 界限
        // 我们不立即写 Directory，但必须通过某种方式标记这块地被占了
        reserve_directory_space(sizeof(uint16_t)); 
    }

    // 5. 槽位挪移（保持有序）
    if (target_idx < header_.key_count) {
        std::memmove(
            slot_array_ptr + (target_idx + 1) * sizeof(SlotEntry),
            slot_array_ptr + target_idx * sizeof(SlotEntry),
            (header_.key_count - target_idx) * sizeof(SlotEntry)
        );
    }

    // 6. 数据写入 (Scheme B)
    header_.upper_ptr -= item_size;
    std::memcpy(data_ + header_.upper_ptr, key_data, item_size);

    // 7. 更新元数据 (解决问题 3：Key_count 仅在 Index 上下文代表记录数)
    header_.key_count++; 
    header_.lower_ptr += sizeof(SlotEntry);

    SlotEntry se{header_.upper_ptr, item_size};
    write_slot_to_buffer(target_idx, se);

    slot_idx = target_idx;
    this->dirty_ = true; 

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

    // 逻辑删除条目（标记槽为无效）
    bool delete_item(uint16_t slot_idx) {
        if (slot_idx >= header_.key_count) {
            return false;
        }

        SlotEntry* slot = get_slot(slot_idx);
        if (!slot) return false;

        // 标记为删除（长度设为0）
        slot->length = 0;

        // 仅序列化页头以记录删除标记（物理回收由上层或后台维护触发）
        serialize_to_buffer();
        return true;
    }

    // 搜索键（二分查找，要求键已排序）
    int search_key(int key) const {
        int left = 0, right = header_.key_count - 1;
        int result = -1;
        
        while (left <= right) {
            int mid = left + (right - left) / 2;
            
            int mid_key;
            if (is_leaf()) {
                LeafNode entry;
                if (!const_cast<Page*>(this)->get_leaf_entry(mid, entry)) {
                    break;
                }
                mid_key = entry.key;
            } else {
                InternalNode entry;
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
                LeafNode entry;
                if (const_cast<Page*>(this)->get_leaf_entry(i, entry)) {
                    if (entry.key == key) {
                        return i;
                    }
                }
            } else {
                InternalNode entry;
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