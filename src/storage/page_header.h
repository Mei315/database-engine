#include <iostream>
#include <vector>
#include <algorithm>
#include <memory>
#include <unordered_map>
#include <cstring>
#include <fstream>
#include <cstdint>

const int PAGE_SIZE = 4096;  // 4KB页大小
const int MAX_KEYS_PER_PAGE = 100;  // 每页最大键数

enum PageType : uint8_t {
    INTERNAL_PAGE = 1,
    LEAF_PAGE = 2
};

#pragma pack(push, 1)  // 强制字节对齐为1，禁止编译器插入Padding（跨编译器一致）
struct PageHeader {
    uint32_t checksum;  // 0-3 校验
    uint32_t magic;     // 4-7 标识格式
    uint16_t version;   // 8-9 版本号
    uint16_t page_type; // 10-11 页类型 (内部节点/叶子节点/溢出页)
    uint64_t lsn;       // 12-19 日志序列号：用于崩溃恢复
    uint32_t page_id;   // 20-23 当前页ID
    uint16_t upper_ptr; // 24-25 数据区起始偏移（向上增长）
    uint16_t lower_ptr; // 26-27 槽目录起始偏移（向下增长）
    uint16_t key_count; // 28-29 当前记录(槽)数量
};
#pragma pack(pop)


static_assert(sizeof(PageHeader) == 30, "PageHeader size mismatch！！！");

// 强制使用小端序读
static inline uint16_t read_u16_le(const uint8_t *p) {
    return (uint16_t)p[0] | (uint16_t(p[1]) << 8);
}
static inline uint32_t read_u32_le(const uint8_t *p) {
    return (uint32_t)p[0] | (uint32_t(p[1]) << 8) | (uint32_t(p[2]) << 16) | (uint32_t(p[3]) << 24);
}
static inline uint64_t read_u64_le(const uint8_t *p) {
    return uint64_t(read_u32_le(p)) | (uint64_t(read_u32_le(p + 4)) << 32);
}

// 强制使用小端序写
static inline void write_u16_le(uint8_t *p, uint16_t v) {
    p[0] = uint8_t(v & 0xFF);
    p[1] = uint8_t((v >> 8) & 0xFF);
}
static inline void write_u32_le(uint8_t *p, uint32_t v) {
    p[0] = uint8_t(v & 0xFF);
    p[1] = uint8_t((v >> 8) & 0xFF);
    p[2] = uint8_t((v >> 16) & 0xFF);
    p[3] = uint8_t((v >> 24) & 0xFF);
}
static inline void write_u64_le(uint8_t *p, uint64_t v) {
    write_u32_le(p, uint32_t(v & 0xFFFFFFFF));
    write_u32_le(p + 4, uint32_t((v >> 32) & 0xFFFFFFFF));
}

// 序列化：将内存中的页头字段按固定顺序写入到一个字节缓冲
static inline void serialize_header(const PageHeader &h, uint8_t *buf) {
    write_u32_le(buf + 0,  h.checksum);
    write_u32_le(buf + 4,  h.magic);
    write_u16_le(buf + 8,  h.version);
    write_u16_le(buf + 10, h.page_type);
    write_u64_le(buf + 12, h.lsn);
    write_u32_le(buf + 20, h.page_id);
    write_u16_le(buf + 24, h.upper_ptr);
    write_u16_le(buf + 26, h.lower_ptr);
    write_u16_le(buf + 28, h.key_count);
}

// 反序列化
static inline void deserialize_header(const uint8_t *buf, PageHeader &h) {
    h.checksum  = read_u32_le(buf + 0);
    h.magic     = read_u32_le(buf + 4);
    h.version   = read_u16_le(buf + 8);
    h.page_type = read_u16_le(buf + 10);
    h.lsn       = read_u64_le(buf + 12);
    h.page_id   = read_u32_le(buf + 20);
    h.upper_ptr = read_u16_le(buf + 24);
    h.lower_ptr = read_u16_le(buf + 26);
    h.key_count = read_u16_le(buf + 28);
}

// 检测数据是否损坏
//写入磁盘前，计算页头内容的哈希值存入
//下次从磁盘读取时，重新计算一遍，如果不一致，则报错
static inline uint32_t simple_checksum(const uint8_t *data, size_t len) {
    uint32_t sum = 2166136261u; // FNV offset basis
    for (size_t i = 0; i < len; ++i) {
        sum ^= data[i];
        sum *= 16777619u; // FNV prime
    }
    return sum;
}

// 计算并写入整个页面的 checksum
static inline void finalize_page_checksum(uint8_t *page_buf) {
    // 跳过前 4 字节（checksum 字段本身），计算后面所有内容的 hash
    // PAGE_SIZE 是 4096
    uint32_t cs = simple_checksum(page_buf + 4, PAGE_SIZE - 4);
    
    // 将结果写回前 4 字节
    write_u32_le(page_buf + 0, cs);
}

// 读取并验证 checksum
static inline bool verify_page_checksum(const uint8_t *page_buf) {
    uint32_t stored_cs = read_u32_le(page_buf + 0);
    uint32_t computed_cs = simple_checksum(page_buf + 4, PAGE_SIZE - 4);
    return stored_cs == computed_cs;
}