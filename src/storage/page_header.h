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
    uint32_t magic;     // 0-3 标识格式
    uint16_t version;   // 4-5 版本
    uint64_t lsn;       // 6-13 日志序列号：用于崩溃恢复
    uint32_t page_id;   // 14-17 当前页ID
    uint16_t upper_ptr; // 18-19 数据区起始偏移（向上增长）
    uint16_t lower_ptr; // 20-21 槽目录起始偏移（向下增长）
    uint8_t  is_leaf;   // 22 是否为叶子节点（0/1）
    uint16_t key_count; // 23-24 当前键数量
    uint32_t checksum;  // 25-28 校验
};
#pragma pack(pop)


static_assert(sizeof(PageHeader) == 29, "PageHeader size mismatch！！！");

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
    write_u32_le(buf + 0, h.magic);
    write_u16_le(buf + 4, h.version);
    write_u64_le(buf + 6, h.lsn);
    write_u32_le(buf + 14, h.page_id);
    write_u16_le(buf + 18, h.upper_ptr);
    write_u16_le(buf + 20, h.lower_ptr);
    buf[22] = h.is_leaf;
    write_u16_le(buf + 23, h.key_count);
    write_u32_le(buf + 25, h.checksum);
}

// 反序列化
static inline void deserialize_header(const uint8_t *buf, PageHeader &h) {
    h.magic = read_u32_le(buf + 0);
    h.version = read_u16_le(buf + 4);
    h.lsn = read_u64_le(buf + 6);
    h.page_id = read_u32_le(buf + 14);
    h.upper_ptr = read_u16_le(buf + 18);
    h.lower_ptr = read_u16_le(buf + 20);
    h.is_leaf = buf[22];
    h.key_count = read_u16_le(buf + 23);
    h.checksum = read_u32_le(buf + 25);
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

static inline void finalize_header_checksum(uint8_t *buf) {
    // 假定 checksum 位于偏移 25，长度为 4
    // 计算整个 buf 除去 checksum 字段之外的 checksum
    uint32_t cs = simple_checksum(buf, 25);
    write_u32_le(buf + 25, cs);
}