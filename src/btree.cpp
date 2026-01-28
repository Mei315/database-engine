#include <iostream>
#include <vector>
#include <algorithm>
#include <memory>
#include <unordered_map>
#include <cstring>
#include <fstream>
#ifdef _WIN32
#include <direct.h>
#else
#include <sys/stat.h>
#include <sys/types.h>
#endif

// ============ 页式存储配置 ============
const int PAGE_SIZE = 4096;  // 4KB页大小
const int MAX_KEYS_PER_PAGE = 100;  // 每页最大键数

using PageID = uint32_t;
const PageID INVALID_PAGE_ID = 0;

// ============ 页类型枚举 ============
enum PageType : uint8_t {
    INTERNAL_PAGE = 1,
    LEAF_PAGE = 2
};

// ============ 页头结构 ============
struct PageHeader {
    PageType pageType;
    uint32_t keyCount;
    PageID parentPageId;
    PageID nextPageId;  // 仅用于叶子节点链表
    PageID prevPageId;  // 仅用于叶子节点链表
    
    PageHeader() : pageType(LEAF_PAGE), keyCount(0), 
                   parentPageId(INVALID_PAGE_ID),
                   nextPageId(INVALID_PAGE_ID),
                   prevPageId(INVALID_PAGE_ID) {}
};

// ============ 页结构（修复：不使用union，分开存储） ============
template<typename KeyType, typename ValueType>
struct Page {
    PageHeader header;
    KeyType keys[MAX_KEYS_PER_PAGE];
    PageID children[MAX_KEYS_PER_PAGE + 1];  // 内部节点使用
    ValueType values[MAX_KEYS_PER_PAGE];      // 叶子节点使用
    
    Page() {
        header = PageHeader();
        memset(keys, 0, sizeof(keys));
        memset(children, 0, sizeof(children));
        // values会自动初始化
    }
    
    // 显式析构函数
    ~Page() {
        // ValueType的析构会自动调用
    }
};

// ============ 缓冲池管理器 ============
template<typename KeyType, typename ValueType>
class BufferPoolManager {
private:
    std::unordered_map<PageID, Page<KeyType, ValueType>*> pageTable;
    PageID nextPageId;
    
public:
    BufferPoolManager() : nextPageId(1) {}
    
    ~BufferPoolManager() {
        for (typename std::unordered_map<PageID, Page<KeyType, ValueType>*>::iterator it = pageTable.begin(); 
             it != pageTable.end(); ++it) {
            delete it->second;
        }
    }
    
    // 分配新页面
    PageID allocatePage() {
        return nextPageId++;
    }
    
    // 获取页面（如果不存在则创建）
    Page<KeyType, ValueType>* fetchPage(PageID pageId) {
        if (pageTable.find(pageId) == pageTable.end()) {
            pageTable[pageId] = new Page<KeyType, ValueType>();
        }
        return pageTable[pageId];
    }
    
    // 刷新页面到磁盘（将页面序列化为可读文本文件，便于验证）
    void flushPage(PageID pageId) {
        Page<KeyType, ValueType>* page = fetchPage(pageId);
        std::cout << "[DISK I/O] 刷新页面 " << pageId << " 到磁盘\n";

    try {
#ifdef _WIN32
        _mkdir("page_files");
#else
        mkdir("page_files", 0755);
#endif
            std::string filePath = std::string("page_files/page_") + std::to_string(pageId) + ".txt";
            std::ofstream ofs(filePath);
            if (!ofs.is_open()) {
                std::cerr << "[ERROR] 无法打开文件写入: " << filePath << "\n";
                return;
            }

            ofs << "PageID: " << pageId << "\n";
            ofs << "PageType: " << (int)page->header.pageType << "\n";
            ofs << "KeyCount: " << page->header.keyCount << "\n";
            ofs << "ParentPageId: " << page->header.parentPageId << "\n";
            ofs << "NextPageId: " << page->header.nextPageId << "\n";
            ofs << "PrevPageId: " << page->header.prevPageId << "\n";

            ofs << "Keys:\n";
            for (uint32_t i = 0; i < page->header.keyCount; i++) {
                ofs << page->keys[i];
                if (i + 1 < page->header.keyCount) ofs << '\t';
            }
            ofs << "\n";

            if (page->header.pageType == INTERNAL_PAGE) {
                ofs << "Children:\n";
                for (uint32_t i = 0; i <= page->header.keyCount; i++) {
                    ofs << page->children[i];
                    if (i < page->header.keyCount) ofs << '\t';
                }
                ofs << "\n";
            } else {
                ofs << "Values:\n";
                for (uint32_t i = 0; i < page->header.keyCount; i++) {
                    ofs << page->values[i];
                    if (i + 1 < page->header.keyCount) ofs << '\t';
                }
                ofs << "\n";
            }

            ofs.close();
        } catch (const std::exception& e) {
            std::cerr << "[ERROR] flushPage 异常: " << e.what() << "\n";
        }
    }
    
    // 删除页面
    void deletePage(PageID pageId) {
        if (pageTable.find(pageId) != pageTable.end()) {
            delete pageTable[pageId];
            pageTable.erase(pageId);
        }
    }
    
    // 获取统计信息
    size_t getPageCount() const { return pageTable.size(); }
    
    void printStats() {
        std::cout << "=== 缓冲池统计 ===\n";
        std::cout << "总页数: " << pageTable.size() << "\n";
        std::cout << "下一个页ID: " << nextPageId << "\n";
        std::cout << "页面大小: " << sizeof(Page<KeyType, ValueType>) << " 字节\n";
    }
};

// ============ 页式B+树 ============
template<typename KeyType, typename ValueType>
class PagedBPlusTree {
private:
    BufferPoolManager<KeyType, ValueType> bufferPool;
    PageID rootPageId;
    PageID firstLeafPageId;
    int order;
    
    // 查找叶子页面
    PageID findLeafPage(KeyType key) {
        PageID currentPageId = rootPageId;
        
        while (true) {
            Page<KeyType, ValueType>* page = bufferPool.fetchPage(currentPageId);
            
            if (page->header.pageType == LEAF_PAGE) {
                return currentPageId;
            }
            
            // 内部节点，查找子节点
            int pos = 0;
            while (pos < (int)page->header.keyCount && key >= page->keys[pos]) {
                pos++;
            }
            currentPageId = page->children[pos];
        }
    }
    
    // 分裂叶子页面
    void splitLeafPage(PageID leafPageId, KeyType key, ValueType value) {
        Page<KeyType, ValueType>* leafPage = bufferPool.fetchPage(leafPageId);
        PageID newLeafPageId = bufferPool.allocatePage();
        Page<KeyType, ValueType>* newLeafPage = bufferPool.fetchPage(newLeafPageId);
        
        newLeafPage->header.pageType = LEAF_PAGE;
        
        std::cout << "[SPLIT] 分裂叶子页面 " << leafPageId << " -> " << newLeafPageId << "\n";
        
        // 临时数组
        std::vector<KeyType> tempKeys(leafPage->keys, leafPage->keys + leafPage->header.keyCount);
        std::vector<ValueType> tempValues(leafPage->values, leafPage->values + leafPage->header.keyCount);
        
        // 插入新键值
        int pos = 0;
        while (pos < (int)tempKeys.size() && tempKeys[pos] < key) pos++;
        tempKeys.insert(tempKeys.begin() + pos, key);
        tempValues.insert(tempValues.begin() + pos, value);
        
        // 分裂点
        int mid = (order + 1) / 2;
        
        // 左半部分
        leafPage->header.keyCount = mid;
        std::copy(tempKeys.begin(), tempKeys.begin() + mid, leafPage->keys);
        std::copy(tempValues.begin(), tempValues.begin() + mid, leafPage->values);
        
        // 右半部分
        newLeafPage->header.keyCount = tempKeys.size() - mid;
        std::copy(tempKeys.begin() + mid, tempKeys.end(), newLeafPage->keys);
        std::copy(tempValues.begin() + mid, tempValues.end(), newLeafPage->values);
        
        // 更新链表指针
        newLeafPage->header.nextPageId = leafPage->header.nextPageId;
        newLeafPage->header.prevPageId = leafPageId;
        if (leafPage->header.nextPageId != INVALID_PAGE_ID) {
            Page<KeyType, ValueType>* nextPage = bufferPool.fetchPage(leafPage->header.nextPageId);
            nextPage->header.prevPageId = newLeafPageId;
        }
        leafPage->header.nextPageId = newLeafPageId;
        
        // 向父节点插入
        if (leafPageId == rootPageId) {
            PageID newRootPageId = bufferPool.allocatePage();
            Page<KeyType, ValueType>* newRootPage = bufferPool.fetchPage(newRootPageId);
            newRootPage->header.pageType = INTERNAL_PAGE;
            newRootPage->header.keyCount = 1;
            newRootPage->keys[0] = newLeafPage->keys[0];
            newRootPage->children[0] = leafPageId;
            newRootPage->children[1] = newLeafPageId;
            
            leafPage->header.parentPageId = newRootPageId;
            newLeafPage->header.parentPageId = newRootPageId;
            rootPageId = newRootPageId;
            
            std::cout << "[ROOT] 创建新根页面 " << newRootPageId << "\n";
        } else {
            newLeafPage->header.parentPageId = leafPage->header.parentPageId;
            insertInternal(newLeafPage->keys[0], leafPage->header.parentPageId, newLeafPageId);
        }
        
        bufferPool.flushPage(leafPageId);
        bufferPool.flushPage(newLeafPageId);
    }
    
    // 分裂内部页面
    void splitInternalPage(PageID internalPageId, KeyType key, PageID childPageId) {
        Page<KeyType, ValueType>* internalPage = bufferPool.fetchPage(internalPageId);
        PageID newInternalPageId = bufferPool.allocatePage();
        Page<KeyType, ValueType>* newInternalPage = bufferPool.fetchPage(newInternalPageId);
        
        newInternalPage->header.pageType = INTERNAL_PAGE;
        
        std::cout << "[SPLIT] 分裂内部页面 " << internalPageId << " -> " << newInternalPageId << "\n";
        
        // 临时数组
        std::vector<KeyType> tempKeys(internalPage->keys, internalPage->keys + internalPage->header.keyCount);
        std::vector<PageID> tempChildren(internalPage->children, internalPage->children + internalPage->header.keyCount + 1);
        
        // 插入新键和子节点
        int pos = 0;
        while (pos < (int)tempKeys.size() && tempKeys[pos] < key) pos++;
        tempKeys.insert(tempKeys.begin() + pos, key);
        tempChildren.insert(tempChildren.begin() + pos + 1, childPageId);
        
        // 分裂点
        int mid = (order + 1) / 2;
        KeyType midKey = tempKeys[mid];
        
        // 左半部分
        internalPage->header.keyCount = mid;
        std::copy(tempKeys.begin(), tempKeys.begin() + mid, internalPage->keys);
        std::copy(tempChildren.begin(), tempChildren.begin() + mid + 1, internalPage->children);
        
        // 右半部分
        newInternalPage->header.keyCount = tempKeys.size() - mid - 1;
        std::copy(tempKeys.begin() + mid + 1, tempKeys.end(), newInternalPage->keys);
        std::copy(tempChildren.begin() + mid + 1, tempChildren.end(), newInternalPage->children);
        
        // 更新子节点的父指针
        for (int i = 0; i <= (int)newInternalPage->header.keyCount; i++) {
            Page<KeyType, ValueType>* childPage = bufferPool.fetchPage(newInternalPage->children[i]);
            childPage->header.parentPageId = newInternalPageId;
        }
        
        // 向父节点插入
        if (internalPageId == rootPageId) {
            PageID newRootPageId = bufferPool.allocatePage();
            Page<KeyType, ValueType>* newRootPage = bufferPool.fetchPage(newRootPageId);
            newRootPage->header.pageType = INTERNAL_PAGE;
            newRootPage->header.keyCount = 1;
            newRootPage->keys[0] = midKey;
            newRootPage->children[0] = internalPageId;
            newRootPage->children[1] = newInternalPageId;
            
            internalPage->header.parentPageId = newRootPageId;
            newInternalPage->header.parentPageId = newRootPageId;
            rootPageId = newRootPageId;
            
            std::cout << "[ROOT] 创建新根页面 " << newRootPageId << "\n";
        } else {
            newInternalPage->header.parentPageId = internalPage->header.parentPageId;
            insertInternal(midKey, internalPage->header.parentPageId, newInternalPageId);
        }
        
        bufferPool.flushPage(internalPageId);
        bufferPool.flushPage(newInternalPageId);
    }
    
    // 向内部节点插入
    void insertInternal(KeyType key, PageID internalPageId, PageID childPageId) {
        Page<KeyType, ValueType>* internalPage = bufferPool.fetchPage(internalPageId);
        
        if (internalPage->header.keyCount < (uint32_t)(order - 1)) {
            int pos = 0;
            while (pos < (int)internalPage->header.keyCount && internalPage->keys[pos] < key) pos++;
            
            // 移动键和子节点
            for (int i = internalPage->header.keyCount; i > pos; i--) {
                internalPage->keys[i] = internalPage->keys[i - 1];
                internalPage->children[i + 1] = internalPage->children[i];
            }
            
            internalPage->keys[pos] = key;
            internalPage->children[pos + 1] = childPageId;
            internalPage->header.keyCount++;
            
            Page<KeyType, ValueType>* childPage = bufferPool.fetchPage(childPageId);
            childPage->header.parentPageId = internalPageId;
            
            bufferPool.flushPage(internalPageId);
        } else {
            splitInternalPage(internalPageId, key, childPageId);
        }
    }
    
public:
    PagedBPlusTree(int ord = 3) : order(ord) {
        rootPageId = bufferPool.allocatePage();
        Page<KeyType, ValueType>* rootPage = bufferPool.fetchPage(rootPageId);
        rootPage->header.pageType = LEAF_PAGE;
        firstLeafPageId = rootPageId;
        
        std::cout << "[INIT] 创建根页面 " << rootPageId << "\n";
    }
    
    // 插入
    void insert(KeyType key, ValueType value) {
        std::cout << "\n[INSERT] 插入 key=" << key << "\n";
        
        PageID leafPageId = findLeafPage(key);
        Page<KeyType, ValueType>* leafPage = bufferPool.fetchPage(leafPageId);
        
        // 检查是否已存在
        for (int i = 0; i < (int)leafPage->header.keyCount; i++) {
            if (leafPage->keys[i] == key) {
                leafPage->values[i] = value;
                bufferPool.flushPage(leafPageId);
                return;
            }
        }
        
        if (leafPage->header.keyCount < (uint32_t)(order - 1)) {
            int pos = 0;
            while (pos < (int)leafPage->header.keyCount && leafPage->keys[pos] < key) pos++;
            
            // 移动键值
            for (int i = leafPage->header.keyCount; i > pos; i--) {
                leafPage->keys[i] = leafPage->keys[i - 1];
                leafPage->values[i] = leafPage->values[i - 1];
            }
            
            leafPage->keys[pos] = key;
            leafPage->values[pos] = value;
            leafPage->header.keyCount++;
            
            bufferPool.flushPage(leafPageId);
        } else {
            splitLeafPage(leafPageId, key, value);
        }
    }
    
    // 查找
    bool search(KeyType key, ValueType& value) {
        PageID leafPageId = findLeafPage(key);
        Page<KeyType, ValueType>* leafPage = bufferPool.fetchPage(leafPageId);
        
        std::cout << "[SEARCH] 在页面 " << leafPageId << " 中查找 key=" << key << "\n";
        
        for (int i = 0; i < (int)leafPage->header.keyCount; i++) {
            if (leafPage->keys[i] == key) {
                value = leafPage->values[i];
                return true;
            }
        }
        return false;
    }
    
    // 范围查询
    std::vector<std::pair<KeyType, ValueType> > rangeQuery(KeyType startKey, KeyType endKey) {
        std::vector<std::pair<KeyType, ValueType> > result;
        PageID leafPageId = findLeafPage(startKey);
        
        std::cout << "[RANGE] 范围查询 [" << startKey << ", " << endKey << "]\n";
        
        while (leafPageId != INVALID_PAGE_ID) {
            Page<KeyType, ValueType>* leafPage = bufferPool.fetchPage(leafPageId);
            
            for (int i = 0; i < (int)leafPage->header.keyCount; i++) {
                if (leafPage->keys[i] >= startKey && leafPage->keys[i] <= endKey) {
                    result.push_back(std::make_pair(leafPage->keys[i], leafPage->values[i]));
                }
                if (leafPage->keys[i] > endKey) return result;
            }
            
            leafPageId = leafPage->header.nextPageId;
        }
        
        return result;
    }
    
    // 打印树结构
    void print() {
        std::cout << "\n=== B+树结构 ===\n";
        std::vector<PageID> currentLevel;
        currentLevel.push_back(rootPageId);
        int level = 0;
        
        while (!currentLevel.empty()) {
            std::cout << "层级 " << level++ << ": ";
            std::vector<PageID> nextLevel;
            
            for (size_t idx = 0; idx < currentLevel.size(); idx++) {
                PageID pageId = currentLevel[idx];
                Page<KeyType, ValueType>* page = bufferPool.fetchPage(pageId);
                std::cout << "[Page" << pageId << ":";
                
                for (int i = 0; i < (int)page->header.keyCount; i++) {
                    std::cout << page->keys[i];
                    if (i < (int)page->header.keyCount - 1) std::cout << ",";
                }
                std::cout << "] ";
                
                if (page->header.pageType == INTERNAL_PAGE) {
                    for (int i = 0; i <= (int)page->header.keyCount; i++) {
                        nextLevel.push_back(page->children[i]);
                    }
                }
            }
            std::cout << "\n";
            currentLevel = nextLevel;
        }
        
        bufferPool.printStats();
    }
};

// ============ 测试代码 ============
int main() {
    std::cout << "========== 页式B+树测试 ==========\n";
    std::cout << "页大小配置: " << PAGE_SIZE << " 字节\n";
    std::cout << "每页最大键数: " << MAX_KEYS_PER_PAGE << "\n\n";
    
    PagedBPlusTree<int, std::string> tree(4);
    
    // 测试1: 插入数据
    std::cout << "\n===== 测试1: 插入数据 =====\n";
    tree.insert(10, "value10");
    tree.insert(20, "value20");
    tree.insert(5, "value5");
    tree.insert(15, "value15");
    tree.insert(25, "value25");
    tree.insert(30, "value30");
    tree.insert(35, "value35");
    tree.insert(40, "value40");
    
    tree.print();
    
    // 测试2: 查找
    std::cout << "\n===== 测试2: 查找操作 =====\n";
    std::string value;
    if (tree.search(15, value)) {
        std::cout << "✓ 找到 key=15: " << value << "\n";
    }
    if (!tree.search(100, value)) {
        std::cout << "✓ key=100 不存在\n";
    }
    
    // 测试3: 范围查询
    std::cout << "\n===== 测试3: 范围查询 =====\n";
    std::vector<std::pair<int, std::string> > results = tree.rangeQuery(10, 30);
    std::cout << "结果:\n";
    for (size_t i = 0; i < results.size(); i++) {
        std::cout << "  " << results[i].first << " -> " << results[i].second << "\n";
    }
    
    // 测试4: 大量插入测试
    std::cout << "\n===== 测试4: 大量插入 =====\n";
    PagedBPlusTree<int, int> largeTree(5);
    for (int i = 1; i <= 200; i++) {
        largeTree.insert(i, i * 100);
    }
    std::cout << "插入20个元素后:\n";
    largeTree.print();
    
    return 0;
}