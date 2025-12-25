#pragma once
#include <cassert>
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <vector>

#include "materialization.h"

#define PAGE_T_SIZE 1024
#define PAGE_SHIFT 10
#define PAGE_MASK (PAGE_T_SIZE - 1)

constexpr size_t INT32_ROWS_PER_PAGE = 1984;

enum class ColumnStorage {
    ValueOwned,   
    PageOwned,
};

struct ColumnImpl {
    virtual ~ColumnImpl() = default;
    virtual value_t get_at(size_t i) const = 0;
    virtual size_t size() const = 0;
    virtual size_t page_num() const = 0;
};

struct Page_t {
    value_t values[PAGE_T_SIZE];
};
struct PageRef32 {
    int32_t* data;
    uint16_t num_rows;
};

struct ValueColumn final : ColumnImpl {
    ValueColumn();
    explicit ValueColumn(size_t expected_rows);

    void push_back(const value_t& v);
    void write_at(const value_t& v , size_t index);
    value_t get_at(size_t index) const override;
    size_t size() const override;
    size_t page_num() const override;

    std::vector<Page_t> pages;
    size_t total_size;
};

struct PageColumn final :ColumnImpl{
    PageColumn();
    explicit PageColumn(size_t expected_rows);
    void push_page(int32_t* page , uint16_t num_rows);
    value_t get_at(size_t index) const override;
    size_t size() const override;
    size_t page_num() const override;
    size_t total_size;

    std::vector< int32_t*> pages;
};

struct Column_t {
    Column_t();
    explicit Column_t(ColumnStorage storage , size_t expected_rows);

    void push_back(const value_t& v);
    void write_at(const value_t& v , size_t index);
    void push_page(int32_t* page , uint16_t num_rows);
    value_t get_at(size_t i) const;
    size_t size() const;
    size_t page_num() const;
    
    std::unique_ptr<ColumnImpl> impl;
    ColumnStorage storage;
};
