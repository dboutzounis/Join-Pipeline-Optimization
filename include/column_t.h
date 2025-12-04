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

struct Page_t {
    value_t values[PAGE_T_SIZE];
};

struct Column_t {
    std::vector<Page_t> pages;
    size_t total_size = 0;

    Column_t();

    Column_t(size_t expected_rows);
    void push_back(const value_t& v);

    value_t get_at(size_t index) const;
};
