#pragma once
#include <memory>
#include <cstddef>
#include <cassert>
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
    std::vector<Page_t*> pages;
    size_t total_size = 0;

    Column_t() : total_size(0) {}

    ~Column_t() {
        for (auto* p : pages) {
            delete p;
        }
    }
    void push_back(const value_t& v) {
        size_t pageIndex = total_size >> PAGE_SHIFT;
        size_t offset    = total_size & PAGE_MASK;

        if (offset == 0) {
            // need a new page
            pages.push_back(new Page_t());
        }

        pages[pageIndex]->values[offset] = v;
        total_size++;
    }

    value_t get_at(size_t index)const{
        if (index >= total_size)
            return value_t::null_value();

        size_t pageIndex = index >> PAGE_SHIFT;
        size_t offset    = index & PAGE_MASK;

        return pages[pageIndex]->values[offset];
    }

};

