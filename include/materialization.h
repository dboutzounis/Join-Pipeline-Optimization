#pragma once

#include <plan.h>

#include <cstdint>

static constexpr int TABLE_SHIFT = 56;
static constexpr int COLUMN_SHIFT = 48;
static constexpr int PAGE_SHIFT = 16;
static constexpr int OFFSET_IDX_SHIFT = 2;

static constexpr uint64_t TABLE_MASK = ((1ull << 8) - 1) << TABLE_SHIFT;
static constexpr uint64_t COLUMN_MASK = ((1ull << 8) - 1) << COLUMN_SHIFT;
static constexpr uint64_t PAGE_MASK = ((1ull << 32) - 1) << PAGE_SHIFT;
static constexpr uint64_t OFFSET_IDX_MASK = ((1ull << 14) - 1) << OFFSET_IDX_SHIFT;

struct Smart_string {
    uint64_t data;

    Smart_string(uint64_t data = 0);

    Smart_string encode(uint32_t table_id, uint32_t column_id, uint32_t page_id, uint32_t offset_idx);

    inline uint32_t get_table_id() const { return (data & TABLE_MASK) >> TABLE_SHIFT; }

    inline uint32_t get_column_id() const { return (data & COLUMN_MASK) >> COLUMN_SHIFT; }

    inline uint32_t get_page_id() const { return (data & PAGE_MASK) >> PAGE_SHIFT; }

    inline uint32_t get_offset_idx() const { return (data & OFFSET_IDX_MASK) >> OFFSET_IDX_SHIFT; }

    std::string get_value(const Plan& plan);
};

enum class ValueType : uint8_t {
    NONE = 0b00,
    INT32 = 0b01,
    SMART_STRING = 0b10,
};

struct value_t {
    uint64_t data;

    value_t();

    static constexpr uint64_t TYPE_MASK = 0b11;

    inline ValueType get_type() const { return static_cast<ValueType>(data & TYPE_MASK); }

    value_t null_value();

    value_t from_int32(int32_t v);

    value_t from_string(Smart_string v);

    bool is_null() const;

    int32_t get_int32() const;

    Smart_string get_string() const;
};