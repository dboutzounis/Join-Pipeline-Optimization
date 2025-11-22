#pragma once

#include <plan.h>

#include <cstdint>

struct Smart_string {
    uint64_t data;

    Smart_string(uint64_t data = 0) : data(data) {}

    static constexpr int TABLE_SHIFT = 56;
    static constexpr int COLUMN_SHIFT = 48;
    static constexpr int PAGE_SHIFT = 16;
    static constexpr int OFFSET_IDX_SHIFT = 2;

    static constexpr uint64_t TABLE_MASK = ((1ull << 8) - 1) << TABLE_SHIFT;
    static constexpr uint64_t COLUMN_MASK = ((1ull << 8) - 1) << COLUMN_SHIFT;
    static constexpr uint64_t PAGE_MASK = ((1ull << 32) - 1) << PAGE_SHIFT;
    static constexpr uint64_t OFFSET_IDX_MASK = ((1ull << 14) - 1) << OFFSET_IDX_SHIFT;

    static Smart_string encode(uint32_t table_id, uint32_t column_id, uint32_t page_id, uint32_t offset_idx) {
        uint64_t str = 0;
        str |= (uint64_t(table_id) << TABLE_SHIFT);
        str |= (uint64_t(column_id) << COLUMN_SHIFT);
        str |= (uint64_t(page_id) << PAGE_SHIFT);
        str |= (uint64_t(offset_idx) << OFFSET_IDX_SHIFT);
        return Smart_string(str);
    }

    uint32_t get_table_id() const { return (data & TABLE_MASK) >> TABLE_SHIFT; }

    uint32_t get_column_id() const { return (data & COLUMN_MASK) >> COLUMN_SHIFT; }

    uint32_t get_page_id() const { return (data & PAGE_MASK) >> PAGE_SHIFT; }

    uint32_t get_offset_idx() const { return (data & OFFSET_IDX_MASK) >> OFFSET_IDX_SHIFT; }

    std::string get_value(const Plan& plan) {
        auto& input = plan.inputs[get_table_id()];
        auto& column = input.columns[get_column_id()];
        auto page_id = get_page_id();
        auto* page = column.pages[page_id]->data;

        auto num_rows = *reinterpret_cast<uint16_t*>(page);

        if (num_rows == 0xffff) {
            auto num_chars = *reinterpret_cast<uint16_t*>(page + 2);
            auto* data_begin = reinterpret_cast<char*>(page + 4);
            std::string value{data_begin, data_begin + num_chars};

            while (++page_id < column.pages.size()) {
                page = column.pages[page_id]->data;
                num_rows = *reinterpret_cast<uint16_t*>(page);
                if (num_rows != 0xfffe) break;
                num_chars = *reinterpret_cast<uint16_t*>(page + 2);
                data_begin = reinterpret_cast<char*>(page + 4);
                value.insert(value.end(), data_begin, data_begin + num_chars);
            }

            return value;
        }

        auto num_non_null = *reinterpret_cast<uint16_t*>(page + 2);
        auto* offset_begin = reinterpret_cast<uint16_t*>(page + 4);
        auto* data_begin = reinterpret_cast<char*>(page + 4 + num_non_null * 2);
        auto offset_idx = get_offset_idx();
        auto string_begin = (offset_idx == 0 ? 0 : offset_begin[offset_idx - 1]);
        auto offset = offset_begin[offset_idx];
        std::string value{data_begin + string_begin, data_begin + offset};

        return value;
    }
};

enum class ValueType : uint8_t {
    NONE = 0b00,
    INT32 = 0b01,
    SMART_STRING = 0b10,
};

struct value_t {
    uint64_t data;

    value_t() : data(static_cast<uint64_t>(ValueType::NONE)) {}

    static constexpr uint64_t TYPE_MASK = 0b11;

    ValueType get_type() const { return static_cast<ValueType>(data & TYPE_MASK); }

    static value_t null_value() {
        value_t x;
        x.data = static_cast<uint64_t>(ValueType::NONE);
        return x;
    }

    static value_t from_int32(int32_t v) {
        value_t x;
        uint64_t val = static_cast<uint64_t>(v);
        val &= 0xffffffffull;
        uint64_t shifted_data = val << 2;
        x.data = shifted_data | static_cast<uint64_t>(ValueType::INT32);
        return x;
    }

    static value_t from_string(Smart_string v) {
        value_t x;
        x.data = (v.data & ~static_cast<uint64_t>(0b11ull)) | static_cast<uint64_t>(ValueType::SMART_STRING);
        return x;
    }

    bool is_null() const { return get_type() == ValueType::NONE; }

    int32_t get_int32() const {
        if (get_type() != ValueType::INT32) throw std::runtime_error("Attempted to read int32_t from a non-int32_t value_t.");
        uint64_t original_data = data >> 2;
        return static_cast<int32_t>(original_data);
    }

    Smart_string get_string() const {
        if (get_type() != ValueType::SMART_STRING) throw std::runtime_error("Attempted to read smart string from a non-smart-string value_t.");
        return Smart_string(data);
    }
};