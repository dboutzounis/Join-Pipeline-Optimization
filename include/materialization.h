#pragma once

#include <plan.h>

#include <cstdint>

struct Smart_string {
    uint8_t table_id;
    uint8_t column_id;
    uint16_t page_id;
    uint16_t offset_idx;

    Smart_string(uint8_t table_id = 0xff, uint8_t column_id = 0xff, uint16_t page_id = 0xffff, uint16_t offset_idx = 0xffff)
        : table_id(table_id), column_id(column_id), page_id(page_id), offset_idx(offset_idx) {}

    std::string get_value(const Plan& plan) {
        auto& input = plan.inputs[table_id];
        auto& column = input.columns[column_id];
        auto* page = column.pages[page_id]->data;

        if (offset_idx == 0xffff) return std::string();

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
        } else {
            auto num_non_null = *reinterpret_cast<uint16_t*>(page + 2);
            auto* offset_begin = reinterpret_cast<uint16_t*>(page + 4);
            auto* data_begin = reinterpret_cast<char*>(page + 4 + num_non_null * 2);
            auto string_begin = offset_idx == 0 ? 0 : offset_begin[offset_idx - 1];
            auto offset = offset_begin[offset_idx];
            std::string value{data_begin + string_begin, data_begin + offset};
            return value;
        }
    }
};

enum class ValueType : uint8_t { INT32, SMART_STRING, NONE };

struct value_t {
    ValueType type;

    union {
        int32_t i32;
        Smart_string stringref;
    };

    value_t() : type(ValueType::NONE) {}

    static value_t from_int32(int32_t v) {
        value_t x;
        x.type = ValueType::INT32;
        x.i32 = v;
        return x;
    }

    static value_t from_string(Smart_string v) {
        value_t x;
        x.type = ValueType::SMART_STRING;
        x.stringref = v;
        return x;
    }
};
