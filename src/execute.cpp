#include <column_t.h>
#include <hardware.h>
#include <hash_algo.h>
#include <inner_column.h>
#include <materialization.h>
#include <plan.h>
#include <table.h>

namespace Contest {

using ExecuteResult = std::vector<Column_t>;

ExecuteResult execute_impl(const Plan& plan, size_t node_idx);

struct JoinAlgorithm {
    bool build_left;
    const Plan& plan;
    ExecuteResult& left;
    ExecuteResult& right;
    ExecuteResult& results;
    size_t left_col, right_col;
    const std::vector<std::tuple<size_t, DataType>>& output_attrs;

    template <class T>
    std::optional<T> extract_key(const value_t& v) const {
        if (v.is_null()) return std::nullopt;
        return std::nullopt;
    }

    template <class T>
    auto run() {
        namespace views = ranges::views;
        HASH_ALGO_TYPE<T, std::vector<size_t>> hash_table;
        if (build_left) {
            for (size_t idx = 0; idx < left[0].total_size; idx++) {
                value_t val = left[left_col].get_at(idx);

                auto smart_key = extract_key<T>(val);
                if (!smart_key) continue;

                const T& key = *smart_key;

                if (auto& itr_vec = hash_table.find(key); itr_vec.size() == 0) {
                    hash_table.emplace(key, std::vector<size_t>(1, idx));
                } else {
                    itr_vec.push_back(idx);
                }
            }
            for (size_t right_idx = 0; right_idx < right[0].total_size; right_idx++) {
                auto smart_key = extract_key<T>(right[right_col].get_at(right_idx));
                if (!smart_key) continue;
                const T& key = *smart_key;
                if (auto& itr_vec = hash_table.find(key); itr_vec.size() != 0) {
                    for (auto left_idx : itr_vec) {
                        size_t out_idx = 0;
                        for (auto [col_idx, _] : output_attrs) {
                            if (col_idx < left.size()) {
                                results[out_idx++].push_back(left[col_idx].get_at(left_idx));
                            } else {
                                results[out_idx++].push_back(right[col_idx - left.size()].get_at(right_idx));
                            }
                        }
                    }
                }
            }
        } else {
            for (size_t idx = 0; idx < right[0].total_size; idx++) {
                value_t val = right[right_col].get_at(idx);
                auto smart_key = extract_key<T>(val);
                if (!smart_key) continue;
                const T& key = *smart_key;
                if (auto& itr_vec = hash_table.find(key); itr_vec.size() == 0) {
                    hash_table.emplace(key, std::vector<size_t>(1, idx));
                } else {
                    itr_vec.push_back(idx);
                }
            }
            for (size_t left_idx = 0; left_idx < left[0].total_size; left_idx++) {
                auto smart_key = extract_key<T>(left[left_col].get_at(left_idx));
                if (!smart_key) continue;
                const T& key = *smart_key;
                if (auto& itr_vec = hash_table.find(key); itr_vec.size() != 0) {
                    for (auto right_idx : itr_vec) {
                        size_t out_idx = 0;
                        for (auto [col_idx, _] : output_attrs) {
                            if (col_idx < left.size()) {
                                results[out_idx++].push_back(left[col_idx].get_at(left_idx));
                            } else {
                                results[out_idx++].push_back(right[col_idx - left.size()].get_at(right_idx));
                            }
                        }
                    }
                }
            }
        }
    }
};

template <>
inline std::optional<int32_t> JoinAlgorithm::extract_key<int32_t>(const value_t& v) const {
    if (v.is_null()) return std::nullopt;
    if (v.get_type() != ValueType::INT32) return std::nullopt;
    return v.get_int32();
}

template <>
inline std::optional<std::string> JoinAlgorithm::extract_key<std::string>(const value_t& v) const {
    if (v.is_null()) return std::nullopt;
    if (v.get_type() != ValueType::SMART_STRING) return std::nullopt;
    Smart_string s = v.get_string();
    return s.get_value(plan);
}

ExecuteResult execute_hash_join(const Plan& plan, const JoinNode& join, const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    auto left_idx = join.left;
    auto right_idx = join.right;
    auto& left_node = plan.nodes[left_idx];
    auto& right_node = plan.nodes[right_idx];
    auto& left_types = left_node.output_attrs;
    auto& right_types = right_node.output_attrs;
    auto left = execute_impl(plan, left_idx);
    auto right = execute_impl(plan, right_idx);
    std::vector<Column_t> results(output_attrs.size());

    JoinAlgorithm join_algorithm{.build_left = join.build_left,
                                 .plan = plan,
                                 .left = left,
                                 .right = right,
                                 .results = results,
                                 .left_col = join.left_attr,
                                 .right_col = join.right_attr,
                                 .output_attrs = output_attrs};
    if (join.build_left) {
        switch (std::get<1>(left_types[join.left_attr])) {
            case DataType::INT32:
                join_algorithm.run<int32_t>();
                break;
            case DataType::VARCHAR:
                join_algorithm.run<std::string>();
                break;
        }
    } else {
        switch (std::get<1>(right_types[join.right_attr])) {
            case DataType::INT32:
                join_algorithm.run<int32_t>();
                break;
            case DataType::VARCHAR:
                join_algorithm.run<std::string>();
                break;
        }
    }

    return results;
}

bool get_bitmap(const uint8_t* bitmap, uint16_t idx) {
    auto byte_idx = idx / 8;
    auto bit = idx % 8;
    return bitmap[byte_idx] & (1u << bit);
}

std::vector<Column_t> copy_scan_materialization(const Plan& plan, const ColumnarTable& table, const std::vector<std::tuple<size_t, DataType>>& output_attrs,
                                                uint8_t table_id) {
    namespace views = ranges::views;
    std::vector<Column_t> results(output_attrs.size(), Column_t(table.num_rows));
    std::vector<DataType> types(table.columns.size());
    auto task = [&](size_t begin, size_t end) {
        size_t col_pap = 0;
        value_t v;
        for (size_t column_idx = begin; column_idx < end; ++column_idx) {
            size_t in_col_idx = std::get<0>(output_attrs[column_idx]);
            auto& column = table.columns[in_col_idx];
            types[in_col_idx] = column.type;
            for (uint32_t page_id = 0; page_id < column.pages.size(); page_id++) {
                auto* page = column.pages[page_id]->data;
                switch (column.type) {
                    case DataType::INT32: {
                        auto num_rows = *reinterpret_cast<uint16_t*>(page);
                        auto* data_begin = reinterpret_cast<int32_t*>(page + 4);
                        auto* bitmap = reinterpret_cast<uint8_t*>(page + PAGE_SIZE - (num_rows + 7) / 8);
                        uint16_t data_idx = 0;
                        for (uint16_t i = 0; i < num_rows; ++i) {
                            if (get_bitmap(bitmap, i)) {
                                auto value = data_begin[data_idx++];
                                results[column_idx].push_back(v.from_int32(value));
                            } else {
                                results[column_idx].push_back(v.null_value());
                            }
                        }
                        break;
                    }
                    case DataType::VARCHAR: {
                        auto num_rows = *reinterpret_cast<uint16_t*>(page);
                        if (num_rows == 0xffff) {
                            auto num_chars = *reinterpret_cast<uint16_t*>(page + 2);
                            auto* data_begin = reinterpret_cast<char*>(page + 4);

                            Smart_string smart_string;
                            smart_string = smart_string.encode(table_id, in_col_idx, page_id, 0);
                            results[column_idx].push_back(v.from_string(smart_string));
                        } else if (num_rows == 0xfffe) {
                            continue;
                        } else {
                            auto num_non_null = *reinterpret_cast<uint16_t*>(page + 2);
                            auto* offset_begin = reinterpret_cast<uint16_t*>(page + 4);
                            auto* data_begin = reinterpret_cast<char*>(page + 4 + num_non_null * 2);
                            auto* string_begin = data_begin;
                            auto* bitmap = reinterpret_cast<uint8_t*>(page + PAGE_SIZE - (num_rows + 7) / 8);
                            uint16_t data_idx = 0;
                            for (uint16_t i = 0; i < num_rows; ++i) {
                                if (get_bitmap(bitmap, i)) {
                                    auto offset = offset_begin[data_idx];
                                    std::string value{string_begin, data_begin + offset};
                                    string_begin = data_begin + offset;

                                    Smart_string smart_string;
                                    smart_string = smart_string.encode(table_id, in_col_idx, page_id, data_idx++);
                                    results[column_idx].push_back(v.from_string(smart_string));
                                } else {
                                    results[column_idx].push_back(v.null_value());
                                }
                            }
                        }
                        break;
                    }
                }
            }
        }
    };
    filter_tp.run(task, output_attrs.size());
    return results;
}

ExecuteResult execute_scan(const Plan& plan, const ScanNode& scan, const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    auto table_id = scan.base_table_id;
    auto& input = plan.inputs[table_id];
    return copy_scan_materialization(plan, input, output_attrs, table_id);
}

ExecuteResult execute_impl(const Plan& plan, size_t node_idx) {
    auto& node = plan.nodes[node_idx];
    return std::visit(
        [&](const auto& value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, JoinNode>) {
                return execute_hash_join(plan, value, node.output_attrs);
            } else {
                return execute_scan(plan, value, node.output_attrs);
            }
        },
        node.data);
}

void set_bitmap(std::vector<uint8_t>& bitmap, uint16_t idx) {
    while (bitmap.size() < idx / 8 + 1) {
        bitmap.emplace_back(0);
    }
    auto byte_idx = idx / 8;
    auto bit = idx % 8;
    bitmap[byte_idx] |= (1u << bit);
}

void unset_bitmap(std::vector<uint8_t>& bitmap, uint16_t idx) {
    while (bitmap.size() < idx / 8 + 1) {
        bitmap.emplace_back(0);
    }
    auto byte_idx = idx / 8;
    auto bit = idx % 8;
    bitmap[byte_idx] &= ~(1u << bit);
}

ColumnarTable to_columnar(const Plan& plan, const ExecuteResult& result, const std::vector<DataType>& ret_types) {
    namespace views = ranges::views;
    ColumnarTable ret;
    ret.num_rows = result.empty() ? 0 : result[0].total_size;
    for (auto [col_idx, data_type] : ret_types | views::enumerate) {
        ret.columns.emplace_back(data_type);
        auto& column = ret.columns.back();
        switch (data_type) {
            case DataType::INT32: {
                uint16_t num_rows = 0;
                std::vector<int32_t> data;
                std::vector<uint8_t> bitmap;
                data.reserve(2048);
                bitmap.reserve(256);
                auto save_page = [&column, &num_rows, &data, &bitmap]() {
                    auto* page = column.new_page()->data;
                    *reinterpret_cast<uint16_t*>(page) = num_rows;
                    *reinterpret_cast<uint16_t*>(page + 2) = static_cast<uint16_t>(data.size());
                    memcpy(page + 4, data.data(), data.size() * 4);
                    memcpy(page + PAGE_SIZE - bitmap.size(), bitmap.data(), bitmap.size());
                    num_rows = 0;
                    data.clear();
                    bitmap.clear();
                };
                for (size_t index = 0; index < result[col_idx].total_size; index++) {
                    auto value = result[col_idx].get_at(index);
                    if (value.get_type() == ValueType::INT32) {
                        if (4 + (data.size() + 1) * 4 + (num_rows / 8 + 1) > PAGE_SIZE) {
                            save_page();
                        }
                        set_bitmap(bitmap, num_rows);
                        data.emplace_back(value.get_int32());
                        ++num_rows;
                    } else if (value.is_null()) {
                        if (4 + (data.size()) * 4 + (num_rows / 8 + 1) > PAGE_SIZE) {
                            save_page();
                        }
                        unset_bitmap(bitmap, num_rows);
                        ++num_rows;
                    }
                }
                if (num_rows != 0) {
                    save_page();
                }
                break;
            }
            case DataType::VARCHAR: {
                uint16_t num_rows = 0;
                std::vector<char> data;
                std::vector<uint16_t> offsets;
                std::vector<uint8_t> bitmap;
                data.reserve(8192);
                offsets.reserve(4096);
                bitmap.reserve(512);
                auto save_long_string = [&column](std::string_view data) {
                    size_t offset = 0;
                    auto first_page = true;
                    while (offset < data.size()) {
                        auto* page = column.new_page()->data;
                        if (first_page) {
                            *reinterpret_cast<uint16_t*>(page) = 0xffff;
                            first_page = false;
                        } else {
                            *reinterpret_cast<uint16_t*>(page) = 0xfffe;
                        }
                        auto page_data_len = std::min(data.size() - offset, PAGE_SIZE - 4);
                        *reinterpret_cast<uint16_t*>(page + 2) = page_data_len;
                        memcpy(page + 4, data.data() + offset, page_data_len);
                        offset += page_data_len;
                    }
                };
                auto save_page = [&column, &num_rows, &data, &offsets, &bitmap]() {
                    auto* page = column.new_page()->data;
                    *reinterpret_cast<uint16_t*>(page) = num_rows;
                    *reinterpret_cast<uint16_t*>(page + 2) = static_cast<uint16_t>(offsets.size());
                    memcpy(page + 4, offsets.data(), offsets.size() * 2);
                    memcpy(page + 4 + offsets.size() * 2, data.data(), data.size());
                    memcpy(page + PAGE_SIZE - bitmap.size(), bitmap.data(), bitmap.size());
                    num_rows = 0;
                    data.clear();
                    offsets.clear();
                    bitmap.clear();
                };
                for (size_t index = 0; index < result[col_idx].total_size; index++) {
                    auto value = result[col_idx].get_at(index);
                    if (value.get_type() == ValueType::SMART_STRING) {
                        std::string str = value.get_string().get_value(plan);
                        if (str.size() > PAGE_SIZE - 7) {
                            if (num_rows > 0) {
                                save_page();
                            }
                            save_long_string(str);
                        } else {
                            if (4 + (offsets.size() + 1) * 2 + (data.size() + str.size()) + (num_rows / 8 + 1) > PAGE_SIZE) {
                                save_page();
                            }
                            set_bitmap(bitmap, num_rows);
                            data.insert(data.end(), str.begin(), str.end());
                            offsets.emplace_back(data.size());
                            ++num_rows;
                        }
                    } else if (value.is_null()) {
                        if (4 + offsets.size() * 2 + data.size() + (num_rows / 8 + 1) > PAGE_SIZE) {
                            save_page();
                        }
                        unset_bitmap(bitmap, num_rows);
                        ++num_rows;
                    } else {
                        throw std::runtime_error("not string or null");
                    }
                }
                if (num_rows != 0) {
                    save_page();
                }
                break;
            }
        }
    }
    return ret;
}

ColumnarTable execute(const Plan& plan, [[maybe_unused]] void* context) {
    namespace views = ranges::views;
    auto ret = execute_impl(plan, plan.root);
    auto ret_types = plan.nodes[plan.root].output_attrs | views::transform([](const auto& v) { return std::get<1>(v); }) | ranges::to<std::vector<DataType>>();
    return to_columnar(plan, ret, ret_types);
}

void* build_context() { return nullptr; }

void destroy_context([[maybe_unused]] void* context) {}

};  // namespace Contest