#include <hardware.h>
#include <hash_algo.h>
#include <inner_column.h>
#include <materialization.h>
#include <plan.h>
#include <table.h>

namespace Contest {

using ExecuteResult = std::vector<std::vector<value_t>>;

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
            for (auto&& [idx, record] : left | views::enumerate) {
                auto smart_key = extract_key<T>(record[left_col]);
                if (!smart_key) continue;
                const T& key = *smart_key;
                if (auto& itr_vec = hash_table.find(key); itr_vec.size() == 0) {
                    hash_table.emplace(key, std::vector<size_t>(1, idx));
                } else {
                    itr_vec.push_back(idx);
                }
            }
            for (auto& right_record : right) {
                auto smart_key = extract_key<T>(right_record[right_col]);
                if (!smart_key) continue;
                const T& key = *smart_key;
                if (auto& itr_vec = hash_table.find(key); itr_vec.size() != 0) {
                    for (auto left_idx : itr_vec) {
                        auto& left_record = left[left_idx];
                        std::vector<value_t> new_record;
                        new_record.reserve(output_attrs.size());
                        for (auto [col_idx, _] : output_attrs) {
                            if (col_idx < left_record.size()) {
                                new_record.emplace_back(left_record[col_idx]);
                            } else {
                                new_record.emplace_back(right_record[col_idx - left_record.size()]);
                            }
                        }
                        results.emplace_back(std::move(new_record));
                    }
                }
            }
        } else {
            for (auto&& [idx, record] : right | views::enumerate) {
                auto smart_key = extract_key<T>(record[right_col]);
                if (!smart_key) continue;
                const T& key = *smart_key;
                if (auto& itr_vec = hash_table.find(key); itr_vec.size() == 0) {
                    hash_table.emplace(key, std::vector<size_t>(1, idx));
                } else {
                    itr_vec.push_back(idx);
                }
            }
            for (auto& left_record : left) {
                auto smart_key = extract_key<T>(left_record[left_col]);
                if (!smart_key) continue;
                const T& key = *smart_key;
                if (auto& itr_vec = hash_table.find(key); itr_vec.size() != 0) {
                    for (auto right_idx : itr_vec) {
                        auto& right_record = right[right_idx];
                        std::vector<value_t> new_record;
                        new_record.reserve(output_attrs.size());
                        for (auto [col_idx, _] : output_attrs) {
                            if (col_idx < left_record.size()) {
                                new_record.emplace_back(left_record[col_idx]);
                            } else {
                                new_record.emplace_back(right_record[col_idx - left_record.size()]);
                            }
                        }
                        results.emplace_back(std::move(new_record));
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
    std::vector<std::vector<value_t>> results;

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

std::vector<std::vector<value_t>> copy_scan_materialization(const Plan& plan, const ColumnarTable& table,
                                                            const std::vector<std::tuple<size_t, DataType>>& output_attrs, uint8_t table_id) {
    namespace views = ranges::views;
    std::vector<std::vector<value_t>> results(table.num_rows, std::vector<value_t>(output_attrs.size(), value_t{}));
    std::vector<DataType> types(table.columns.size());
    auto task = [&](size_t begin, size_t end) {
        size_t col_pap = 0;
        for (size_t column_idx = begin; column_idx < end; ++column_idx) {
            size_t in_col_idx = std::get<0>(output_attrs[column_idx]);
            auto& column = table.columns[in_col_idx];
            types[in_col_idx] = column.type;
            size_t row_idx = 0;
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
                                if (row_idx >= table.num_rows) {
                                    throw std::runtime_error("row_idx");
                                }
                                results[row_idx++][column_idx] = value_t::from_int32(value);
                            } else {
                                ++row_idx;
                            }
                        }
                        break;
                    }
                    case DataType::VARCHAR: {
                        auto num_rows = *reinterpret_cast<uint16_t*>(page);
                        if (num_rows == 0xffff) {
                            auto num_chars = *reinterpret_cast<uint16_t*>(page + 2);
                            auto* data_begin = reinterpret_cast<char*>(page + 4);
                            if (row_idx >= table.num_rows) {
                                throw std::runtime_error("row_idx");
                            }

                            Smart_string smart_string;
                            smart_string = Smart_string::encode(table_id, in_col_idx, page_id, 0);
                            results[row_idx++][column_idx] = value_t::from_string(smart_string);
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
                                    if (row_idx >= table.num_rows) {
                                        throw std::runtime_error("row_idx");
                                    }

                                    Smart_string smart_string;
                                    smart_string = Smart_string::encode(table_id, in_col_idx, page_id, data_idx++);
                                    results[row_idx++][column_idx] = value_t::from_string(smart_string);
                                } else {
                                    ++row_idx;
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

std::vector<std::vector<Data>> convert_from_value_t_to_Data(const Plan& plan, const ExecuteResult& result, const std::vector<DataType>& ret_types) {
    if (result.empty()) return {};

    size_t rows = result.size();
    size_t cols = result[0].size();

    if (ret_types.size() != cols) throw std::runtime_error("convert_from_value_t_to_Data: ret_types size mismatch");

    std::vector<std::vector<Data>> transformed_results(rows, std::vector<Data>(cols, std::monostate{}));

    for (size_t i = 0; i < rows; ++i) {
        if (result[i].size() != cols) throw std::runtime_error("convert_from_value_t_to_Data: inconsistent row widths");
        for (size_t j = 0; j < cols; ++j) {
            const value_t& v = result[i][j];
            if (v.is_null()) continue;

            if (ret_types[j] == DataType::INT32) {
                int32_t val = v.get_int32();
                transformed_results[i][j].emplace<int32_t>(val);
            } else if (ret_types[j] == DataType::VARCHAR) {
                Smart_string s = v.get_string();
                std::string materialized = s.get_value(plan);
                transformed_results[i][j].emplace<std::string>(std::move(materialized));
            } else {
                throw std::runtime_error("convert_from_value_t_to_Data: unsupported DataType in ret_types");
            }
        }
    }

    return transformed_results;
}

ColumnarTable execute(const Plan& plan, [[maybe_unused]] void* context) {
    namespace views = ranges::views;
    auto ret = execute_impl(plan, plan.root);
    auto ret_types = plan.nodes[plan.root].output_attrs | views::transform([](const auto& v) { return std::get<1>(v); }) | ranges::to<std::vector<DataType>>();
    auto trans_ret = convert_from_value_t_to_Data(plan, ret, ret_types);
    Table table{std::move(trans_ret), std::move(ret_types)};
    return table.to_columnar();
}

void* build_context() { return nullptr; }

void destroy_context([[maybe_unused]] void* context) {}

}  // namespace Contest
