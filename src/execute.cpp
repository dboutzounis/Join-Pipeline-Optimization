#include <column_t.h>
#include <hardware.h>
#include <hash_algo.h>
#include <inner_column.h>
#include <materialization.h>
#include <plan.h>
#include <table.h>
#include <unchained.h>

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
        Unchained hash_table;
        ExecuteResult& build_table = build_left ? left : right;
        ExecuteResult& probe_table = build_left ? right : left;
        size_t build_col = build_left ? left_col : right_col;
        size_t probe_col = build_left ? right_col : left_col;

        const size_t build_rows = build_table.empty() ? 0 : build_table[0].size();

        uint32_t num_threads = static_cast<uint32_t>(std::thread::hardware_concurrency());
        if (num_threads == 0) num_threads = 1;

        uint32_t num_partitions = 1;
        uint32_t target = num_threads * 4;
        while (num_partitions < target) num_partitions <<= 1;

        auto collected = collect_build_tuples(build_table[build_col], build_rows, num_threads, num_partitions);
        auto params_per_partition = hash_table.counting_per_partition(collected);

        uint64_t total_tuples = 0;

        for (const auto& p : params_per_partition) total_tuples += p.tuple_count;
        hash_table.allocate_tuple_storage(total_tuples);

        std::vector<std::thread> workers_build;
        workers_build.reserve(num_threads);
        std::atomic<uint32_t> partition_idx{0};

        for (uint32_t tid = 0; tid < num_threads; tid++) {
            workers_build.emplace_back([&, tid]() {
                while (true) {
                    uint32_t p = partition_idx.fetch_add(1);
                    if (p >= num_partitions) break;

                    hash_table.post_process_build(collected, params_per_partition[p], p);
                }
            });
        }

        for (auto& t : workers_build) t.join();

        std::free(collected.global_base);

        std::vector<ExecuteResult> local_results(num_threads);

        for (auto& res : local_results) {
            res.resize(output_attrs.size());
        }

        std::atomic<size_t> next_probe{0};
        std::vector<std::thread> workers_probe;
        workers_probe.reserve(num_threads);

        const size_t probe_size = probe_table[0].size();
        size_t chunk = std::max<size_t>(1, (probe_size + num_threads * 4 - 1) / (num_threads * 4));

        for (size_t tid = 0; tid < num_threads; ++tid) {
            workers_probe.emplace_back(&JoinAlgorithm::probe_worker<int32_t>, this, std::ref(next_probe), std::ref(local_results[tid]), std::ref(left),
                                       std::ref(right), std::ref(build_table), std::ref(probe_table), std::ref(hash_table), std::cref(output_attrs), probe_col,
                                       build_left, probe_size, chunk);
        }
        for (auto& t : workers_probe) t.join();

        for (size_t tid = 0; tid < num_threads; ++tid) {
            auto& src = local_results[tid];

            for (size_t col = 0; col < src.size(); ++col) {
                const size_t n = src[col].size();
                for (size_t i = 0; i < n; ++i) {
                    results[col].push_back(src[col].get_at(i));
                }
            }
        }
        return &results;
    }
    template <class T>
    void probe_worker(std::atomic<size_t>& next_probe, ExecuteResult& local_results, ExecuteResult& left, ExecuteResult& right, ExecuteResult& build_table,
                      ExecuteResult& probe_table, Unchained& hash_table, const std::vector<std::tuple<size_t, DataType>>& output_attrs, size_t probe_col,
                      size_t build_left, size_t probe_size, size_t chunk) {
        while (true) {
            size_t start = next_probe.fetch_add(chunk);
            if (start >= probe_size) break;

            size_t end = std::min(start + chunk, probe_size);

            for (size_t probe_idx = start; probe_idx < end; probe_idx++) {
                auto smart_key = extract_key<T>(probe_table[probe_col].get_at(probe_idx));
                if (!smart_key) continue;
                const T& key = *smart_key;
                if (auto itr_vec = hash_table.lookup(static_cast<int32_t>(key)); itr_vec.size() != 0) {
                    for (auto build_idx : itr_vec) {
                        size_t out_idx = 0;
                        for (auto [col_idx, _] : output_attrs) {
                            if (col_idx < left.size()) {
                                size_t row = build_left ? build_idx : probe_idx;
                                local_results[out_idx++].push_back(left[col_idx].get_at(row));
                            } else {
                                size_t row = build_left ? probe_idx : build_idx;
                                local_results[out_idx++].push_back(right[col_idx - left.size()].get_at(row));
                            }
                        }
                    }
                }
            }
        }
    }
};

template <>
std::optional<int32_t> JoinAlgorithm::extract_key<int32_t>(const value_t& v) const {
    if (v.is_null()) return std::nullopt;
    if (v.get_type() != ValueType::INT32) return std::nullopt;
    return v.get_int32();
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
    ExecuteResult results(output_attrs.size());

    JoinAlgorithm join_algorithm{.build_left = join.build_left,
                                 .plan = plan,
                                 .left = left,
                                 .right = right,
                                 .results = results,
                                 .left_col = join.left_attr,
                                 .right_col = join.right_attr,
                                 .output_attrs = output_attrs};

    join_algorithm.run<int32_t>();

    return results;
}

bool get_bitmap(const uint8_t* bitmap, uint16_t idx) {
    auto byte_idx = idx / 8;
    auto bit = idx % 8;
    return bitmap[byte_idx] & (1u << bit);
}

static bool column_has_nulls(const Column& column) {
    for (const auto& page_ptr : column.pages) {
        auto* page = page_ptr->data;

        uint16_t num_rows = *reinterpret_cast<uint16_t*>(page + 0);
        uint16_t num_non_null = *reinterpret_cast<uint16_t*>(page + 2);

        if (num_non_null < num_rows) {
            return true;
        }
    }
    return false;
}

ExecuteResult copy_scan_materialization(const Plan& plan, const ColumnarTable& table, const std::vector<std::tuple<size_t, DataType>>& output_attrs,
                                        uint8_t table_id) {
    ExecuteResult results;
    results.reserve(output_attrs.size());

    std::vector<bool> has_nulls(output_attrs.size());

    // detect collumns with nulls
    for (size_t i = 0; i < output_attrs.size(); ++i) {
        size_t in_col_idx = std::get<0>(output_attrs[i]);
        const auto& column = table.columns[in_col_idx];

        if (column.type == DataType::INT32) {
            has_nulls[i] = column_has_nulls(column);
        } else {
            has_nulls[i] = true;
        }

        if (column.type == DataType::INT32 && !has_nulls[i]) {
            results.emplace_back(ColumnStorage::PageOwned, table.num_rows);
        } else {
            results.emplace_back(ColumnStorage::ValueOwned, table.num_rows);
        }
    }

    auto task = [&](size_t begin, size_t end) {
        value_t v;

        for (size_t column_idx = begin; column_idx < end; ++column_idx) {
            size_t in_col_idx = std::get<0>(output_attrs[column_idx]);
            const auto& column = table.columns[in_col_idx];

            for (size_t page_id = 0; page_id < column.pages.size(); ++page_id) {
                auto* page = column.pages[page_id]->data;

                switch (column.type) {
                    case DataType::INT32: {
                        uint16_t num_rows = *reinterpret_cast<uint16_t*>(page + 0);
                        auto* data_begin = reinterpret_cast<int32_t*>(page + 4);

                        if (!has_nulls[column_idx]) {
                            results[column_idx].push_page(data_begin, num_rows);
                            break;
                        }

                        auto* bitmap = reinterpret_cast<uint8_t*>(page + PAGE_SIZE - (num_rows + 7) / 8);

                        uint16_t data_idx = 0;
                        for (uint16_t i = 0; i < num_rows; ++i) {
                            if (get_bitmap(bitmap, i)) {
                                results[column_idx].push_back(v.from_int32(data_begin[data_idx++]));
                            } else {
                                results[column_idx].push_back(v.null_value());
                            }
                        }
                        break;
                    }

                    case DataType::VARCHAR: {
                        uint16_t num_rows = *reinterpret_cast<uint16_t*>(page + 0);

                        if (num_rows == 0xffff) {
                            Smart_string s;
                            s = s.encode(table_id, in_col_idx, page_id, 0);
                            results[column_idx].push_back(v.from_string(s));
                            break;
                        }

                        if (num_rows == 0xfffe) {
                            continue;
                        }

                        uint16_t num_non_null = *reinterpret_cast<uint16_t*>(page + 2);
                        auto* offset_begin = reinterpret_cast<uint16_t*>(page + 4);
                        auto* data_begin = reinterpret_cast<char*>(page + 4 + num_non_null * 2);
                        auto* string_begin = data_begin;
                        auto* bitmap = reinterpret_cast<uint8_t*>(page + PAGE_SIZE - (num_rows + 7) / 8);

                        uint16_t data_idx = 0;
                        for (uint16_t i = 0; i < num_rows; ++i) {
                            if (get_bitmap(bitmap, i)) {
                                uint16_t offset = offset_begin[data_idx];

                                Smart_string s;
                                s = s.encode(table_id, in_col_idx, page_id, data_idx++);
                                results[column_idx].push_back(v.from_string(s));

                                string_begin += offset;
                            } else {
                                results[column_idx].push_back(v.null_value());
                            }
                        }
                        break;
                    }

                    default:
                        break;
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
    ret.num_rows = result.empty() ? 0 : result[0].size();
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
                for (size_t index = 0; index < result[col_idx].size(); index++) {
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
                for (size_t index = 0; index < result[col_idx].size(); index++) {
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
            case DataType::INT64:
                break;
            case DataType::FP64:
                break;
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