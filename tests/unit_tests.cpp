#include <plan.h>
#include <table.h>

#include <atomic>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <thread>

#include "hash_algo.h"

void sort(std::vector<std::vector<Data>>& table) { std::sort(table.begin(), table.end()); }

TEST_CASE("Empty join", "[join]") {
    Plan plan;
    plan.new_scan_node(0, {{0, DataType::INT32}});
    plan.new_scan_node(1, {{0, DataType::INT32}});
    plan.new_join_node(true, 0, 1, 0, 0, {{0, DataType::INT32}, {1, DataType::INT32}});
    ColumnarTable table1, table2;
    table1.columns.emplace_back(DataType::INT32);
    table2.columns.emplace_back(DataType::INT32);
    plan.inputs.emplace_back(std::move(table1));
    plan.inputs.emplace_back(std::move(table2));
    plan.root = 2;
    auto* context = Contest::build_context();
    auto result = Contest::execute(plan, context);
    Contest::destroy_context(context);
    REQUIRE(result.num_rows == 0);
    REQUIRE(result.columns.size() == 2);
    REQUIRE(result.columns[0].type == DataType::INT32);
    REQUIRE(result.columns[1].type == DataType::INT32);
}

TEST_CASE("One line join", "[join]") {
    Plan plan;
    plan.new_scan_node(0, {{0, DataType::INT32}});
    plan.new_scan_node(1, {{0, DataType::INT32}});
    plan.new_join_node(true, 0, 1, 0, 0, {{0, DataType::INT32}, {1, DataType::INT32}});
    std::vector<std::vector<Data>> data{
        {
            1,
        },
    };
    std::vector<DataType> types{DataType::INT32};
    Table table(std::move(data), std::move(types));
    ColumnarTable table1 = table.to_columnar();
    ColumnarTable table2 = table.to_columnar();
    plan.inputs.emplace_back(std::move(table1));
    plan.inputs.emplace_back(std::move(table2));
    plan.root = 2;
    auto* context = Contest::build_context();
    auto result = Contest::execute(plan, context);
    Contest::destroy_context(context);
    REQUIRE(result.num_rows == 1);
    REQUIRE(result.columns.size() == 2);
    REQUIRE(result.columns[0].type == DataType::INT32);
    REQUIRE(result.columns[1].type == DataType::INT32);
    auto result_table = Table::from_columnar(result);
    std::vector<std::vector<Data>> ground_truth{
        {
            1,
            1,
        },
    };
    REQUIRE(result_table.table() == ground_truth);
}

TEST_CASE("Simple join", "[join]") {
    Plan plan;
    plan.new_scan_node(0, {{0, DataType::INT32}});
    plan.new_scan_node(1, {{0, DataType::INT32}});
    plan.new_join_node(true, 0, 1, 0, 0, {{0, DataType::INT32}, {1, DataType::INT32}});
    std::vector<std::vector<Data>> data{
        {
            1,
        },
        {
            2,
        },
        {
            3,
        },
    };
    std::vector<DataType> types{DataType::INT32};
    Table table(std::move(data), std::move(types));
    ColumnarTable table1 = table.to_columnar();
    ColumnarTable table2 = table.to_columnar();
    plan.inputs.emplace_back(std::move(table1));
    plan.inputs.emplace_back(std::move(table2));
    plan.root = 2;
    auto* context = Contest::build_context();
    auto result = Contest::execute(plan, context);
    Contest::destroy_context(context);
    REQUIRE(result.num_rows == 3);
    REQUIRE(result.columns.size() == 2);
    REQUIRE(result.columns[0].type == DataType::INT32);
    REQUIRE(result.columns[1].type == DataType::INT32);
    auto result_table = Table::from_columnar(result);
    std::vector<std::vector<Data>> ground_truth{
        {
            1,
            1,
        },
        {
            2,
            2,
        },
        {
            3,
            3,
        },
    };
    sort(result_table.table());
    REQUIRE(result_table.table() == ground_truth);
}

TEST_CASE("Empty Result", "[join]") {
    Plan plan;
    plan.new_scan_node(0, {{0, DataType::INT32}});
    plan.new_scan_node(1, {{0, DataType::INT32}});
    plan.new_join_node(true, 0, 1, 0, 0, {{0, DataType::INT32}, {1, DataType::INT32}});
    std::vector<std::vector<Data>> data1{
        {
            1,
        },
        {
            2,
        },
        {
            3,
        },
    };
    std::vector<std::vector<Data>> data2{
        {
            4,
        },
        {
            5,
        },
        {
            6,
        },
    };
    std::vector<DataType> types{DataType::INT32};
    Table table1(std::move(data1), types);
    Table table2(std::move(data2), std::move(types));
    ColumnarTable input1 = table1.to_columnar();
    ColumnarTable input2 = table2.to_columnar();
    plan.inputs.emplace_back(std::move(input1));
    plan.inputs.emplace_back(std::move(input2));
    plan.root = 2;
    auto* context = Contest::build_context();
    auto result = Contest::execute(plan, context);
    Contest::destroy_context(context);
    REQUIRE(result.num_rows == 0);
    REQUIRE(result.columns.size() == 2);
    REQUIRE(result.columns[0].type == DataType::INT32);
    REQUIRE(result.columns[1].type == DataType::INT32);
}

TEST_CASE("Multiple same keys", "[join]") {
    Plan plan;
    plan.new_scan_node(0, {{0, DataType::INT32}});
    plan.new_scan_node(1, {{0, DataType::INT32}});
    plan.new_join_node(true, 0, 1, 0, 0, {{0, DataType::INT32}, {1, DataType::INT32}});
    std::vector<std::vector<Data>> data1{
        {
            1,
        },
        {
            1,
        },
        {
            2,
        },
        {
            3,
        },
    };
    std::vector<DataType> types{DataType::INT32};
    Table table1(std::move(data1), std::move(types));
    ColumnarTable input1 = table1.to_columnar();
    ColumnarTable input2 = table1.to_columnar();
    plan.inputs.emplace_back(std::move(input1));
    plan.inputs.emplace_back(std::move(input2));
    plan.root = 2;
    auto* context = Contest::build_context();
    auto result = Contest::execute(plan, context);
    Contest::destroy_context(context);
    REQUIRE(result.num_rows == 6);
    REQUIRE(result.columns.size() == 2);
    REQUIRE(result.columns[0].type == DataType::INT32);
    REQUIRE(result.columns[1].type == DataType::INT32);
    auto result_table = Table::from_columnar(result);
    std::vector<std::vector<Data>> ground_truth{
        {
            1,
            1,
        },
        {
            1,
            1,
        },
        {
            1,
            1,
        },
        {
            1,
            1,
        },
        {
            2,
            2,
        },
        {
            3,
            3,
        },
    };
    sort(result_table.table());
    REQUIRE(result_table.table() == ground_truth);
}

TEST_CASE("NULL keys", "[join]") {
    Plan plan;
    plan.new_scan_node(0, {{0, DataType::INT32}});
    plan.new_scan_node(1, {{0, DataType::INT32}});
    plan.new_join_node(true, 0, 1, 0, 0, {{0, DataType::INT32}, {1, DataType::INT32}});
    std::vector<std::vector<Data>> data1{
        {
            1,
        },
        {
            1,
        },
        {
            std::monostate{},
        },
        {
            2,
        },
        {
            3,
        },
    };
    std::vector<DataType> types{DataType::INT32};
    Table table1(std::move(data1), std::move(types));
    ColumnarTable input1 = table1.to_columnar();
    ColumnarTable input2 = table1.to_columnar();
    plan.inputs.emplace_back(std::move(input1));
    plan.inputs.emplace_back(std::move(input2));
    plan.root = 2;
    auto* context = Contest::build_context();
    auto result = Contest::execute(plan, context);
    Contest::destroy_context(context);
    REQUIRE(result.num_rows == 6);
    REQUIRE(result.columns.size() == 2);
    REQUIRE(result.columns[0].type == DataType::INT32);
    REQUIRE(result.columns[1].type == DataType::INT32);
    auto result_table = Table::from_columnar(result);
    std::vector<std::vector<Data>> ground_truth{
        {
            1,
            1,
        },
        {
            1,
            1,
        },
        {
            1,
            1,
        },
        {
            1,
            1,
        },
        {
            2,
            2,
        },
        {
            3,
            3,
        },
    };
    sort(result_table.table());
    REQUIRE(result_table.table() == ground_truth);
}

TEST_CASE("Multiple columns", "[join]") {
    Plan plan;
    plan.new_scan_node(0, {{0, DataType::INT32}});
    plan.new_scan_node(1, {{1, DataType::VARCHAR}, {0, DataType::INT32}});
    plan.new_join_node(true, 0, 1, 0, 1, {{0, DataType::INT32}, {2, DataType::INT32}, {1, DataType::VARCHAR}});
    using namespace std::string_literals;
    std::vector<std::vector<Data>> data1{
        {
            1,
            "xxx"s,
        },
        {
            1,
            "yyy"s,
        },
        {
            std::monostate{},
            "zzz"s,
        },
        {
            2,
            "uuu"s,
        },
        {
            3,
            "vvv"s,
        },
    };
    std::vector<DataType> types{DataType::INT32, DataType::VARCHAR};
    Table table1(std::move(data1), std::move(types));
    ColumnarTable input1 = table1.to_columnar();
    ColumnarTable input2 = table1.to_columnar();
    plan.inputs.emplace_back(std::move(input1));
    plan.inputs.emplace_back(std::move(input2));
    plan.root = 2;
    auto* context = Contest::build_context();
    auto result = Contest::execute(plan, context);
    Contest::destroy_context(context);
    REQUIRE(result.num_rows == 6);
    REQUIRE(result.columns.size() == 3);
    REQUIRE(result.columns[0].type == DataType::INT32);
    REQUIRE(result.columns[1].type == DataType::INT32);
    REQUIRE(result.columns[2].type == DataType::VARCHAR);
    auto result_table = Table::from_columnar(result);
    std::vector<std::vector<Data>> ground_truth{
        {1, 1, "xxx"s}, {1, 1, "xxx"s}, {1, 1, "yyy"s}, {1, 1, "yyy"s}, {2, 2, "uuu"s}, {3, 3, "vvv"s},
    };
    sort(result_table.table());
    REQUIRE(result_table.table() == ground_truth);
}

TEST_CASE("Build on right", "[join]") {
    Plan plan;
    plan.new_scan_node(0, {{0, DataType::INT32}});
    plan.new_scan_node(1, {{1, DataType::VARCHAR}, {0, DataType::INT32}});
    plan.new_join_node(false, 0, 1, 0, 1, {{0, DataType::INT32}, {2, DataType::INT32}, {1, DataType::VARCHAR}});
    using namespace std::string_literals;
    std::vector<std::vector<Data>> data1{
        {
            1,
            "xxx"s,
        },
        {
            1,
            "yyy"s,
        },
        {
            std::monostate{},
            "zzz"s,
        },
        {
            2,
            "uuu"s,
        },
        {
            3,
            "vvv"s,
        },
    };
    std::vector<DataType> types{DataType::INT32, DataType::VARCHAR};
    Table table1(std::move(data1), std::move(types));
    ColumnarTable input1 = table1.to_columnar();
    ColumnarTable input2 = table1.to_columnar();
    plan.inputs.emplace_back(std::move(input1));
    plan.inputs.emplace_back(std::move(input2));
    plan.root = 2;
    auto* context = Contest::build_context();
    auto result = Contest::execute(plan, context);
    Contest::destroy_context(context);
    REQUIRE(result.num_rows == 6);
    REQUIRE(result.columns.size() == 3);
    REQUIRE(result.columns[0].type == DataType::INT32);
    REQUIRE(result.columns[1].type == DataType::INT32);
    REQUIRE(result.columns[2].type == DataType::VARCHAR);
    auto result_table = Table::from_columnar(result);
    std::vector<std::vector<Data>> ground_truth{
        {1, 1, "xxx"s}, {1, 1, "xxx"s}, {1, 1, "yyy"s}, {1, 1, "yyy"s}, {2, 2, "uuu"s}, {3, 3, "vvv"s},
    };
    sort(result_table.table());
    REQUIRE(result_table.table() == ground_truth);
}

TEST_CASE("leftdeep 2-level join", "[join]") {
    Plan plan;
    plan.new_scan_node(0, {{0, DataType::INT32}});
    plan.new_scan_node(1, {{0, DataType::INT32}});
    plan.new_scan_node(2, {{0, DataType::INT32}});
    plan.new_join_node(true, 0, 1, 0, 0, {{0, DataType::INT32}, {1, DataType::INT32}});
    plan.new_join_node(false, 3, 2, 0, 0, {{0, DataType::INT32}, {1, DataType::INT32}, {2, DataType::INT32}});
    std::vector<std::vector<Data>> data{
        {
            1,
        },
        {
            2,
        },
        {
            3,
        },
    };
    std::vector<DataType> types{DataType::INT32};
    Table table(std::move(data), std::move(types));
    ColumnarTable table1 = table.to_columnar();
    ColumnarTable table2 = table.to_columnar();
    ColumnarTable table3 = table.to_columnar();
    plan.inputs.emplace_back(std::move(table1));
    plan.inputs.emplace_back(std::move(table2));
    plan.inputs.emplace_back(std::move(table3));
    plan.root = 4;
    auto* context = Contest::build_context();
    auto result = Contest::execute(plan, context);
    Contest::destroy_context(context);
    REQUIRE(result.num_rows == 3);
    REQUIRE(result.columns.size() == 3);
    REQUIRE(result.columns[0].type == DataType::INT32);
    REQUIRE(result.columns[1].type == DataType::INT32);
    REQUIRE(result.columns[2].type == DataType::INT32);
    auto result_table = Table::from_columnar(result);
    std::vector<std::vector<Data>> ground_truth{
        {
            1,
            1,
            1,
        },
        {
            2,
            2,
            2,
        },
        {
            3,
            3,
            3,
        },
    };
    sort(result_table.table());
    REQUIRE(result_table.table() == ground_truth);
}

TEST_CASE("3-way join", "[join]") {
    Plan plan;
    plan.new_scan_node(0, {{0, DataType::INT32}, {1, DataType::VARCHAR}});
    plan.new_scan_node(1, {{0, DataType::INT32}, {1, DataType::VARCHAR}});
    plan.new_scan_node(2, {{0, DataType::INT32}, {1, DataType::VARCHAR}});
    plan.new_join_node(false, 0, 1, 0, 0, {{0, DataType::INT32}, {1, DataType::VARCHAR}});
    plan.new_join_node(false, 3, 2, 0, 0, {{0, DataType::INT32}, {1, DataType::VARCHAR}, {3, DataType::VARCHAR}});
    using namespace std::string_literals;
    std::vector<std::vector<Data>> data1{
        {1, "a"s},
        {2, "b"s},
        {3, "c"s},
    };
    std::vector<std::vector<Data>> data2{
        {1, "x"s},
        {2, "y"s},
    };
    std::vector<std::vector<Data>> data3{
        {1, "u"s},
        {2, "v"s},
        {3, "w"s},
    };
    std::vector<DataType> types{DataType::INT32, DataType::VARCHAR};
    Table table1(std::move(data1), types);
    Table table2(std::move(data2), types);
    Table table3(std::move(data3), types);
    ColumnarTable input1 = table1.to_columnar();
    ColumnarTable input2 = table2.to_columnar();
    ColumnarTable input3 = table3.to_columnar();
    plan.inputs.emplace_back(std::move(input1));
    plan.inputs.emplace_back(std::move(input2));
    plan.inputs.emplace_back(std::move(input3));
    plan.root = 4;
    auto* context = Contest::build_context();
    auto result = Contest::execute(plan, context);
    Contest::destroy_context(context);
    REQUIRE(result.num_rows == 2);
    REQUIRE(result.columns.size() == 3);
    REQUIRE(result.columns[0].type == DataType::INT32);
    REQUIRE(result.columns[1].type == DataType::VARCHAR);
    REQUIRE(result.columns[2].type == DataType::VARCHAR);
    auto result_table = Table::from_columnar(result);
    std::vector<std::vector<Data>> ground_truth{
        {1, "a"s, "u"s},
        {2, "b"s, "v"s},
    };
    sort(result_table.table());
    REQUIRE(result_table.table() == ground_truth);
}


TEST_CASE("Robin Hood manual insertion and find verification", "[robin][robin_find]") {
    Robin_Hood<int,std::vector<int>> table(4);
    auto& ht = table.get_table();

    size_t idx1 = hash_index(1, ht.size());
    size_t idx2 = hash_index(2, ht.size());
    size_t idx3 = hash_index(3, ht.size());

    

    ht[idx1].assign (1, {10, 20}, 0);
    ht[idx2].assign (2, {30}, 0);
    ht[idx3].assign (3, {99, 100, 101}, 1);


    SECTION("Find existing keys manually placed") {
        auto& result1 = table.find(1);
        REQUIRE(result1.size() == 2);
        REQUIRE(result1[0] == 10);
        REQUIRE(result1[1] == 20);

        auto& result2 = table.find(2);
        REQUIRE(result2.size() == 1);
        REQUIRE(result2[0] == 30);

        auto& result3 = table.find(3);
        REQUIRE(result3.size() == 3);
        REQUIRE(result3[2] == 101);
    }

    SECTION("Find missing key returns dummy") {
        Robin_Hood<int, std::vector<size_t>> table(4);
        auto& result = table.find(999);
        REQUIRE(result.size() == 0);
    }
}
TEST_CASE("Robin Hood emplace basic behavior", "[robin][robin_emplace]") {
    SECTION("Insert single key-value pair") {
        Robin_Hood<int, std::vector<size_t>> hash_table(4);
        hash_table.emplace(1, {10, 20});
        auto& result = hash_table.find(1);
        REQUIRE(result.size() == 2);
        REQUIRE(result[0] == 10);
        REQUIRE(result[1] == 20);
    }

    SECTION("Insert multiple keys of char* type") {
        Robin_Hood<const char*, std::vector<size_t>> hash_table(8);

        hash_table.emplace("iasonas", {22});
        hash_table.emplace("kostis", {33});
        hash_table.emplace("antreas", {10, 20});

        auto& v1 = hash_table.find("iasonas");
        auto& v2 = hash_table.find("kostis");
        auto& v3 = hash_table.find("antreas");

        REQUIRE(v1.size() == 1);
        REQUIRE(v1[0] == 22);

        REQUIRE(v2.size() == 1);
        REQUIRE(v2[0] == 33);

        REQUIRE(v3.size() == 2);
        REQUIRE(v3[0] == 10);
        REQUIRE(v3[1] == 20);
    }
}

TEST_CASE("Robin Hood rehash basic growth and key preservation", "[robin][robin_rehash]") {
    Robin_Hood<int, std::vector<int>> table(4);

    size_t prev_capacity = table.get_table().size();

    std::vector<int> inserted_keys;

    // pushing to the limit.
    for (int i = 1; i <= 10000; i += 2) {
        inserted_keys.push_back(i);
        table.emplace(i, std::vector<int>{i});

        size_t new_capacity = table.get_table().size();

        if (new_capacity != prev_capacity) {
            std::cout << "Rehash triggered! "
                      << "Old: " << prev_capacity << " → New: " << new_capacity << "\n";
        }

        for (int key : inserted_keys) {
            auto& result = table.find(key);
            REQUIRE(result.size() == 1);
            REQUIRE(result[0] == key);
        }

        prev_capacity = new_capacity;
    }
}

TEST_CASE("Robin Hood handles collisions and preserves correctness", "[robin][robin_collision]") {
    Robin_Hood<int, std::vector<size_t>> table(8);
    auto& ht = table.get_table();

    int k1 = 0;
    int k2 = 8;
    int k3 = 16;

    table.emplace(k1, {11});
    table.emplace(k2, {22});
    table.emplace(k3, {33});

    auto& v1 = table.find(k1);
    auto& v2 = table.find(k2);
    auto& v3 = table.find(k3);

    REQUIRE(v1.size() == 1);
    REQUIRE(v2.size() == 1);
    REQUIRE(v3.size() == 1);

    REQUIRE(v1[0] == 11);
    REQUIRE(v2[0] == 22);
    REQUIRE(v3[0] == 33);

    bool found_k1 = false, found_k2 = false, found_k3 = false;
    for (auto& entry : ht) {
        if (entry.tsl == -1) continue;
        if (entry.key == k1) found_k1 = true;
        if (entry.key == k2) found_k2 = true;
        if (entry.key == k3) found_k3 = true;
    }
    REQUIRE(found_k1);
    REQUIRE(found_k2);
    REQUIRE(found_k3);

    std::cout << "\n--- Collision Test Table ---\n";
    table.print();
}

TEST_CASE("Hopscotch manual insertion and find function", "[hopscotch][hopscotch_find]") {
    Hopscotch<int, std::vector<size_t>> hop(4, 8);

    auto& hash_table = hop.get_hashtable();
    int key1 = 5;
    int key2 = 13;

    std::vector<size_t> val1 = {1, 2, 3};
    std::vector<size_t> val2 = {9, 8};

    size_t idx1 = hop.hash(key1);
    hash_table[idx1].key = key1;
    hash_table[idx1].value = val1;
    hash_table[idx1].occupied = true;
    hash_table[idx1].bitmap |= (1ull << 0);

    size_t idx2 = (idx1 + 1) % 8;
    hash_table[idx2].key = key2;
    hash_table[idx2].value = val2;
    hash_table[idx2].occupied = true;

    hash_table[idx1].bitmap |= (1ull << 1);

    SECTION("Find key that sits at base hash position") {
        auto& found1 = hop.find(key1);
        REQUIRE(found1.size() == val1.size());
        REQUIRE(found1[0] == 1);
        REQUIRE(found1[2] == 3);
    }

    SECTION("Find key that sits in neighborhood (collision case)") {
        auto& found2 = hop.find(key2);
        REQUIRE(found2.size() == val2.size());
        REQUIRE(found2[0] == 9);
        REQUIRE(found2[1] == 8);
    }

    SECTION("Find non-existing key returns dummy") {
        auto& found3 = hop.find(999);
        REQUIRE(found3.empty());
    }
}

TEST_CASE("Hopscotch emplace basic behavior", "[hopscotch][hopscotch_emplace]") {
    SECTION("Insert single key-value pair") {
        Hopscotch<int, std::vector<size_t>> hop(4, 8);
        hop.emplace(1, {10, 20});
        auto& result = hop.find(1);
        REQUIRE(result.size() == 2);
        REQUIRE(result[0] == 10);
        REQUIRE(result[1] == 20);
    }

    SECTION("Insert multiple keys without collisions") {
        Hopscotch<std::string, std::vector<size_t>> hop(4, 32);
        hop.emplace("iasonas", {22});
        auto& val1 = hop.find("iasonas");
        hop.print();
        REQUIRE_FALSE(val1.empty());
        REQUIRE(val1.size() == 1);
        REQUIRE(val1[0] == 22);
        hop.emplace("dimitris", {33});
        auto& val2 = hop.find("dimitris");
        hop.print();
        REQUIRE_FALSE(val2.empty());
        REQUIRE(val2.size() == 1);
        REQUIRE(val2[0] == 33);
        hop.emplace("spyros", {10, 20});
        auto& val3 = hop.find("spyros");
        hop.print();
        REQUIRE_FALSE(val3.empty());
        REQUIRE(val3.size() == 2);
        REQUIRE(val3[0] == 10);
        REQUIRE(val3[1] == 20);
    }
}

TEST_CASE("Hopscotch emplace with collisions", "[hopscotch][hopscotch_emplace_collision]") {
    Hopscotch<int, std::vector<size_t>> hop(4, 8);

    int k1 = 5;
    int k2 = 13;
    int k3 = 21;
    int k4 = 22;
    int k5 = 29;

    std::vector<size_t> v1 = {1};
    std::vector<size_t> v2 = {2};
    std::vector<size_t> v3 = {3};
    std::vector<size_t> v4 = {4};
    std::vector<size_t> v5 = {5};

    hop.emplace(k1, v1);
    hop.emplace(k2, v2);
    hop.emplace(k3, v3);
    REQUIRE(hop.find(k1)[0] == 1);
    REQUIRE(hop.find(k2)[0] == 2);
    REQUIRE(hop.find(k3)[0] == 3);

    hop.emplace(k4, v4);
    hop.print();
    hop.emplace(k5, v5);
    hop.print();
    REQUIRE(hop.find(k1)[0] == 1);
    REQUIRE(hop.find(k2)[0] == 2);
    REQUIRE(hop.find(k3)[0] == 3);
    REQUIRE(hop.find(k4)[0] == 4);
    REQUIRE(hop.find(k5)[0] == 5);
}

TEST_CASE("Hopscotch rehash test 1", "[hopscotch][hopscotch_rehash_1]") {
    Hopscotch<int, std::vector<size_t>> hop(2, 2);
    auto initial_hash_table = hop.get_hashtable();
    size_t initial_size = initial_hash_table.size();

    REQUIRE(initial_size == 2);

    int k1 = 5;
    int k2 = 13;
    int k3 = 21;

    std::vector<size_t> v1 = {1};
    std::vector<size_t> v2 = {2};
    std::vector<size_t> v3 = {3};

    hop.emplace(k1, v1);
    hop.emplace(k2, v2);
    hop.emplace(k3, v3);
    auto& found1 = hop.find(k1);
    auto& found2 = hop.find(k2);
    auto& found3 = hop.find(k3);
    REQUIRE(found1.size() == 1);
    REQUIRE(found1[0] == v1[0]);
    REQUIRE(found2.size() == 1);
    REQUIRE(found2[0] == v2[0]);
    REQUIRE(found3.size() == 1);
    REQUIRE(found3[0] == v3[0]);

    auto grown_table = hop.get_hashtable();
    size_t new_size = grown_table.size();

    REQUIRE(new_size > initial_size);
    REQUIRE(new_size % initial_size == 0);
}

TEST_CASE("Hopscotch rehash test 2", "[hopscotch][hopscotch_rehash_2]") {
    Hopscotch<int, std::vector<size_t>> hop(2, 2);
    auto initial_hash_table = hop.get_hashtable();
    size_t initial_size = initial_hash_table.size();

    REQUIRE(initial_size == 2);

    int k1 = 0;
    int k2 = 1;
    int k3 = 3;

    std::vector<size_t> v1 = {1};
    std::vector<size_t> v2 = {2};
    std::vector<size_t> v3 = {3};

    hop.emplace(k1, v1);
    hop.emplace(k2, v2);
    hop.emplace(k3, v3);
    auto& found1 = hop.find(k1);
    auto& found2 = hop.find(k2);
    auto& found3 = hop.find(k3);
    REQUIRE(found1.size() == 1);
    REQUIRE(found1[0] == v1[0]);
    REQUIRE(found2.size() == 1);
    REQUIRE(found2[0] == v2[0]);
    REQUIRE(found3.size() == 1);
    REQUIRE(found3[0] == v3[0]);

    auto grown_table = hop.get_hashtable();
    size_t new_size = grown_table.size();

    REQUIRE(new_size > initial_size);
    REQUIRE(new_size % initial_size == 0);
}

TEST_CASE("Hopscotch rehash test 3", "[hopscotch][hopscotch_rehash_3]") {
    Hopscotch<int, std::vector<size_t>> hop(2, 4);
    auto initial_hash_table = hop.get_hashtable();
    size_t initial_size = initial_hash_table.size();

    REQUIRE(initial_size == 4);

    int k1 = 1;
    int k2 = 2;
    int k3 = 6;
    int k4 = 5;

    std::vector<size_t> v1 = {1};
    std::vector<size_t> v2 = {2};
    std::vector<size_t> v3 = {3};
    std::vector<size_t> v4 = {4};

    hop.emplace(k1, v1);
    hop.emplace(k2, v2);
    hop.emplace(k3, v3);
    hop.emplace(k4, v4);
    auto& found1 = hop.find(k1);
    auto& found2 = hop.find(k2);
    auto& found3 = hop.find(k3);
    auto& found4 = hop.find(k4);
    REQUIRE(found1.size() == 1);
    REQUIRE(found1[0] == v1[0]);
    REQUIRE(found2.size() == 1);
    REQUIRE(found2[0] == v2[0]);
    REQUIRE(found3.size() == 1);
    REQUIRE(found3[0] == v3[0]);
    REQUIRE(found4.size() == 1);
    REQUIRE(found4[0] == v4[0]);

    auto grown_table = hop.get_hashtable();
    size_t new_size = grown_table.size();

    REQUIRE(new_size > initial_size);
    REQUIRE(new_size % initial_size == 0);
}

TEST_CASE("Cuckoo manual insertion and find function", "[cuckoo][cuckoo_find]") {
    Cuckoo<int, std::vector<size_t>> cuckoo(8);

    auto& hash_table = cuckoo.get_hashtable();

    SECTION("Manually insert items into specific hash positions") {
        int key1 = 42;
        int key2 = 99;
        std::vector<size_t> val1 = {1, 2, 3};
        std::vector<size_t> val2 = {9, 8};

        size_t idx1_t0 = cuckoo.hash(key1) % 8;
        size_t idx2_t1 = (cuckoo.hash(key2) ^ 0x9e3779b97f4a7c15ULL) % 8;

        hash_table[idx1_t0].key = key1;
        hash_table[idx1_t0].value = val1;
        hash_table[idx1_t0].occupied = true;

        hash_table[8 + idx2_t1].key = key2;
        hash_table[8 + idx2_t1].value = val2;
        hash_table[8 + idx2_t1].occupied = true;

        auto& found1 = cuckoo.find(key1);
        REQUIRE(found1.size() == val1.size());
        REQUIRE(found1[0] == 1);
        REQUIRE(found1[2] == 3);

        auto& found2 = cuckoo.find(key2);
        REQUIRE(found2.size() == val2.size());
        REQUIRE(found2[0] == 9);
        REQUIRE(found2[1] == 8);
    }
}

TEST_CASE("Cuckoo emplace basic behavior", "[cuckoo][cuckoo_emplace]") {
    SECTION("Insert single key-value pair") {
        Cuckoo<int, std::vector<size_t>> hash_table;
        hash_table.emplace(1, {10, 20});
        auto& result = hash_table.find(1);
        REQUIRE(result.size() == 2);
        REQUIRE(result[0] == 10);
        REQUIRE(result[1] == 20);
    }

    SECTION("Insert multiple keys without collision") {
        Cuckoo<const char*, std::vector<size_t>> hash_table;
        hash_table.emplace("iasonas", {22});
        auto& val1 = hash_table.find("iasonas");
        hash_table.print();
        REQUIRE_FALSE(val1.empty());
        REQUIRE(val1.size() == 1);
        REQUIRE(val1[0] == 22);
        hash_table.emplace("dimitris", {33});
        auto& val2 = hash_table.find("dimitris");
        hash_table.print();
        REQUIRE_FALSE(val2.empty());
        REQUIRE(val2.size() == 1);
        REQUIRE(val2[0] == 33);
        hash_table.emplace("spyros", {10, 20});
        auto& val3 = hash_table.find("spyros");
        hash_table.print();
        REQUIRE_FALSE(val3.empty());
        REQUIRE(val3.size() == 2);
        REQUIRE(val3[0] == 10);
        REQUIRE(val3[1] == 20);
    }
}

TEST_CASE("Cuckoo rehash basic growth", "[cuckoo][cuckoo_rehash]") {
    Cuckoo<int, std::vector<size_t>> table(2);

    auto& initial_hash_table = table.get_hashtable();
    size_t initial_size_0 = initial_hash_table.size() / 2;
    size_t initial_size_1 = initial_hash_table.size() / 2;

    for (size_t i = 1; i < 100; i *= 2) {
        table.emplace(i, std::vector<size_t>{i});

        auto& found = table.find(i);
        REQUIRE(found.size() == 1);
        REQUIRE(found[0] == i);
    }

    auto& grown_table = table.get_hashtable();
    size_t new_size_0 = grown_table.size() / 2;
    size_t new_size_1 = grown_table.size() / 2;

    REQUIRE((new_size_0 > initial_size_0 || new_size_1 > initial_size_1));

    for (size_t i = 1; i < 100; i *= 2) {
        auto& val = table.find(i);
        REQUIRE_FALSE(val.empty());
        REQUIRE(val[0] == i);
    }

    REQUIRE((new_size_0 % initial_size_0 == 0 || new_size_1 % initial_size_1 == 0));
}

TEST_CASE("Cuckoo rehash timeout detection (non-blocking)", "[cuckoo][cuckoo_circle_safe_detection]") {
    Cuckoo<int, std::vector<size_t>> cuckoo(4);

    auto& hash_table = cuckoo.get_hashtable();

    int key = 42;
    std::vector<size_t> val1 = {1, 2, 3};
    std::vector<size_t> val2 = {9, 8};

    size_t idx0 = cuckoo.hash(key) % 4;
    size_t idx1 = (cuckoo.hash(key) ^ 0x9e3779b97f4a7c15ULL) % 4;
    hash_table[idx0].key = key;
    hash_table[idx0].value = val1;
    hash_table[idx0].occupied = true;
    hash_table[4 + idx1].key = key;
    hash_table[4 + idx1].value = val2;
    hash_table[4 + idx1].occupied = true;

    std::atomic<bool> finished = false;

    std::thread worker([&]() {
        cuckoo.emplace(key, {10, 20});
        finished = true;
    });
    worker.detach();

    bool success = false;
    for (int i = 0; i < 50; ++i) {
        if (finished) {
            success = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    if (!success)
        SUCCEED("emplace() did not finish within 500 ms — expected infinite loop detected");
    else
        FAIL("emplace() returned unexpectedly — rehash completed (or fixed");
}