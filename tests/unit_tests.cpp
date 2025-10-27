#include <catch2/catch_test_macros.hpp>

#include <table.h>
#include <plan.h>
#include "hash_algo.h"

void sort(std::vector<std::vector<Data>>& table) {
    std::sort(table.begin(), table.end());
}

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
        {1, },
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
        {1, 1,},
    };
    REQUIRE(result_table.table() == ground_truth);
}

TEST_CASE("Simple join", "[join]") {
    Plan plan;
    plan.new_scan_node(0, {{0, DataType::INT32}});
    plan.new_scan_node(1, {{0, DataType::INT32}});
    plan.new_join_node(true, 0, 1, 0, 0, {{0, DataType::INT32}, {1, DataType::INT32}});
    std::vector<std::vector<Data>> data{
        {1,},
        {2,},
        {3,},
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
        {1, 1,},
        {2, 2,},
        {3, 3,},
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
        {1,},
        {2,},
        {3,},
    };
    std::vector<std::vector<Data>> data2{
        {4,},
        {5,},
        {6,},
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
        {1,},
        {1,},
        {2,},
        {3,},
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
        {1, 1,},
        {1, 1,},
        {1, 1,},
        {1, 1,},
        {2, 2,},
        {3, 3,},
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
        {1,               },
        {1,               },
        {std::monostate{},},
        {2,               },
        {3,               },
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
        {1, 1,},
        {1, 1,},
        {1, 1,},
        {1, 1,},
        {2, 2,},
        {3, 3,},
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
        {1               , "xxx"s,},
        {1               , "yyy"s,},
        {std::monostate{}, "zzz"s,},
        {2               , "uuu"s,},
        {3               , "vvv"s,},
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
        {1, 1, "xxx"s},
        {1, 1, "xxx"s},
        {1, 1, "yyy"s},
        {1, 1, "yyy"s},
        {2, 2, "uuu"s},
        {3, 3, "vvv"s},
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
        {1               , "xxx"s,},
        {1               , "yyy"s,},
        {std::monostate{}, "zzz"s,},
        {2               , "uuu"s,},
        {3               , "vvv"s,},
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
        {1, 1, "xxx"s},
        {1, 1, "xxx"s},
        {1, 1, "yyy"s},
        {1, 1, "yyy"s},
        {2, 2, "uuu"s},
        {3, 3, "vvv"s},
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
        {1,},
        {2,},
        {3,},
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
        {1, 1, 1,},
        {2, 2, 2,},
        {3, 3, 3,},
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

TEST_CASE("Robin Hood manual insertion and find verification", "[robin_find]") {
    Robin_Hood<int, std::vector<size_t>> table(5);
    auto& ht = table.get_table();


    size_t idx1 = hash_index(1, ht.size());
    size_t idx2 = hash_index(3, ht.size());
    size_t idx3 = hash_index(7, ht.size()); 

    
    ht[idx1] = {{1, {10, 20}}, 0};
    ht[idx2] = {{3, {30}}, 0};
    ht[idx3] = {{7, {99, 100, 101}}, 1};

    SECTION("Find existing keys manually placed") {
        auto& result1 = table.find(1);
        REQUIRE(result1.size() == 2);
        REQUIRE(result1[0] == 10);
        REQUIRE(result1[1] == 20);

        auto& result2 = table.find(3);
        REQUIRE(result2.size() == 1);
        REQUIRE(result2[0] == 30);

        auto& result3 = table.find(7);
        REQUIRE(result3.size() == 3);
        REQUIRE(result3[2] == 101);
    }

    SECTION("Find missing key returns dummy") {
        auto& result = table.find(999);
        REQUIRE(result.size() == 0); 
    }

    table.print();
}
TEST_CASE("Robin Hood emplace basic behavior", "[robin_emplace]") {
   
    SECTION("Insert single key-value pair") {
        Robin_Hood<int, std::vector<size_t>> hash_table(3);
        hash_table.emplace(1, {10, 20});
        auto& result = hash_table.find(1);
        REQUIRE(result.size() == 2);
        REQUIRE(result[0] == 10);
        REQUIRE(result[1] == 20);
    }

    SECTION("Insert multiple keys of char* type") {
            Robin_Hood<const char*, std::vector<size_t>> hash_table(3);

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

TEST_CASE("Robin Hood rehash basic growth and key preservation", "[robin_rehash]") {

    Robin_Hood<int, std::vector<int>> table(4);

    size_t prev_capacity = table.get_table().size();

    std::vector<int> inserted_keys;

    for (int i = 1; i <= 100; i *= 2) {
        inserted_keys.push_back(i);
        table.emplace(i, std::vector<int>{i});

        size_t new_capacity = table.get_table().size();


        if (new_capacity != prev_capacity) {
            std::cout << "Rehash triggered! " 
                      << "Old: " << prev_capacity 
                      << " → New: " << new_capacity << "\n";
        }

        for (int key : inserted_keys) {
            auto& result = table.find(key);
            REQUIRE(result.size() == 1);
            REQUIRE(result[0] == key);
        }

        prev_capacity = new_capacity;
    }

    std::cout << "\nFinal Table after insertions:\n";
    table.print();
}

TEST_CASE("Robin Hood handles collisions and preserves correctness", "[robin_collision]") {
  

    Robin_Hood<int, std::vector<size_t>> table(5);
    auto& ht = table.get_table();

    int k1 = 5;   
    int k2 = 10;  
    int k3 = 15;

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
        if (entry.second == -1) continue;
        if (entry.first.first == k1) found_k1 = true;
        if (entry.first.first == k2) found_k2 = true;
        if (entry.first.first == k3) found_k3 = true;
    }
    REQUIRE(found_k1);
    REQUIRE(found_k2);
    REQUIRE(found_k3);

    std::cout << "\n--- Collision Test Table ---\n";
    table.print();
}