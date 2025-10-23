#pragma once
#include <iostream>
#include <unordered_map>
#include <vector>

// T vector<size_t> at the moment.
template <typename Key, typename T>
class Hash_Algorithm {
   public:
    virtual void emplace(const Key& key, T value) = 0;
    virtual T& find(const Key& key) = 0;
    virtual void print() const = 0;
};

template <typename Key, typename T>
class Base_Solution : public Hash_Algorithm<Key, T> {
    std::unordered_map<Key, T> hash_table;

   public:
    void emplace(const Key& key, T value) override { hash_table.emplace(key, value); }

    T& find(const Key& key) override {
        auto itr = hash_table.find(key);

        if (itr == hash_table.end()) {
            static T dummy{};
            return dummy;
        }

        return itr->second;
    }

    void print() const {
        std::cout << "{\n";
        for (const auto& [key, vec] : hash_table) {
            std::cout << "  " << key << " : [";
            for (size_t i = 0; i < vec.size(); ++i) {
                std::cout << vec[i];
                if (i + 1 < vec.size()) std::cout << ", ";
            }
            std::cout << "]\n";
        }
        std::cout << "}\n";
    }
};
template <typename Key, typename T>
class Robin_Hood : public Hash_Algorithm<Key, T> {
    std::vector<std::pair<std::pair<Key, T>, int>> hash_table;
    int capacity , num_elements;

   
   public:

    explicit Robin_Hood(size_t table_size = 2000)
    : hash_table(table_size, {{}, -1}),
      capacity(table_size),
      num_elements(0) {}

    std::vector<std::pair<std::pair<Key, T>, int>>& get_table();

    void emplace(const Key& key, T value) override;

    T& find(const Key& key) override ;

    void rehash(size_t new_capacity) ;
 
    void print()const ;
};



// Hash function for indices
template <typename T>
size_t hash_index(const T& value, size_t vector_size) {
    return std::hash<T>{}(value) % vector_size;
}

template <typename Key, typename T>
void Robin_Hood<Key, T>::print() const {
    for (size_t i = 0; i < hash_table.size(); ++i) {
        const auto& entry = hash_table[i];
        const auto& key = entry.first.first;
        const auto& value = entry.first.second;
        int distance = entry.second;

        std::cout << "Index " << i << " : ";

        if (distance == -1) {
            std::cout << "[EMPTY]" << std::endl;
        } else {
            std::cout << "Key = " << key
                      << ", Distance = " << distance
                      << ", Values: ";

            for (const auto& v : value)
                std::cout << v << " ";
            std::cout << std::endl;
        }
    }

    std::cout<<"END OF PRINT \n";
}



template <typename Key, typename T>
void Robin_Hood<Key, T>::rehash(size_t new_capacity) {

    std::vector<std::pair<std::pair<Key, T>, int>> new_table(
        new_capacity, {{}, -1});

    
    for (auto& entry : hash_table) {
        if (entry.second != -1) {  
            auto& key = entry.first.first;
            auto& value = entry.first.second;

         
            size_t idx = hash_index(key, new_capacity);
            int tsl = 0;

            while (true) {
                auto& new_entry = new_table[idx];
                auto& target_tsl = new_entry.second;

                if (target_tsl == -1) {
                    new_entry = {{key, value}, tsl};
                    break;
                }

                if (target_tsl < tsl) {
                    std::swap(new_entry.first.first, key);
                    std::swap(new_entry.first.second, value);
                    std::swap(new_entry.second, tsl);
                }

                tsl++;
                idx = (idx + 1) % new_capacity;
            }
        }
    }

    hash_table.swap(new_table);
    capacity = hash_table.size();
}


template <typename Key, typename T>
void Robin_Hood<Key, T>::emplace(const Key& key, T value) {

    if ((num_elements + 1) > 0.7 * capacity) {
        rehash(capacity * 2);
    }
    
    auto source_tsl = 0;
    auto source_key = key;
    auto source_value = value;
    size_t idx = hash_index(key, hash_table.size());
    
    
    do {
        auto& entry = hash_table[idx];
        auto& target_tsl = entry.second;     
        auto& target_key = entry.first.first;
        auto& target_value = entry.first.second;

        if (target_tsl == -1) {
            entry = {{source_key, source_value}, source_tsl};
            num_elements+=1;
            return;
        }
        if (target_tsl < source_tsl) {
        
            std::swap(source_key, target_key);
            std::swap(source_value, target_value);
            std::swap(source_tsl, target_tsl); 
        }

        source_tsl++;
        idx = (idx + 1) % hash_table.size();
    } while (true);


}

template <typename Key, typename T>
std::vector<std::pair<std::pair<Key, T>, int>>& Robin_Hood<Key, T>::get_table() {
    return hash_table;
}

template <typename Key, typename T>
T& Robin_Hood<Key, T>::find(const Key& key) {
    size_t index = hash_index(key, hash_table.size());
    size_t source_tsl = 0;

    do {
        auto& entry = hash_table[index];
        const auto& target_key = entry.first.first;
        auto& value = entry.first.second;
        auto& target_tsl = entry.second;

        std::cout<<target_tsl<<"\n";

        if (entry.second != -1 && target_key == key)
            return value;

        index = (index + 1) % hash_table.size();
        if(source_tsl++>target_tsl)
            break;
    }while(1);

    static T dummy{};
    return dummy;
}