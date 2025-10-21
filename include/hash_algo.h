#pragma once
#include <functional>
#include <iostream>
#include <unordered_map>
#include <vector>

// T vector<size_t> at the moment.
template <typename Key, typename T>
class Hash_Algorithm {
   public:
    virtual void emplace(const Key& key, const T& value) = 0;
    virtual T& find(const Key& key) = 0;
    virtual void print() const = 0;
};

template <typename Key, typename T>
class Base_Solution : public Hash_Algorithm<Key, T> {
    std::unordered_map<Key, T> hash_table;

   public:
    void emplace(const Key& key, const T& value) override { hash_table.emplace(key, value); }

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
class Cuckoo : public Hash_Algorithm<Key, T> {
    struct Bucket {
        Key key;
        T value;
        bool occupied;

        Bucket() : occupied(false) {}
    };
    size_t *size, dim, *active;
    std::vector<std::vector<Bucket>> hash_table;
    std::vector<std::function<size_t(const Key&)>> hash_functions;

    size_t hash_with_seed(const Key& key, size_t seed, size_t i) const { return (std::hash<Key>{}(key) ^ (seed * 0x9e3779b97f4a7c15ULL)) % size[i]; }

    void rehash(size_t index) {
        std::vector<Bucket> old_hash_table = std::move(hash_table[index]);

        size[index] *= 2;
        hash_table[index].clear();
        hash_table[index].assign(size[index], Bucket());
        active[index] = 0;

        for (auto& bucket : old_hash_table)
            if (bucket.occupied) emplace(bucket.key, std::move(bucket.value));
    }

   public:
    Cuckoo(size_t dim = 2, size_t size = 1024) : dim(dim), size(new size_t[dim]), active(new size_t[dim]), hash_table(dim, std::vector<Bucket>(size)) {
        for (int i = 0; i < dim; i++) {
            this->size[i] = size;
            this->active[i] = 0;
            hash_functions.push_back([this, i](const Key& key) { return hash_with_seed(key, i + 1, i); });
        }
    }

    ~Cuckoo() {
        delete[] size;
        delete[] active;
    }
    void emplace(const Key& key, const T& value) override {
        const Key* curkey = &key;
        const T* curval = &value;
        size_t table_idx = 0;
        size_t swap_count = 0;
        size_t max_swap = 0;
        for (int i = 0; i < dim; i++) max_swap += size[i];
        while (swap_count < max_swap) {
            size_t index = hash_functions[table_idx](*curkey);

            if (!hash_table[table_idx][index].occupied) {
                hash_table[table_idx][index].key = *curkey;
                hash_table[table_idx][index].value = *curval;
                hash_table[table_idx][index].occupied = true;
                active[table_idx]++;
                if (static_cast<double>(active[table_idx]) / size[table_idx] > 0.5) rehash(table_idx);
                return;
            }

            std::swap(const_cast<Key&>(*curkey), hash_table[table_idx][index].key);
            std::swap(const_cast<T&>(*curval), hash_table[table_idx][index].value);

            table_idx = (table_idx + 1) % dim;
            swap_count++;
        }

        Key temp_key = *curkey;
        T temp_val = *curval;
        rehash(0);
        emplace(temp_key, temp_val);
    }

    T& find(const Key& key) override {
        for (size_t i = 0; i < dim; i++) {
            size_t index = hash_functions[i](key);
            auto& bucket = hash_table[i][index];
            if (bucket.occupied && bucket.key == key) return bucket.value;
        }
        static T dummy{};
        return dummy;
    }

    void print() const {}
};