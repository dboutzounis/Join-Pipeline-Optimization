#pragma once
#include <cstdint>
#include <cstring>
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
class Hopscotch : public Hash_Algorithm<Key, T> {
    struct Bucket {
        Key key;
        T value;
        bool occupied;
        std::vector<uint8_t> bitmap;

        Bucket() : occupied(false) {}

        Bucket(size_t bitmap_size) : occupied(false), bitmap(bitmap_size, 0) {}
    };

    std::vector<Bucket> hash_table;
    size_t size, H, active, bitmap_size;

    size_t hash(const Key& k) const { return std::hash<Key>{}(k) % size; }

    inline void set_bit(std::vector<uint8_t>& bitmap, size_t i) { bitmap[i >> 3] |= static_cast<uint8_t>(1u << (i & 7)); }

    inline void clear_bit(std::vector<uint8_t>& bitmap, size_t i) { bitmap[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7))); }

    inline bool check_bit(std::vector<uint8_t>& bitmap, size_t i) const { return (bitmap[i >> 3] & static_cast<uint8_t>(1u << (i & 7))) != 0; }

    inline size_t count_bits(std::vector<uint8_t>& bitmap) const {
        size_t count = 0;
        for (size_t i = 0; i < H; i++) count += check_bit(bitmap, i);
        return count;
    }

    void rehash() {
        std::vector<Bucket> old_hash_table = std::move(hash_table);

        size *= 2;
        hash_table.clear();
        hash_table.assign(size, Bucket(bitmap_size));

        for (auto& bucket : old_hash_table)
            if (bucket.occupied) emplace(bucket.key, std::move(bucket.value));
    }

    void move_payload(size_t dst, size_t src) {
        auto& d = hash_table[dst];
        auto& s = hash_table[src];

        d.key = std::move(s.key);
        d.value = std::move(s.value);
        d.occupied = true;
        s.occupied = false;
    }

   public:
    Hopscotch(size_t H, size_t size = 2048) : H(H), size(size), active(0) {
        bitmap_size = (H + (8 * sizeof(uint8_t) - 1)) / (8 * sizeof(uint8_t));
        hash_table.assign(size, Bucket(bitmap_size));
    }

    T& find(const Key& key) override {
        size_t index = hash(key);

        for (size_t j = index; j < index + H; j++)
            if (hash_table[j % size].occupied && hash_table[j % size].key == key) return hash_table[j % size].value;

        static T dummy{};
        return dummy;
    }

    void emplace(const Key& key, const T& value) override {
        size_t i = hash(key);

        if (count_bits(hash_table[i].bitmap) == H) {
            rehash();
            emplace(key, std::move(value));
            return;
        }

        size_t j = i;
        while (j < i + size && hash_table[j % size].occupied) j++;

        if (j == i + size) {
            rehash();
            emplace(key, std::move(value));
            return;
        }

        j = j % size;

        while ((j + size - i) % size >= H) {
            bool flag = false;
            for (size_t p = H - 1; p > 0; p--) {
                size_t y = (j + size - p) % size;
                auto& bucket = hash_table[y];
                if (!bucket.occupied) continue;
                size_t k = hash(bucket.key);
                if ((j + size - k) % size < H) {
                    move_payload(j, y);
                    clear_bit(hash_table[k].bitmap, (y + size - k) % size);
                    set_bit(hash_table[k].bitmap, (j + size - k) % size);
                    j = y;
                    flag = true;
                    break;
                }
            }
            if (!flag) {
                rehash();
                emplace(key, std::move(value));
                return;
            }
        }

        hash_table[j].key = key;
        hash_table[j].value = std::move(value);
        hash_table[j].occupied = true;
        set_bit(hash_table[i].bitmap, (j + size - i) % size);
        active++;
    }

    void print() const {
        std::cout << "{\n";
        for (size_t i = 0; i < size; i++) {
            const auto& bucket = hash_table[i];
            if (!bucket.occupied) continue;
            std::cout << " " << bucket.key << " :";

            if constexpr (std::is_same_v<T, std::vector<size_t>>) {
                std::cout << "[";
                for (size_t j = 0; j < bucket.value.size(); ++j) {
                    std::cout << bucket.value[j];
                    if (j + 1 < bucket.value.size()) std::cout << ", ";
                }
                std::cout << "]";
            } else {
                std::cout << "\n";
            }
            std::cout << "}\n";
        }
    }
};