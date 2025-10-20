#pragma once
#include <cstring>
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
class Hopscotch : public Hash_Algorithm<Key, T> {
    struct Bucket {
        Key key;
        T value;
        bool occupied;
        char* bitmap;
        Bucket() : occupied(false), bitmap(nullptr) {}
        Bucket(size_t H, size_t bitmap_size) {
            occupied = false;
            bitmap = new char[bitmap_size];
            std::memset(bitmap, 0, bitmap_size);
        }
        Bucket(const Bucket& other) : key(other.key), value(other.value), occupied(other.occupied) {
            if (other.bitmap) {
                bitmap = new char[std::strlen(other.bitmap)];
                std::memcpy(bitmap, other.bitmap, std::strlen(other.bitmap));
            } else {
                bitmap = nullptr;
            }
        }

        Bucket(Bucket&& other) noexcept : key(std::move(other.key)), value(std::move(other.value)), occupied(other.occupied), bitmap(other.bitmap) {
            other.bitmap = nullptr;
            other.occupied = false;
        }
        ~Bucket() { delete[] bitmap; }
    };

    std::vector<Bucket> hash_table;
    size_t size, H, active, bitmap_size;
    size_t hash(const Key& k) { return std::hash<Key>{}(k) % size; }
    inline void set_bit(char* bitmap, size_t i) { bitmap[i / 8] |= static_cast<char>(1 << (i % 8)); }

    inline void clear_bit(char* bitmap, size_t i) { bitmap[i / 8] &= static_cast<char>(~(1 << (i % 8))); }

    inline bool check_bit(char* bitmap, size_t i) { return bitmap[i / 8] & static_cast<char>(1 << (i % 8)); }

    inline size_t count_bits(char* bitmap) {
        size_t count = 0;
        for (size_t i = 0; i < bitmap_size * 8; i++) {
            if (check_bit(bitmap, i)) count++;
        }
        return count;
    }

    void rehash() {
        std::vector<Bucket> old_hash_table = std::move(hash_table);
        hash_table.clear();
        hash_table.resize(size * 2);
        size *= 2;
        for (auto& bucket : hash_table) {
            bucket.bitmap = new char[bitmap_size];
            std::memset(bucket.bitmap, 0, bitmap_size);
            bucket.occupied = false;
        }
        for (auto& bucket : old_hash_table) {
            if (bucket.occupied) {
                this->emplace(bucket.key, std::move(bucket.value));
            }
        }
    }

   public:
    Hopscotch(size_t H, size_t size = 1024) {
        this->size = size;
        this->H = H;
        active = 0;
        hash_table.resize(size);
        bitmap_size = (H + (8 * sizeof(char) - 1)) / (8 * sizeof(char));
        for (auto& bucket : hash_table) {
            bucket.bitmap = new char[bitmap_size];
            std::memset(bucket.bitmap, 0, bitmap_size);
            bucket.occupied = false;
        }
    }

    ~Hopscotch() = default;

    T& find(const Key& key) override {
        size_t index = hash(key);
        for (size_t j = index; j < index + H; j++) {
            if (hash_table[j % size].occupied && hash_table[j % size].key == key) return hash_table[j % size].value;
        }
        static T dummy{};
        return dummy;
    }

    void emplace(const Key& key, T value) override {
        size_t i = hash(key);
        if (count_bits(hash_table[i].bitmap) == H) {
            rehash();
            emplace(key, std::move(value));
            return;
        }
        size_t j = i;
        while (j < i + size && hash_table[j % size].occupied) {
            j++;
        }
        if (j == i + size) {
            rehash();
            emplace(key, std::move(value));
            return;
        }

        while ((j + size - i) % size >= H) {
            bool flag = false;

            for (size_t p = H - 1; p > 0; p--) {
                size_t y = (j + size - p) % size;
                auto& bucket = hash_table[y];
                if (!bucket.occupied) continue;
                size_t k = hash(bucket.key);
                if ((j + size - k) % size < H) {
                    hash_table[j] = std::move(bucket);
                    bucket.occupied = false;
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
        set_bit(hash_table[j].bitmap, (j + size - i) % size);
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