#pragma once
#include <iostream>
#include <unordered_map>


//T vector<size_t> at the moment.
template <typename Key, typename T>
class Hash_Algorithm {
   public:
    virtual void emplace(const Key& key, T& value) = 0;

    virtual T& find(const Key& key) = 0;

    virtual T end(){
        return nullptr; // “no vector”
    }

};

template <typename Key, typename T>
class Base_Solution : public Hash_Akgorithm<Key , T>{

    std::unordered_map<Key, T> hash_table;
    
    public:

    void emplace(const Key& key, T& value)override{
        hash_table.emplace(key, std::vector<size_t>(1, idx));
    }

    T& find(const Key& key)override{
        
        auto itr = hash_table.find(key);
        
        return itr == hash_table.end() ? nullptr : itr->second;
    }

};

