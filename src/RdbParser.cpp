#include "RdbParser.h"
#include <fstream>
#include <iostream>
#include <iterator>

RdbParser::ParseResult RdbParser::loadFromFile(
    const std::string& file_path,
    std::unordered_map<std::string, std::string>& kv_store,
    std::unordered_map<std::string, std::chrono::steady_clock::time_point>& expiry_store
) {
    std::ifstream file(file_path, std::ios::binary);
    if (!file.is_open()) {
        return {false, "RDB file not found: " + file_path};
    }
    
    std::vector<uint8_t> data((std::istreambuf_iterator<char>(file)), 
                              std::istreambuf_iterator<char>());
    file.close();
    
    if (data.size() < MIN_RDB_SIZE) {
        return {false, "RDB file too small"};
    }
    
    if (!isValidRdbHeader(data)) {
        return {false, "Invalid RDB file format"};
    }
    
    bool success = parseRdbData(data, kv_store, expiry_store);
    return {success, success ? "" : "Failed to parse RDB data"};
}

bool RdbParser::parseRdbData(
    const std::vector<uint8_t>& data,
    std::unordered_map<std::string, std::string>& kv_store,
    std::unordered_map<std::string, std::chrono::steady_clock::time_point>& expiry_store
) {
    size_t pos = MAGIC_STRING_SIZE + VERSION_SIZE;
    
    while (pos < data.size()) {
        if (pos >= data.size()) break;
        
        uint8_t opcode = data[pos++];
        std::chrono::steady_clock::time_point expiry_time = 
            std::chrono::steady_clock::time_point::max();
        
        if (opcode == OPCODE_EOF) {
            break;
        } else if (opcode == OPCODE_EXPIRY_MS) {
            if (pos + 8 > data.size()) break;
            uint64_t expiry_ms = 0;
            for (int i = 0; i < 8; i++) {
                expiry_ms |= (uint64_t)data[pos + i] << (i * 8);
            }
            pos += 8;
            expiry_time = convertEpochToSteady(expiry_ms);
            if (pos >= data.size()) break;
            opcode = data[pos++];
        } else if (opcode == OPCODE_EXPIRY_SEC) {
            if (pos + 4 > data.size()) break;
            uint32_t expiry_sec = 0;
            for (int i = 0; i < 4; i++) {
                expiry_sec |= (uint32_t)data[pos + i] << (i * 8);
            }
            pos += 4;
            expiry_time = convertEpochToSteady((uint64_t)expiry_sec * 1000);
            if (pos >= data.size()) break;
            opcode = data[pos++];
        } else if (opcode == OPCODE_SELECTDB) {
            if (pos >= data.size()) break;
            pos++; // Skip database number
            continue;
        } else if (opcode == OPCODE_HASHTABLE_SIZE) {
            skipHashTableSizeInfo(data, pos);
            continue;
        } else if (opcode == OPCODE_AUXILIARY) {
            skipAuxiliaryField(data, pos);
            continue;
        }
        
        if (opcode == VALUE_TYPE_STRING) {
            if (pos >= data.size()) break;
            uint8_t key_len = data[pos++];
            if (pos + key_len > data.size()) break;
            std::string key(data.begin() + pos, data.begin() + pos + key_len);
            pos += key_len;
            
            if (pos >= data.size()) break;
            uint8_t val_len = data[pos++];
            if (pos + val_len > data.size()) break;
            std::string value(data.begin() + pos, data.begin() + pos + val_len);
            pos += val_len;
            
            kv_store[key] = value;
            
            if (expiry_time != std::chrono::steady_clock::time_point::max()) {
                expiry_store[key] = expiry_time;
            }
        } else {
            break;
        }
    }
    
    return true;
}

std::chrono::steady_clock::time_point RdbParser::convertEpochToSteady(uint64_t epoch_ms) {
    auto now_system = std::chrono::system_clock::now();
    auto now_steady = std::chrono::steady_clock::now();
    auto current_epoch_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now_system.time_since_epoch()).count();
    auto diff_ms = (int64_t)epoch_ms - current_epoch_ms;
    return now_steady + std::chrono::milliseconds(diff_ms);
}

bool RdbParser::isValidRdbHeader(const std::vector<uint8_t>& data) {
    if (data.size() < MAGIC_STRING_SIZE) return false;
    return std::string(data.begin(), data.begin() + MAGIC_STRING_SIZE) == "REDIS";
}

void RdbParser::skipAuxiliaryField(const std::vector<uint8_t>& data, size_t& pos) {
    if (pos >= data.size()) return;
    uint8_t key_len = data[pos++];
    pos += key_len;
    
    if (pos >= data.size()) return;
    uint8_t val_first_byte = data[pos++];
    
    if (val_first_byte == 0xC0) {
        if (pos < data.size()) pos++;
    } else if (val_first_byte == 0xC2) {
        pos += 4;
    } else {
        pos += val_first_byte;
    }
}

void RdbParser::skipHashTableSizeInfo(const std::vector<uint8_t>& data, size_t& pos) {
    if (pos >= data.size()) return;
    uint8_t len_byte = data[pos++];
    if ((len_byte & 0xC0) == 0xC0) {
        pos += 4;
    }
    
    if (pos >= data.size()) return;
    len_byte = data[pos++];
    if ((len_byte & 0xC0) == 0xC0) {
        pos += 4;
    }
}