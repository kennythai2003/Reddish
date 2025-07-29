#pragma once

#include <string>
#include <unordered_map>
#include <chrono>
#include <vector>
#include <cstdint>

class RdbParser {
public:
    struct ParseResult {
        bool success;
        std::string error_message;
    };

    static ParseResult loadFromFile(
        const std::string& file_path,
        std::unordered_map<std::string, std::string>& kv_store,
        std::unordered_map<std::string, std::chrono::steady_clock::time_point>& expiry_store
    );

private:
    static bool parseRdbData(
        const std::vector<uint8_t>& data,
        std::unordered_map<std::string, std::string>& kv_store,
        std::unordered_map<std::string, std::chrono::steady_clock::time_point>& expiry_store
    );

    static std::chrono::steady_clock::time_point convertEpochToSteady(uint64_t epoch_ms);
    static bool isValidRdbHeader(const std::vector<uint8_t>& data);
    static void skipAuxiliaryField(const std::vector<uint8_t>& data, size_t& pos);
    static void skipHashTableSizeInfo(const std::vector<uint8_t>& data, size_t& pos);
    
    static const uint8_t OPCODE_EOF = 0xFF;
    static const uint8_t OPCODE_SELECTDB = 0xFE;
    static const uint8_t OPCODE_HASHTABLE_SIZE = 0xFB;
    static const uint8_t OPCODE_AUXILIARY = 0xFA;
    static const uint8_t OPCODE_EXPIRY_MS = 0xFC;
    static const uint8_t OPCODE_EXPIRY_SEC = 0xFD;
    static const uint8_t VALUE_TYPE_STRING = 0x00;
    
    static const size_t MIN_RDB_SIZE = 9;
    static const size_t MAGIC_STRING_SIZE = 5;
    static const size_t VERSION_SIZE = 4;
};