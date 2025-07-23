#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>

class CommandHandler {
public:
    CommandHandler(
        std::unordered_map<std::string, std::string>& kv,
        std::unordered_map<std::string, std::chrono::steady_clock::time_point>& expiry,
        std::unordered_map<std::string, std::vector<std::string>>& list
    );

    std::string handle(const std::vector<std::string>& args);
    bool isExpired(const std::string& key);
private:
    std::unordered_map<std::string, std::string>& kv_store;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point>& expiry_store;
    std::unordered_map<std::string, std::vector<std::string>>& list_store;
};
