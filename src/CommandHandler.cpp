#include "CommandHandler.h"
#include <algorithm>

CommandHandler::CommandHandler(
    std::unordered_map<std::string, std::string>& kv,
    std::unordered_map<std::string, std::chrono::steady_clock::time_point>& expiry,
    std::unordered_map<std::string, std::vector<std::string>>& list
) : kv_store(kv), expiry_store(expiry), list_store(list) {}

bool CommandHandler::isExpired(const std::string& key) {
    auto exp_it = expiry_store.find(key);
    if (exp_it != expiry_store.end()) {
        if (std::chrono::steady_clock::now() >= exp_it->second) {
            kv_store.erase(key);
            expiry_store.erase(key);
            return true;
        }
    }
    return false;
}

std::string CommandHandler::handle(const std::vector<std::string>& args) {
    if (args.empty()) return "-ERR unknown command\r\n";
    std::string cmd = args[0];
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
    if (cmd == "PING" && args.size() == 1) {
        return "+PONG\r\n";
    } else if (cmd == "ECHO" && args.size() == 2) {
        return "$" + std::to_string(args[1].length()) + "\r\n" + args[1] + "\r\n";
    } else if (cmd == "SET" && (args.size() == 3 || args.size() == 5)) {
        kv_store[args[1]] = args[2];
        expiry_store.erase(args[1]);
        if (args.size() == 5) {
            std::string px_arg = args[3];
            std::transform(px_arg.begin(), px_arg.end(), px_arg.begin(), ::tolower);
            if (px_arg == "px") {
                int ms_val = std::stoi(args[4]);
                expiry_store[args[1]] = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms_val);
            }
        }
        return "+OK\r\n";
    } else if (cmd == "GET" && args.size() == 2) {
        std::string key = args[1];
        if (isExpired(key)) return "$-1\r\n";
        auto it = kv_store.find(key);
        if (it != kv_store.end()) {
            return "$" + std::to_string(it->second.length()) + "\r\n" + it->second + "\r\n";
        } else {
            return "$-1\r\n";
        }
    } else if (cmd == "RPUSH" && args.size() >= 3) {
        std::string key = args[1];
        std::vector<std::string> elements(args.begin() + 2, args.end());
        if (list_store.find(key) == list_store.end()) {
            list_store[key] = elements;
        } else {
            list_store[key].insert(list_store[key].end(), elements.begin(), elements.end());
        }
        return ":" + std::to_string(list_store[key].size()) + "\r\n";
    }
    return "-ERR unknown command\r\n";
}
