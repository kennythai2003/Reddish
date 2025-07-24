#include "CommandHandler.h"
#include <algorithm>
#include <map>

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

// Add a new type for stream entries
struct StreamEntry {
    std::string id;
    std::map<std::string, std::string> fields;
};
// Add a new member for streams
static std::unordered_map<std::string, std::vector<StreamEntry>> stream_store;

std::string CommandHandler::handle(const std::vector<std::string>& args) {
    // ...existing code...
    if (args.empty()) return "-ERR unknown command\r\n";
    std::string cmd = args[0];
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
    if (cmd == "LPOP" && (args.size() == 2 || args.size() == 3)) {
        std::string key = args[1];
        auto it = list_store.find(key);
        if (it == list_store.end() || it->second.empty()) {
            return "$-1\r\n";
        }
        int count = 1;
        if (args.size() == 3) {
            try {
                count = std::stoi(args[2]);
            } catch (...) {
                return "-ERR value is not an integer or out of range\r\n";
            }
            if (count <= 0) {
                return "*-1\r\n";
            }
        }
        int actual_count = std::min(count, static_cast<int>(it->second.size()));
        if (actual_count == 1) {
            std::string val = it->second.front();
            it->second.erase(it->second.begin());
            return "$" + std::to_string(val.size()) + "\r\n" + val + "\r\n";
        } else {
            std::string resp = "*" + std::to_string(actual_count) + "\r\n";
            for (int i = 0; i < actual_count; ++i) {
                std::string val = it->second.front();
                it->second.erase(it->second.begin());
                resp += "$" + std::to_string(val.size()) + "\r\n" + val + "\r\n";
            }
            return resp;
        }
    }
    if (cmd == "LLEN" && args.size() == 2) {
        std::string key = args[1];
        auto it = list_store.find(key);
        int len = (it == list_store.end()) ? 0 : static_cast<int>(it->second.size());
        return ":" + std::to_string(len) + "\r\n";
    }
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
    } else if (cmd == "LPUSH" && args.size() >= 3) {
        std::string key = args[1];
        std::vector<std::string> elements(args.begin() + 2, args.end());
        // Insert elements in reverse order at the front
        if (list_store.find(key) == list_store.end()) {
            // New list: insert in reverse so leftmost arg is head
            list_store[key] = std::vector<std::string>(elements.rbegin(), elements.rend());
        } else {
            // Existing list: insert each element at front in reverse order
            auto& lst = list_store[key];
            lst.insert(lst.begin(), elements.rbegin(), elements.rend());
        }
        return ":" + std::to_string(list_store[key].size()) + "\r\n";
    } else if (cmd == "LRANGE" && args.size() == 4) {
        std::string key = args[1];
        int start = std::stoi(args[2]);
        int stop = std::stoi(args[3]);
        auto it = list_store.find(key);
        if (it == list_store.end()) {
            return "*0\r\n";
        }
        const std::vector<std::string>& list = it->second;
        int list_size = static_cast<int>(list.size());
        // Handle negative indexes
        if (start < 0) start = list_size + start;
        if (stop < 0) stop = list_size + stop;
        // Clamp to valid range
        if (start < 0) start = 0;
        if (stop < 0) stop = 0;
        if (start >= list_size || start > stop) {
            return "*0\r\n";
        }
        if (stop >= list_size) stop = list_size - 1;
        int count = stop - start + 1;
        if (count <= 0) return "*0\r\n";
        std::string resp = "*" + std::to_string(count) + "\r\n";
        for (int i = start; i <= stop; ++i) {
            resp += "$" + std::to_string(list[i].size()) + "\r\n" + list[i] + "\r\n";
        }
        return resp;
    } else if (cmd == "XADD" && args.size() >= 4 && args.size() % 2 == 0) {
        std::string stream_key = args[1];
        std::string entry_id = args[2];
        StreamEntry entry;
        entry.id = entry_id;
        for (size_t i = 3; i + 1 < args.size(); i += 2) {
            entry.fields[args[i]] = args[i + 1];
        }
        stream_store[stream_key].push_back(entry);
        return "$" + std::to_string(entry_id.size()) + "\r\n" + entry_id + "\r\n";
    } else if (cmd == "TYPE" && args.size() == 2) {
        std::string key = args[1];
        if (kv_store.find(key) != kv_store.end()) {
            return "+string\r\n";
        } else if (stream_store.find(key) != stream_store.end()) {
            return "+stream\r\n";
        } else {
            return "+none\r\n";
        }
    }
    return "-ERR unknown command\r\n";
}
