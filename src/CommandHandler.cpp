#include "CommandHandler.h"
#include <algorithm>
#include <map>
#include <climits>

// Make stream_store a global variable that persists across translation units
std::unordered_map<std::string, std::vector<StreamEntry>> stream_store;

// Global replica count that can be updated from Server.cpp
int replica_count = 0;

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
    
    // MULTI command: start transaction (queueing not implemented yet)
    if (cmd == "MULTI" && args.size() == 1) {
        return "+OK\r\n";
    }
    // EXEC command: error if MULTI has not been called (transaction not started)
    if (cmd == "EXEC" && args.size() == 1) {
        // Transaction state not tracked yet, always error for now
        return "-ERR EXEC without MULTI\r\n";
    }
    // INCR command: key exists and has a numerical value, or key does not exist
    if (cmd == "INCR" && args.size() == 2) {
        std::string key = args[1];
        auto it = kv_store.find(key);
        if (it != kv_store.end()) {
            // Check if value is a valid integer
            try {
                long long val = std::stoll(it->second);
                val += 1;
                it->second = std::to_string(val);
                return ":" + std::to_string(val) + "\r\n";
            } catch (...) {
                // Will handle non-integer values in later stages
                return "-ERR value is not an integer or out of range\r\n";
            }
        } else {
            // Key does not exist: set to 1
            kv_store[key] = "1";
            return ":1\r\n";
        }
    }
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
    } else if (cmd == "XADD" && args.size() >= 5 && (args.size() - 3) % 2 == 0) {
        std::string stream_key = args[1];
        std::string entry_id = args[2];
        long long ms = 0, seq = 0;
        bool auto_time = (entry_id == "*");
        bool auto_seq = false;
        std::string ms_str, seq_str;
        
        if (auto_time) {
            // Get current unix time in ms
            ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()
            ).count();
            ms_str = std::to_string(ms);
            seq = 0;
            // Check if this ms already exists in stream, increment seq if needed
            auto& entries = stream_store[stream_key];
            long long last_seq = -1;
            for (const auto& e : entries) {
                size_t e_dash = e.id.find('-');
                if (e_dash != std::string::npos) {
                    long long e_ms = 0, e_seq = 0;
                    try {
                        e_ms = std::stoll(e.id.substr(0, e_dash));
                        e_seq = std::stoll(e.id.substr(e_dash + 1));
                    } catch (...) { continue; }
                    if (e_ms == ms && e_seq > last_seq) last_seq = e_seq;
                }
            }
            if (last_seq >= 0) seq = last_seq + 1;
        } else {
            size_t dash_pos = entry_id.find('-');
            if (dash_pos == std::string::npos) {
                return "-ERR The ID specified in XADD is invalid\r\n";
            }
            ms_str = entry_id.substr(0, dash_pos);
            seq_str = entry_id.substr(dash_pos + 1);
            auto_seq = (seq_str == "*");
            try {
                ms = std::stoll(ms_str);
                if (!auto_seq) seq = std::stoll(seq_str);
            } catch (...) {
                return "-ERR The ID specified in XADD is invalid\r\n";
            }
            // Minimum allowed ID is 0-1
            if (ms == 0 && !auto_seq && seq == 0) {
                return "-ERR The ID specified in XADD must be greater than 0-0\r\n";
            }
            if (ms < 0 || (!auto_seq && seq < 0)) {
                return "-ERR The ID specified in XADD is invalid\r\n";
            }
            auto& entries = stream_store[stream_key];
            if (auto_seq) {
                long long last_seq = -1;
                for (const auto& e : entries) {
                    size_t e_dash = e.id.find('-');
                    if (e_dash != std::string::npos) {
                        long long e_ms = 0, e_seq = 0;
                        try {
                            e_ms = std::stoll(e.id.substr(0, e_dash));
                            e_seq = std::stoll(e.id.substr(e_dash + 1));
                        } catch (...) { continue; }
                        if (e_ms == ms && e_seq > last_seq) last_seq = e_seq;
                    }
                }
                if (ms == 0 && last_seq == -1) seq = 1;
                else seq = last_seq + 1;
            }
            // Validate against last entry for explicit IDs only
            if (!auto_seq && !entries.empty()) {
                const StreamEntry& last = entries.back();
                size_t last_dash = last.id.find('-');
                long long last_ms = 0, last_seq = 0;
                if (last_dash != std::string::npos) {
                    try {
                        last_ms = std::stoll(last.id.substr(0, last_dash));
                        last_seq = std::stoll(last.id.substr(last_dash + 1));
                    } catch (...) {}
                }
                if (ms < last_ms || (ms == last_ms && seq <= last_seq)) {
                    return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                }
            }
        }
        
        std::string final_id = ms_str + "-" + std::to_string(seq);
        StreamEntry entry;
        entry.id = final_id;
        for (size_t i = 3; i + 1 < args.size(); i += 2) {
            entry.fields[args[i]] = args[i + 1];
        }
        stream_store[stream_key].push_back(entry);
        return "$" + std::to_string(final_id.size()) + "\r\n" + final_id + "\r\n";
        
    } else if (cmd == "TYPE" && args.size() == 2) {
        std::string key = args[1];
        if (kv_store.find(key) != kv_store.end()) {
            return "+string\r\n";
        } else if (stream_store.find(key) != stream_store.end()) {
            return "+stream\r\n";
        } else {
            return "+none\r\n";
        }
        
    } else if (cmd == "XRANGE" && args.size() == 4) {
        std::string stream_key = args[1];
        std::string start_id = args[2];
        std::string end_id = args[3];
        auto it = stream_store.find(stream_key);
        if (it == stream_store.end()) {
            return "*0\r\n";
        }
        
        // Helper to parse id, defaulting sequence number if missing
        auto parse_id = [](const std::string& id, bool is_start, const std::vector<StreamEntry>& entries) -> std::pair<long long, long long> {
            if (is_start && id == "-") {
                // Beginning of stream
                return {LLONG_MIN, LLONG_MIN};
            }
            if (!is_start && id == "+") {
                // End of stream
                return {LLONG_MAX, LLONG_MAX};
            }
            size_t dash = id.find('-');
            if (dash != std::string::npos) {
                long long ms = 0, seq = 0;
                try {
                    ms = std::stoll(id.substr(0, dash));
                    seq = std::stoll(id.substr(dash + 1));
                } catch (...) {}
                return {ms, seq};
            } else {
                long long ms = 0;
                try { ms = std::stoll(id); } catch (...) {}
                if (is_start) return {ms, 0};
                // Find max sequence number for ms
                long long max_seq = 0;
                for (const auto& e : entries) {
                    size_t e_dash = e.id.find('-');
                    if (e_dash != std::string::npos) {
                        long long e_ms = 0, e_seq = 0;
                        try {
                            e_ms = std::stoll(e.id.substr(0, e_dash));
                            e_seq = std::stoll(e.id.substr(e_dash + 1));
                        } catch (...) { continue; }
                        if (e_ms == ms && e_seq > max_seq) max_seq = e_seq;
                    }
                }
                return {ms, max_seq};
            }
        };
        
        const std::vector<StreamEntry>& entries = it->second;
        auto [start_ms, start_seq] = parse_id(start_id, true, entries);
        auto [end_ms, end_seq] = parse_id(end_id, false, entries);
        
        std::vector<const StreamEntry*> result;
        for (const auto& entry : entries) {
            size_t dash = entry.id.find('-');
            if (dash == std::string::npos) continue;
            long long ms = 0, seq = 0;
            try {
                ms = std::stoll(entry.id.substr(0, dash));
                seq = std::stoll(entry.id.substr(dash + 1));
            } catch (...) { continue; }
            // Only include entries with IDs >= start and <= end
            if ((ms > start_ms || (ms == start_ms && seq >= start_seq)) &&
                (ms < end_ms || (ms == end_ms && seq <= end_seq))) {
                result.push_back(&entry);
            }
        }
        
        std::string resp = "*" + std::to_string(result.size()) + "\r\n";
        for (const auto* entry : result) {
            resp += "*2\r\n";
            resp += "$" + std::to_string(entry->id.size()) + "\r\n" + entry->id + "\r\n";
            resp += "*" + std::to_string(entry->fields.size() * 2) + "\r\n";
            for (const auto& kv : entry->fields) {
                resp += "$" + std::to_string(kv.first.size()) + "\r\n" + kv.first + "\r\n";
                resp += "$" + std::to_string(kv.second.size()) + "\r\n" + kv.second + "\r\n";
            }
        }
        return resp;
        
    } else if (cmd == "XREAD" && args.size() >= 4 && args[1] == "streams") {
        // Find the split between stream keys and IDs
        int n_streams = (args.size() - 2) / 2;
        if ((args.size() - 2) % 2 != 0 || n_streams < 1) {
            return "-ERR wrong number of arguments for 'XREAD' command\r\n";
        }

        std::vector<std::string> stream_keys;
        std::vector<std::string> last_ids;
        for (int i = 2; i < 2 + n_streams; ++i) {
            stream_keys.push_back(args[i]);
        }
        for (int i = 2 + n_streams; i < 2 + 2 * n_streams; ++i) {
            last_ids.push_back(args[i]);
        }

        // If $ is passed as the ID, replace it with the highest ID in the stream
        for (int s = 0; s < n_streams; ++s) {
            if (last_ids[s] == "$") {
                auto it = stream_store.find(stream_keys[s]);
                if (it != stream_store.end() && !it->second.empty()) {
                    // Find the highest ID
                    last_ids[s] = it->second.back().id;
                } else {
                    last_ids[s] = "0-0";
                }
            }
        }

        std::string resp = "*" + std::to_string(n_streams) + "\r\n";
        for (int s = 0; s < n_streams; ++s) {
            const std::string& stream_key = stream_keys[s];
            const std::string& last_id = last_ids[s];
            auto it = stream_store.find(stream_key);

            resp += "*2\r\n$" + std::to_string(stream_key.size()) + "\r\n" + stream_key + "\r\n";

            if (it == stream_store.end()) {
                resp += "*0\r\n";
                continue;
            }

            // Parse last_id
            size_t dash = last_id.find('-');
            long long last_ms = 0, last_seq = 0;
            if (dash != std::string::npos) {
                try {
                    last_ms = std::stoll(last_id.substr(0, dash));
                    last_seq = std::stoll(last_id.substr(dash + 1));
                } catch (...) {}
            } else {
                try { last_ms = std::stoll(last_id); } catch (...) {}
                last_seq = 0;
            }

            const std::vector<StreamEntry>& entries = it->second;
            std::vector<const StreamEntry*> result;
            for (const auto& entry : entries) {
                size_t e_dash = entry.id.find('-');
                if (e_dash == std::string::npos) continue;
                long long ms = 0, seq = 0;
                try {
                    ms = std::stoll(entry.id.substr(0, e_dash));
                    seq = std::stoll(entry.id.substr(e_dash + 1));
                } catch (...) { continue; }
                // Only include entries with IDs > last_id (exclusive)
                if (ms > last_ms || (ms == last_ms && seq > last_seq)) {
                    result.push_back(&entry);
                }
            }

            resp += "*" + std::to_string(result.size()) + "\r\n";
            for (const auto* entry : result) {
                resp += "*2\r\n";
                resp += "$" + std::to_string(entry->id.size()) + "\r\n" + entry->id + "\r\n";
                resp += "*" + std::to_string(entry->fields.size() * 2) + "\r\n";
                for (const auto& kv : entry->fields) {
                    resp += "$" + std::to_string(kv.first.size()) + "\r\n" + kv.first + "\r\n";
                    resp += "$" + std::to_string(kv.second.size()) + "\r\n" + kv.second + "\r\n";
                }
            }
        }
        return resp;
    // WAIT command is handled in Server.cpp due to async nature
    } else if (cmd == "KEYS" && args.size() == 2) {
        // KEYS command: KEYS <pattern>
        std::string pattern = args[1];
        std::vector<std::string> matching_keys;
        
        // For now, support simple "*" pattern (match all keys)
        if (pattern == "*") {
            for (const auto& pair : kv_store) {
                // Skip expired keys
                if (!isExpired(pair.first)) {
                    matching_keys.push_back(pair.first);
                }
            }
        } else {
            // For other patterns, could implement glob matching here
            // For now, just exact match
            if (kv_store.find(pattern) != kv_store.end() && !isExpired(pattern)) {
                matching_keys.push_back(pattern);
            }
        }
        
        // Return RESP array
        std::string response = "*" + std::to_string(matching_keys.size()) + "\r\n";
        for (const std::string& key : matching_keys) {
            response += "$" + std::to_string(key.size()) + "\r\n" + key + "\r\n";
        }
        return response;
    }
    
    return "-ERR unknown command\r\n";
}
//finnally dubs
