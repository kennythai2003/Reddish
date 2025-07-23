#include "RespParser.h"
#include <string>
#include <vector>

std::vector<std::string> RespParser::parseArray(const std::string& req) {
    std::vector<std::string> out;
    size_t pos = 0;
    if (req[pos] == '*') {
        size_t rn = req.find("\r\n", pos);
        if (rn == std::string::npos) return out;
        pos = rn + 2;
    }
    while (pos < req.size()) {
        if (req[pos] != '$') break;
        size_t len_end = req.find("\r\n", pos);
        if (len_end == std::string::npos) break;
        int len = std::stoi(req.substr(pos + 1, len_end - pos - 1));
        size_t val_start = len_end + 2;
        if (val_start + len > req.size()) break;
        out.push_back(req.substr(val_start, len));
        pos = val_start + len + 2;
    }
    return out;
}
