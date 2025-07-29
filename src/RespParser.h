#pragma once

#include <string>
#include <vector>

class RespParser {
public:
    static std::vector<std::string> parseArray(const std::string& req);
};
