#pragma once

#include <iostream>
#include <fstream>
#include <cstdint>
#include <stdexcept>
#include <cmath>

class Decompressor {
public:
    Decompressor(std::istream& is) : br(is) {}

//    TODO: add main implementation body.

private:
    std::istream& br;
    uint32_t header_{0};
    uint32_t t_{0};
    uint32_t delta_{0};
    uint64_t value_{0};
};