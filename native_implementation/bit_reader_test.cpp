#include <vector>
#include <sstream>
#include "bit_reader.h"

int main() {
    try {
        std::ifstream file("read_output.bin", std::ios::binary);
        BitReader reader(file);
        uint64_t value = reader.readBits(64); // Example usage
        std::cout << "Read value: " << value << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Exception caught: " << e.what() << std::endl;
    }
    return 0;
}