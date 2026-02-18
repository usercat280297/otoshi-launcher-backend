#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

namespace fs = std::filesystem;

static uint64_t fnv1a64_file(const fs::path& file_path) {
    constexpr uint64_t kOffset = 1469598103934665603ULL;
    constexpr uint64_t kPrime = 1099511628211ULL;
    uint64_t hash = kOffset;

    std::ifstream in(file_path, std::ios::binary);
    if (!in.is_open()) return hash;

    char buffer[8192];
    while (in.good()) {
        in.read(buffer, sizeof(buffer));
        std::streamsize bytes = in.gcount();
        for (std::streamsize i = 0; i < bytes; ++i) {
            hash ^= static_cast<uint8_t>(buffer[i]);
            hash *= kPrime;
        }
    }
    return hash;
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: otoshi_fs_scanner <root-dir>\n";
        return 1;
    }

    fs::path root = fs::u8path(argv[1]);
    if (!fs::exists(root) || !fs::is_directory(root)) {
        std::cerr << "invalid root: " << root.string() << "\n";
        return 1;
    }

    uint64_t file_count = 0;
    uint64_t total_bytes = 0;
    uint64_t aggregate = 1469598103934665603ULL;
    constexpr uint64_t kPrime = 1099511628211ULL;

    for (const auto& entry : fs::recursive_directory_iterator(root, fs::directory_options::skip_permission_denied)) {
        if (!entry.is_regular_file()) continue;
        std::error_code ec;
        auto size = entry.file_size(ec);
        if (ec) continue;

        ++file_count;
        total_bytes += static_cast<uint64_t>(size);
        uint64_t file_hash = fnv1a64_file(entry.path());
        aggregate ^= file_hash;
        aggregate *= kPrime;
    }

    std::ostringstream output;
    output << "{"
           << "\"root\":\"" << root.u8string() << "\","
           << "\"file_count\":" << file_count << ","
           << "\"total_bytes\":" << total_bytes << ","
           << "\"aggregate_hash\":\"0x" << std::hex << std::setw(16) << std::setfill('0') << aggregate
           << "\"}";
    std::cout << output.str() << std::endl;
    return 0;
}
