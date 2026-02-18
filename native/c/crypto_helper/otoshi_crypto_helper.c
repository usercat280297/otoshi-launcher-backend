#include <stddef.h>
#include <stdint.h>

#if defined(_WIN32)
#define OTOSHI_EXPORT __declspec(dllexport)
#else
#define OTOSHI_EXPORT
#endif

OTOSHI_EXPORT uint64_t otoshi_fnv1a64(const uint8_t *data, size_t len) {
    const uint64_t offset = 1469598103934665603ULL;
    const uint64_t prime = 1099511628211ULL;
    uint64_t hash = offset;
    if (data == NULL) {
        return hash;
    }
    for (size_t i = 0; i < len; ++i) {
        hash ^= (uint64_t)data[i];
        hash *= prime;
    }
    return hash;
}

OTOSHI_EXPORT int otoshi_consttime_eq(const uint8_t *left, const uint8_t *right, size_t len) {
    if (left == NULL || right == NULL) {
        return 0;
    }
    uint8_t diff = 0;
    for (size_t i = 0; i < len; ++i) {
        diff |= (uint8_t)(left[i] ^ right[i]);
    }
    return diff == 0 ? 1 : 0;
}
