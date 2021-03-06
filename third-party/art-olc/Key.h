#ifndef ART_OLC_KEY_H
#define ART_OLC_KEY_H

#include <stdint.h>
#include <cstring>
#include <memory>
#include <ostream>
#include <assert.h>

namespace ART_OLC {

using KeyLen = uint32_t;

class Key {
    static constexpr uint32_t stackLen = 128;
    uint32_t len = 0;

    uint8_t *data;

    uint8_t stackKey[stackLen];
public:

    Key() : data(nullptr) {}

    ~Key();

    Key(const Key &key) = delete;

    Key(Key &&key);
    Key& operator=(Key&& key);

    void set(const char bytes[], const std::size_t length);

    void operator=(const char key[]);

    bool operator==(const Key &k) const {
        if (k.getKeyLen() != getKeyLen()) {
            return false;
        }
        return std::memcmp(&k[0], data, getKeyLen()) == 0;
    }

    uint8_t &operator[](std::size_t i);

    const uint8_t &operator[](std::size_t i) const;

    KeyLen getKeyLen() const;

    void setKeyLen(KeyLen len);

    // Encode a user key into a sequence of ordered bytes
    static Key encode(int64_t key);

    // Decode the stored key into the original user key
    static int64_t decode(const Key& art_key);
};


inline uint8_t &Key::operator[](std::size_t i) {
    assert(i < len);
    return data[i];
}

inline const uint8_t &Key::operator[](std::size_t i) const {
    assert(i < len);
    return data[i];
}

inline KeyLen Key::getKeyLen() const { return len; }

inline Key::~Key() {
    if (len > stackLen) {
        delete[] data;
        data = nullptr;
    }
}

inline Key::Key(Key &&key) {
    len = key.len;
    if (len > stackLen) {
        data = key.data;
        key.data = nullptr;
    } else {
        memcpy(stackKey, key.stackKey, key.len);
        data = stackKey;
    }
}

inline Key& Key::operator=(Key &&key) {
    len = key.len;
    if (len > stackLen) {
        data = key.data;
        key.data = nullptr;
    } else {
        memcpy(stackKey, key.stackKey, key.len);
        data = stackKey;
    }
    return *this;
}

inline void Key::set(const char bytes[], const std::size_t length) {
    if (len > stackLen) {
        delete[] data;
    }
    if (length <= stackLen) {
        memcpy(stackKey, bytes, length);
        data = stackKey;
    } else {
        data = new uint8_t[length];
        memcpy(data, bytes, length);
    }
    len = length;
}

inline void Key::operator=(const char key[]) {
    if (len > stackLen) {
        delete[] data;
    }
    len = strlen(key);
    if (len <= stackLen) {
        memcpy(stackKey, key, len);
        data = stackKey;
    } else {
        data = new uint8_t[len];
        memcpy(data, key, len);
    }
}

inline void Key::setKeyLen(KeyLen newLen) {
    if (len == newLen) return;
    if (len > stackLen) {
        delete[] data;
    }
    len = newLen;
    if (len > stackLen) {
        data = new uint8_t[len];
    } else {
        data = stackKey;
    }
}

inline Key Key::encode(int64_t key){
    uint64_t encoded_key = __builtin_bswap64(key ^ (1ull <<63));
    Key art_key;
    art_key.data = art_key.stackKey;
    reinterpret_cast<uint64_t*>(art_key.data)[0] = encoded_key;  // store inside stackKey
    art_key.setKeyLen(sizeof(uint64_t)); // 8 bytes
    return art_key;
}

inline int64_t Key::decode(const Key& art_key){
    uint64_t encoded_key = reinterpret_cast<uint64_t*>(art_key.data)[0];
    return __builtin_bswap64(encoded_key) ^ (1ull <<63);
}

inline std::ostream& operator<<(std::ostream& out, const Key& key){
    out << "{KEY length: " << key.getKeyLen() << ", ";
    if( key.getKeyLen() == 8 ){
        uint64_t value = 0;
        for(int i = 0, sz = key.getKeyLen(); i < sz; i++){
            value += (static_cast<int64_t>(key[i]) << (8 * (7-i)));
        }
        out << "value=" << value << " (" << Key::decode(key) << ")" << std::dec << ", ";
    }
    out << "bytes={";
    for(int i = 0, sz = key.getKeyLen(); i < sz; i++){
        if(i > 0) out << ", ";
        out << i << "=" << (int) key[i];
    }
    out << "}";

    out << "}";

    return out;
}

} // namespace ART_OLC

#endif // ART_OLC_KEY_H
