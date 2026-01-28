import os
import mmap
import struct

MAGIC = b"ULTRAMM1"
HEADER_STRUCT = struct.Struct("<8sIQ")  # magic, key_size, record_count
OFFSET_STRUCT = struct.Struct("<QI")    # offset, length


class SequenceMmapStore:
    def __init__(self, index_path, data_path):
        self._index_fd = open(index_path, "rb")
        self._data_fd = open(data_path, "rb")
        self._index_mm = mmap.mmap(self._index_fd.fileno(), 0, access=mmap.ACCESS_READ)
        self._data_mm = mmap.mmap(self._data_fd.fileno(), 0, access=mmap.ACCESS_READ)

        magic, key_size, count = HEADER_STRUCT.unpack_from(self._index_mm, 0)
        if magic != MAGIC:
            raise ValueError("Invalid mmap index file magic: {0}".format(index_path))

        self._key_size = int(key_size)
        self._count = int(count)
        self._header_size = HEADER_STRUCT.size
        self._record_size = self._key_size + OFFSET_STRUCT.size

    def __len__(self):
        return self._count

    def __contains__(self, key):
        return self._find_index(key) >= 0

    def get(self, key, default=None):
        idx = self._find_index(key)
        if idx < 0:
            return default
        offset, length = self._offset_and_length(idx)
        return self._data_mm[offset:offset + length].decode("ascii")

    def __getitem__(self, key):
        value = self.get(key, None)
        if value is None:
            raise KeyError(key)
        return value

    def close(self):
        try:
            self._index_mm.close()
        finally:
            self._index_fd.close()
        try:
            self._data_mm.close()
        finally:
            self._data_fd.close()

    def _offset_and_length(self, idx):
        base = self._header_size + (idx * self._record_size) + self._key_size
        return OFFSET_STRUCT.unpack_from(self._index_mm, base)

    def _key_at(self, idx):
        base = self._header_size + (idx * self._record_size)
        return self._index_mm[base:base + self._key_size]

    def _find_index(self, key):
        if not isinstance(key, (bytes, bytearray, memoryview)):
            return -1
        key_bytes = bytes(key)
        if len(key_bytes) != self._key_size:
            return -1
        lo = 0
        hi = self._count - 1
        while lo <= hi:
            mid = (lo + hi) // 2
            mid_key = self._key_at(mid)
            if mid_key == key_bytes:
                return mid
            if mid_key < key_bytes:
                lo = mid + 1
            else:
                hi = mid - 1
        return -1


def build_sequence_store(index_folder, basename, seq_dict):
    if not seq_dict:
        return

    index_path = os.path.join(index_folder, basename + ".mmidx")
    data_path = os.path.join(index_folder, basename + ".mmdata")

    items = list(seq_dict.items())
    first_key = items[0][0]
    if not isinstance(first_key, (bytes, bytearray, memoryview)):
        raise ValueError("Keys must be bytes for mmap store: {0}".format(basename))
    key_size = len(first_key)

    for key, _ in items:
        if not isinstance(key, (bytes, bytearray, memoryview)):
            raise ValueError("Keys must be bytes for mmap store: {0}".format(basename))
        if len(key) != key_size:
            raise ValueError("Inconsistent key size in mmap store: {0}".format(basename))

    items.sort(key=lambda kv: bytes(kv[0]))

    with open(data_path, "wb") as data_f, open(index_path, "wb") as idx_f:
        idx_f.write(HEADER_STRUCT.pack(MAGIC, key_size, len(items)))
        offset = 0
        for key, seq in items:
            if isinstance(seq, str):
                seq_bytes = seq.encode("ascii")
            else:
                seq_bytes = bytes(seq)
            data_f.write(seq_bytes)
            idx_f.write(bytes(key))
            idx_f.write(OFFSET_STRUCT.pack(offset, len(seq_bytes)))
            offset += len(seq_bytes)
