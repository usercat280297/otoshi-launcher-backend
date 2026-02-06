import hashlib
from typing import Generator


def iter_chunk_bytes(seed: bytes, size: int) -> Generator[bytes, None, None]:
    counter = 0
    remaining = size
    while remaining > 0:
        counter_bytes = counter.to_bytes(4, "little", signed=False)
        block = hashlib.sha256(seed + counter_bytes).digest()
        take = min(remaining, len(block))
        yield block[:take]
        remaining -= take
        counter += 1


def chunk_hash(seed: bytes, size: int) -> str:
    hasher = hashlib.sha256()
    for block in iter_chunk_bytes(seed, size):
        hasher.update(block)
    return hasher.hexdigest()


def file_hash(seed_prefix: bytes, size: int, chunk_size: int) -> str:
    hasher = hashlib.sha256()
    chunk_count = max(1, (size + chunk_size - 1) // chunk_size)
    for index in range(chunk_count):
        chunk_size_bytes = chunk_size
        if index == chunk_count - 1:
            chunk_size_bytes = size - (chunk_size * (chunk_count - 1))
        seed = seed_prefix + b":" + str(index).encode("ascii")
        for block in iter_chunk_bytes(seed, chunk_size_bytes):
            hasher.update(block)
    return hasher.hexdigest()