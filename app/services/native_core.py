import ctypes
import sys
from typing import Optional

from ..core.config import LAUNCHER_CORE_PATH


class NativeLauncherCore:
    def __init__(self, library_path: Optional[str] = None) -> None:
        self._lib = None
        self._score_search = None
        self._load(library_path)

    @property
    def available(self) -> bool:
        return self._lib is not None

    def version(self) -> str:
        if not self._lib:
            return "unavailable"
        self._lib.launcher_core_version.restype = ctypes.c_char_p
        result = self._lib.launcher_core_version()
        return result.decode("ascii") if result else "unknown"

    def hash_file(self, path: str) -> str:
        if not self._lib:
            raise RuntimeError("native core unavailable")
        buffer = ctypes.create_string_buffer(65)
        result = self._lib.launcher_hash_file(
            path.encode("utf-8"), buffer, ctypes.sizeof(buffer)
        )
        if result != 0:
            raise RuntimeError(self.last_error())
        return buffer.value.decode("ascii")

    def build_manifest(self, directory: str, output_path: str, chunk_size: int) -> None:
        if not self._lib:
            raise RuntimeError("native core unavailable")
        result = self._lib.launcher_build_manifest(
            directory.encode("utf-8"),
            output_path.encode("utf-8"),
            ctypes.c_uint32(chunk_size),
        )
        if result != 0:
            raise RuntimeError(self.last_error())

    def score_search(self, query: str, candidate: str) -> float:
        if not self._lib or not self._score_search:
            raise RuntimeError("native core unavailable")
        return float(self._score_search(query.encode("utf-8"), candidate.encode("utf-8")))

    def last_error(self) -> str:
        if not self._lib:
            return "native core unavailable"
        buffer = ctypes.create_string_buffer(512)
        self._lib.launcher_last_error(buffer, ctypes.sizeof(buffer))
        return buffer.value.decode("utf-8", errors="replace")

    def _load(self, library_path: Optional[str]) -> None:
        candidates = []
        env_path = LAUNCHER_CORE_PATH.strip()
        if library_path:
            candidates.append(library_path)
        if env_path:
            candidates.append(env_path)

        if sys.platform.startswith("win"):
            candidates.append("launcher_core.dll")
        elif sys.platform == "darwin":
            candidates.append("liblauncher_core.dylib")
        else:
            candidates.append("liblauncher_core.so")

        for candidate in candidates:
            try:
                self._lib = ctypes.CDLL(candidate)
                break
            except OSError:
                continue

        if not self._lib:
            return

        self._lib.launcher_hash_file.argtypes = [
            ctypes.c_char_p,
            ctypes.c_void_p,
            ctypes.c_size_t,
        ]
        self._lib.launcher_hash_file.restype = ctypes.c_int

        self._lib.launcher_build_manifest.argtypes = [
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_uint32,
        ]
        self._lib.launcher_build_manifest.restype = ctypes.c_int

        self._score_search = getattr(self._lib, "launcher_score_search", None)
        if self._score_search:
            self._score_search.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
            self._score_search.restype = ctypes.c_float

        self._lib.launcher_last_error.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
        self._lib.launcher_last_error.restype = ctypes.c_size_t


_native_core: Optional[NativeLauncherCore] = None


def get_native_core() -> Optional[NativeLauncherCore]:
    global _native_core
    if _native_core is None:
        core = NativeLauncherCore()
        _native_core = core if core.available else None
    return _native_core
