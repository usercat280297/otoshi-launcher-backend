"""
Python wrapper for native C++ lua loader
Optional performance optimization
"""
import ctypes
import os
from pathlib import Path
from typing import Optional, List, Dict

class LuaFile(ctypes.Structure):
    _fields_ = [
        ("app_id", ctypes.c_char * 32),
        ("content", ctypes.POINTER(ctypes.c_char)),
        ("size", ctypes.c_size_t),
    ]

class NativeLuaLoader:
    def __init__(self):
        self.lib = None
        self._load_library()
    
    def _load_library(self):
        """Load native library if available"""
        lib_paths = [
            Path("./lua_loader.dll"),
            Path("./native/lua_loader.dll"),
            Path("../native/lua_loader.dll"),
        ]
        
        for lib_path in lib_paths:
            if lib_path.exists():
                try:
                    self.lib = ctypes.CDLL(str(lib_path))
                    self._setup_functions()
                    return
                except Exception as e:
                    print(f"Failed to load {lib_path}: {e}")
        
        print("Native lua loader not available, using Python fallback")
    
    def _setup_functions(self):
        """Setup C function signatures"""
        # load_lua_files
        self.lib.load_lua_files.argtypes = [
            ctypes.c_char_p,
            ctypes.POINTER(ctypes.POINTER(LuaFile)),
            ctypes.POINTER(ctypes.c_int)
        ]
        self.lib.load_lua_files.restype = ctypes.c_int
        
        # free_lua_files
        self.lib.free_lua_files.argtypes = [
            ctypes.POINTER(LuaFile),
            ctypes.c_int
        ]
        
        # verify_lua_dir
        self.lib.verify_lua_dir.argtypes = [ctypes.c_char_p]
        self.lib.verify_lua_dir.restype = ctypes.c_int
    
    def load_lua_files(self, lua_dir: Path) -> Optional[List[Dict[str, str]]]:
        """Load all lua files from directory using native code"""
        if not self.lib:
            return None
        
        files_ptr = ctypes.POINTER(LuaFile)()
        count = ctypes.c_int(0)
        
        result = self.lib.load_lua_files(
            str(lua_dir).encode('utf-8'),
            ctypes.byref(files_ptr),
            ctypes.byref(count)
        )
        
        if result != 0:
            return None
        
        lua_files = []
        for i in range(count.value):
            lua_file = files_ptr[i]
            content = ctypes.string_at(lua_file.content, lua_file.size).decode('utf-8')
            lua_files.append({
                'app_id': lua_file.app_id.decode('utf-8'),
                'content': content
            })
        
        self.lib.free_lua_files(files_ptr, count)
        return lua_files
    
    def verify_lua_dir(self, lua_dir: Path) -> int:
        """Verify lua directory and return file count"""
        if not self.lib:
            return -1
        
        return self.lib.verify_lua_dir(str(lua_dir).encode('utf-8'))

# Global instance
_native_loader = NativeLuaLoader()

def load_lua_files_fast(lua_dir: Path) -> Optional[List[Dict[str, str]]]:
    """Fast lua loading using native code if available"""
    return _native_loader.load_lua_files(lua_dir)

def verify_lua_dir(lua_dir: Path) -> int:
    """Verify lua directory"""
    return _native_loader.verify_lua_dir(lua_dir)
