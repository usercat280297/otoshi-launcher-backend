from __future__ import annotations

from datetime import date
from typing import Optional
import ctypes
import os

_NATIVE_AGE_CALCULATOR = None


def _load_native_age_calculator():
    global _NATIVE_AGE_CALCULATOR
    if _NATIVE_AGE_CALCULATOR is not None:
        return _NATIVE_AGE_CALCULATOR
    lib_path = os.getenv("LAUNCHER_CORE_PATH", "")
    if not lib_path:
        _NATIVE_AGE_CALCULATOR = None
        return None
    try:
        lib = ctypes.CDLL(lib_path)
    except OSError:
        _NATIVE_AGE_CALCULATOR = None
        return None
    try:
        func = lib.launcher_calculate_age
        func.argtypes = [ctypes.c_uint32, ctypes.c_uint32, ctypes.c_uint32]
        func.restype = ctypes.c_int
        _NATIVE_AGE_CALCULATOR = func
        return func
    except AttributeError:
        _NATIVE_AGE_CALCULATOR = None
        return None


def _validate_birthdate(year: int, month: int, day: int) -> date:
    today = date.today()
    try:
        born = date(year, month, day)
    except ValueError as exc:
        raise ValueError("Invalid birth date") from exc
    if born > today:
        raise ValueError("Birth date cannot be in the future")
    return born


def calculate_age(year: int, month: int, day: int) -> int:
    born = _validate_birthdate(year, month, day)
    native = _load_native_age_calculator()
    if native:
        native_age = native(year, month, day)
        if native_age >= 0:
            return int(native_age)
    today = date.today()
    age = today.year - born.year
    if (today.month, today.day) < (born.month, born.day):
        age -= 1
    return age


def verify_age(required_age: Optional[int], year: int, month: int, day: int) -> dict:
    required_age = int(required_age or 0)
    if required_age < 0:
        required_age = 0
    age = calculate_age(year, month, day)
    allowed = required_age <= 0 or age >= required_age
    return {"allowed": allowed, "age": age, "required_age": required_age}
