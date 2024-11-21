import pytest
from pathlib import Path
from nflogic.cache import (
    valid_cachename,
    get_cachenames,
    CacheHandler,
    KeyAlreadyProcessedError,
    KeyNotFoundError,
)


MOCK_CACHE_VALUES = [
    {"path": "foo", "buy": True},
    {"path": "bar", "buy": False},
    {"path": "baz", "buy":True},
]


def test_valid_cachename():
    ch1 = CacheHandler("foo.c")
    ch2 = CacheHandler("bar.cache")
    ch3 = CacheHandler(".cache_baz")
    cache_names = [c.cachename for c in [ch1, ch2, ch3]]
    try:
        for cn in cache_names:
            assert valid_cachename(cn) == True
        not_cachename = "itsveryunlikelythatwewillhaveacachenamethisbigandspecific"
        assert valid_cachename(not_cachename) == False
    finally:
        for cache in [ch1, ch2, ch3]:
            Path(cache.cachefile).unlink()


def test_get_cachenames():
    pass


def test_add_rm_value():
    ch = CacheHandler("test_add_rm_value")
    for i, v in enumerate(MOCK_CACHE_VALUES):
        ch.add(v)
        assert ch.data == MOCK_CACHE_VALUES[: i + 1]
    for i, v in enumerate(MOCK_CACHE_VALUES):
        ch.rm(v)
        assert ch.data == MOCK_CACHE_VALUES[i + 1 :]
    Path(ch.cachefile).unlink()


def test_rm_error():
    try:
        ch = CacheHandler("test_rm_error")
        with pytest.raises(KeyNotFoundError):
            ch.rm(MOCK_CACHE_VALUES[0])
    finally:
        Path(ch.cachefile).unlink()


def test_add_error():
    try:
        ch = CacheHandler("test_add_error")
        ch.add(MOCK_CACHE_VALUES[0])
        with pytest.raises(KeyAlreadyProcessedError):
            ch.add(MOCK_CACHE_VALUES[0])
    finally:
        Path(ch.cachefile).unlink()


def test_data_persistance():
    try:
        ch = CacheHandler("test_data_persistance")
        ch.add(MOCK_CACHE_VALUES[0])
        del ch
        new_ch = CacheHandler("test_data_persistance")
        assert new_ch.data == [MOCK_CACHE_VALUES[0]]
    finally:
        Path(new_ch.cachefile).unlink()


def test_heal_file():
    ch = CacheHandler("test_heal_file")
    for item in MOCK_CACHE_VALUES:
        ch.add(item)
    try:
        Path(ch.cachefile).unlink()
        ch.rm(MOCK_CACHE_VALUES[2])
        assert ch._load() == MOCK_CACHE_VALUES[:2]
        ch.data = []
        ch.add(MOCK_CACHE_VALUES[2])
        assert ch.data == MOCK_CACHE_VALUES
    finally:
        Path(ch.cachefile).unlink()
