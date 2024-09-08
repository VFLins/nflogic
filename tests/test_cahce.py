import pytest
from pathlib import Path
from nflogic.cache import CacheHandler, KeyAlreadyProcessedError, KeyNotFoundError


def test_add_rm_value():
    values = ["foo", "bar", "baz"]
    ch = CacheHandler("test_add_rm_value")

    for i, v in enumerate(values):
        ch.add(v)
        assert ch.data == values[: i + 1]

    for i, v in enumerate(values):
        ch.rm(v)
        assert ch.data == values[i + 1 :]

    Path(ch.cachefile).unlink()


def test_rm_error():
    try:
        ch = CacheHandler("test_rm_error")
        with pytest.raises(KeyNotFoundError):
            ch.rm("something")

    finally:
        Path(ch.cachefile).unlink()


def test_add_error():
    try:
        ch = CacheHandler("test_add_error")
        ch.add("foo")
        with pytest.raises(KeyAlreadyProcessedError):
            ch.add("foo")

    finally:
        Path(ch.cachefile).unlink()


def test_data_persistance():
    try:
        ch = CacheHandler("test_data_persistance")
        ch.add("foo")
        del ch

        new_ch = CacheHandler("test_data_persistance")
        assert new_ch.data == ["foo"]

    finally:
        Path(new_ch.cachefile).unlink()


def test_heal_file():
    ch = CacheHandler("test_heal_file")
    for item in ["foo", "bar", "baz"]:
        ch.add(item)

    try:
        Path(ch.cachefile).unlink()
        ch.rm("bar")
        assert ch._load() == ["foo", "baz"]

        ch.data = []
        ch.add("bar")
        assert ch.data == ["foo", "baz", "bar"]

    finally:
        Path(ch.cachefile).unlink()
