import pytest
import os
from pathlib import Path
from tempfile import TemporaryFile, TemporaryDirectory
from nflogic import parse
from nflogic.cache import (
    CACHE_PATH,
    _save_successfull_fileparse,
    get_not_processed_inputs,
    valid_cachename,
    get_cachenames,
    CacheHandler,
    KeyAlreadyProcessedError,
    KeyNotFoundError,
)


MOCK_CACHE_VALUES = [
    {"path": "foo", "buy": True},
    {"path": "bar", "buy": False},
    {"path": "baz", "buy": True},
]


def test_cache_format():
    try:
        ch1 = CacheHandler("foo.c")
        ch2 = CacheHandler("bar.cache")
        ch3 = CacheHandler(".cache_baz")
        cache_names = [c.cachename for c in [ch1, ch2, ch3]]
        for cn in cache_names:
            assert CacheHandler(cn).is_valid()
    finally:
        for cache in [ch1, ch2, ch3]:
            Path(cache.cachefile).unlink()


def test_get_cachenames():
    new_cache = CacheHandler("Ensure at least one cache file")
    try:
        # test if all names gotten were valid
        cache_names = get_cachenames()
        for name in cache_names:
            assert valid_cachename(name) == True
        # test if got all relevant names
        with TemporaryFile("x", dir=CACHE_PATH):
            filenames = [
                os.path.splitext(f)[0]
                for f in os.listdir(CACHE_PATH)
                if Path(CACHE_PATH, f).is_file()
            ]
            for name in filenames:
                if name not in cache_names:
                    assert valid_cachename(name) == False
    finally:
        Path(new_cache.cachefile).unlink()


def test_add_rm_value():
    try:
        ch = CacheHandler("test_add_rm_value")
        for i, v in enumerate(MOCK_CACHE_VALUES):
            ch.add(v)
            assert ch.data == MOCK_CACHE_VALUES[: i + 1]
        for i, v in enumerate(MOCK_CACHE_VALUES):
            ch.rm(v)
            assert ch.data == MOCK_CACHE_VALUES[i + 1 :]
    finally:
        Path(ch.cachefile).unlink()


@pytest.mark.parametrize(
    "item",
    [
        {"path": str(Path(CACHE_PATH, "filename.cache")), "buy": "False"},
        {"path": 112233, "buy": False},
    ],
)
def test_check_item_fail(item):
    """Test fail cases of CacheHandler._check_item() method."""
    # will raise TypeError if doesn't find an item of incorrect type
    with pytest.raises(TypeError):
        ch = CacheHandler("test_check_item")
        ch._check_item(item)


def test_rm_error():
    """Test CacheHandler.rm() method."""
    try:
        ch = CacheHandler("test_rm_error")
        with pytest.raises(KeyNotFoundError):
            ch.rm(MOCK_CACHE_VALUES[0])
    finally:
        Path(ch.cachefile).unlink()


def test_is_valid():
    """test CacheHandler.is_valid() method."""
    try:
        ch = CacheHandler("test_is_valid")
        # false if is not list
        ch.data = "this is a string, not a list"
        assert ch.is_valid() == False
        # false if an item of the list is not ParserInput type
        ch.data = [
            MOCK_CACHE_VALUES[0],
            MOCK_CACHE_VALUES[1],
            "this string is not of ParserInput type",
        ]
        assert ch.is_valid() == False
        # true case
        ch.data = MOCK_CACHE_VALUES
        assert ch.is_valid() == True
    finally:
        Path(ch.cachefile).unlink()


def test_add_error():
    """test CacheHandler.add() method."""
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
    """Test CacheHandler._heal() method, rebuilding memory data from file."""
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


def test_first_invalid_elem():
    """Test CacheHandler._first_invalid_elem() method."""
    try:
        ch = CacheHandler("test_first_invalid_elem")
        ch.data = MOCK_CACHE_VALUES
        assert ch._first_invalid_elem() == None
        # return not dict
        ch.data.append("invalid value")
        assert ch._first_invalid_elem() == "invalid value"
        _ = ch.data.pop()
        # return missing key
        ch.data.append({"path": "some path"})
        assert ch._first_invalid_elem() == {"path": "some path"}
        _ = ch.data.pop()
        # return key of wrong type
        ch.data.append({"path": "some path", "buy": "should be boolean"})
        assert ch._first_invalid_elem() == {
            "path": "some path",
            "buy": "should be boolean",
        }
        _ = ch.data.pop()
        ch.data.append({"path": 1234, "buy": True})
        assert ch._first_invalid_elem() == {"path": 1234, "buy": True}
    finally:
        Path(ch.cachefile).unlink()


def test_get_not_processed_inputs_fact():
    """Test get_not_processed_inputs() when parsing only fact table data."""
    with TemporaryDirectory() as dir:
        file_paths = [os.path.join(dir, f"file{i}.xml") for i in range(5)]
        for id, file_path in enumerate(file_paths):
            buffer = open(file_path, "w+")
            buffer.write(
                f"""<nfeProc xmlns="http://www.portalfiscal.inf.br/nfe" versao="4.00"><NFe xmlns="http://www.portalfiscal.inf.br/nfe"><infNFe Id="NFe{id}" versao="4.00"><emit><xNome>TEST_SELLER</xNome></emit><dest><xNome>TEST_BUYER</xNome></dest></infNFe></NFe></nfeProc>"""
            )
            buffer.close()
            result = get_not_processed_inputs(
                file_paths, buy=True, ignore_fails=False, full_parse=False
            )
            expected = [{"path": file, "buy": True} for file in file_paths[id:]]
            assert list(result) == expected
            _save_successfull_fileparse(
                parse.FactParser({"path": file_path, "buy": True})
            )

        # cleanup
        success_cache = CacheHandler("__fact_table_success__", full_parse=False)
        for parser_input in [{"path": file, "buy": True} for file in file_paths]:
            success_cache.rm(parser_input)


def test_get_not_processed_inputs_ignore_data():
    """Test if test_get_not_processed_inputs()'s `ignore_data` are evaluated correctly."""
    # TODO
    pass
