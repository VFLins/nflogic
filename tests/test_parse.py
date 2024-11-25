import pytest
from datetime import datetime, timedelta, tzinfo
import os
import copy
from nflogic.parse import (
    ParserInitError,
    valid_int,
    valid_float,
    valid_list_of_numbers,
    convert_to_list_of_numbers,
    convert_from_list_of_numbers,
    BaseParser,
    FactParser,
    FactRowElem,
)


SCRIPT_DIR = os.path.split(os.path.realpath(__file__))[0]
TEST_XML_V4 = os.path.join(SCRIPT_DIR, "test_xml_v4.xml")


class tzBrazilEast(tzinfo):

    def utcoffset(self, dt: datetime | None = None) -> timedelta:
        return timedelta(hours=-3) + self.dst(dt)

    def dst(self, dt: datetime | None = None):
        return timedelta(0)

    def tzname(self, dt: datetime | None) -> str | None:
        return "Brazil/East"


CORRECT_ROWDATA = {
    "ChaveNFe": "12312312312312312312312312312312312312312312",
    "DataHoraEmi": datetime(2020, 1, 1, 12, 12, 21, tzinfo=tzBrazilEast()),
    "PagamentoTipo": "1;4",
    "PagamentoValor": "100.0;10.2",
    "TotalProdutos": "110.2",
    "TotalDesconto": "0",
    "TotalTributos": "22.2",
}


@pytest.mark.parametrize(
    "val,expected",
    [("ABc", False), ("584", True), ("8.9", False), (123, True), (21.9, True)],
)
def test_valid_int(val, expected):
    assert valid_int(val) == expected


@pytest.mark.parametrize(
    "val,expected",
    [("ABc", False), ("584", True), ("8.9", True), (123, True), (21.9, True)],
)
def test_valid_float(val, expected):
    assert valid_float(val) == expected


@pytest.mark.parametrize(
    "val,expected",
    [
        ("543.5", True),
        ("5", True),
        ("12.3;34.5", True),
        ("1;2;3", True),
        ("[12;3]", False),
        ("-123", False),
        ("3.2.1", False),
        ("A1255", False),
        ("123;", False),
        (";123", False),
    ]
)
def test_valid_list_of_numbers(val, expected):
    """Test valid_list_of_numbers() function."""
    assert valid_list_of_numbers(val) == expected


def test_get_data():
    parser_inp = {
        "path": TEST_XML_V4,
        "buy": True,
    }
    dp = FactParser(parser_inp)
    dp.parse()

    if dp.erroed():
        raise dp.err[-1]

    assert dp.data.ChaveNFe == "26240811122233344455550010045645641789789784"
    assert dp.data.DataHoraEmi == datetime(
        2024, 8, 31, 16, 17, 16, tzinfo=tzBrazilEast()
    )
    assert dp.data.PagamentoTipo == "14"
    assert dp.data.PagamentoValor == "996.85"
    assert dp.data.TotalProdutos == "996.85"
    assert dp.data.TotalDesconto == "0.00"
    assert dp.data.TotalTributos == "348.77"


@pytest.mark.parametrize(
    "upd_key,val,valid",
    [
        (None, None, True),
        (
            "PagamentoValor",
            "100,0;10,2]",
            False,
        ),
        ("DataHoraEmi", "2020-01-01T12:12:00-03:00", False),
        ("TotalDesconto", "abc", False),
        ("ChaveNFe", "123text23not12allowed32312312312312312312312", False),
    ],
)
def test_validation(upd_key: str, val: any, valid: bool):
    rowdata = copy.deepcopy(CORRECT_ROWDATA)
    if upd_key:
        rowdata[upd_key] = val

    if not valid:
        with pytest.raises(ValueError):
            # calls "self._validate_all()" on self.__init__()
            _ = FactRowElem(**rowdata)
    else:
        row = FactRowElem(**rowdata)
        for elem in rowdata.keys():
            assert rowdata[elem] == row.__dict__[elem]


@pytest.mark.parametrize(
    "val,expected",
    [
        ([1, 2, 3, 4], "1;2;3;4"),
        ([123.45, 22.5], "123.45;22.5"),
        ([0], "0"),
        (34, "34"),
    ],
)
def test_convert_to_list_of_numbers(val, expected):
    assert convert_to_list_of_numbers(val) == expected


@pytest.mark.parametrize(
    "val,expected",
    [
        ("1;2;3;4", [1.0, 2.0, 3.0, 4.0]),
        ("123.45; 22.5", [123.45, 22.5]),
        ("0", [0.0]),
    ],
)
def test_convert_from_list_of_numbers(val, expected):
    """Test convert_from_list_of_numbers() function."""
    assert convert_from_list_of_numbers(val) == expected


def test_base_parser_init_key_error():
    """Test error raised when BaseParser is initiated missing an expected key in parser_input."""
    parser = BaseParser({"path":TEST_XML_V4})
    assert ParserInitError in [type(e) for e in parser.err]


def test_base_parser_init_input_type_error():
    """Test error raised when BaseParser is initiated with a wrong input type."""
    parser = BaseParser([TEST_XML_V4, True])
    assert ParserInitError in [type(e) for e in parser.err]
