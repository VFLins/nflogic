import pytest
import sqlite3
from pathlib import Path
import os
import copy
from datetime import datetime, timedelta, tzinfo

from nflogic.parse import FactRowElem, FactParser
from nflogic.db import (
    gen_tablename,
    create_table,
    insert_row,
    processed_keys,
)


TEST_DIR = os.path.split(os.path.realpath(__file__))[0]
TEST_PARSER_INPUTS = {
    "v4_buy": {"path": str(Path(TEST_DIR, "test_xml_v4.xml")), "buy": True},
    "v4_sell": {"path": str(Path(TEST_DIR, "test_xml_v4.xml")), "buy": False}
}

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
    "PagamentoTipo": "[1;4]",
    "PagamentoValor": "[100.0;10.2]",
    "TotalProdutos": "110.2",
    "TotalDesconto": "0",
    "TotalTributos": "22.2",
}


@pytest.mark.parametrize(
    "name,expect",
    [
        ("123 empresa diferente 11122233", "_EMPRESA_DIFERENTE_11122233"),
        ("MERC. COMERCIANTES", "MERC_COMERCIANTES"),
        ("Sociedade Anônima S/A", "SOCIEDADE_ANÔNIMA_SA"),
        ("algo com asterisco* ltda.", "ALGO_COM_ASTERISCO_LTDA"),
        ("ACADEMIA DOS NÚMEROS IND.-COM.", "ACADEMIA_DOS_NÚMEROS_INDCOM"),
    ],
)
def test_gen_tablename(name: str, expect: str):
    assert gen_tablename(name) == expect


def test_create_table():
    with sqlite3.connect(":memory:") as con:
        create_table(con, "Nome da Empresa", close=False)
        create_table(con, "Empresa com número 345", close=False)
        cursor = con.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        assert cursor.fetchall() == [("NOME_DA_EMPRESA",), ("EMPRESA_COM_NÚMERO_345",)]


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


def test_processed_keys():
    """Test processed_keys() function."""
    with sqlite3.connect(":memory:") as con:
        parser = FactParser(TEST_PARSER_INPUTS["v4_sell"])
        parser.parse()
        tablename = gen_tablename(parser.name) 
        insert_row(parser=parser, con=con, close=False)
        keys = processed_keys(tablename=tablename, con=con, close=False)
        assert keys == ["26240811122233344455550010045645641789789784"]


def test_insert_row_fail():
    """Test fail cases of insert_row_fail()."""
    with sqlite3.connect(":memory:") as con:
        parser = FactParser(TEST_PARSER_INPUTS["v4_buy"])
        with pytest.raises(ValueError):
            insert_row(parser=parser, con=con, close=False)
