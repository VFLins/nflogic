import pytest
import sqlite3
import copy
from datetime import datetime, timedelta, tzinfo

from nflogic.db import (
    gen_tablename,
    create_table,
    RowElem,
    insert_row,
)


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
        ("PagamentoValor", "100,0;10,2]", False,),
        ("DataHoraEmi", "2020-01-01T12:12:00-03:00", False),
        ("TotalDesconto", "abc", False),
        ("ChaveNFe", "123text23not12allowed32312312312312312312312", False),
    ],
)
def test_validation(upd_key: str, val: any, valid:bool):
    rowdata = copy.deepcopy(CORRECT_ROWDATA)
    if upd_key:
        rowdata[upd_key] = val

    if not valid:
        with pytest.raises(ValueError):
            # calls "self._validate_all()" on self.__init__()
            _ = RowElem(**rowdata)
    else:
        row = RowElem(**rowdata)
        for elem in rowdata.keys():
            assert rowdata[elem] == row.__dict__[elem]
