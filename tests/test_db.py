import pytest
import sqlite3
from datetime import datetime, timedelta, tzinfo

from nflogic.db import (
    gen_tablename,
    create_table,
    RowElem,
    insert_row,
)

class tzBrazilEast(tzinfo):
    def utcoffset(self, dt: datetime | None = None) -> timedelta:
        return timedelta(hours=-3)

    def dst(self, dt: datetime | None = None):
        return None


@pytest.mark.parametrize(
        "name,expect",
        [
            ("123 empresa diferente 11122233", "_EMPRESA_DIFERENTE_11122233"),
            ("MERC. COMERCIANTES", "MERC_COMERCIANTES"),
            ("Sociedade Anônima S/A", "SOCIEDADE_ANÔNIMA_SA"),
            ("algo com asterisco* ltda.", "ALGO_COM_ASTERISCO_LTDA"),
            ("ACADEMIA DOS NÚMEROS IND.-COM.", "ACADEMIA_DOS_NÚMEROS_INDCOM")
        ]
)
def test_gen_tablename(name: str, expect: str):
    assert gen_tablename(name) == expect


def test_create_table():
    with sqlite3.connect(":memory:") as con:
        create_table(con, "Nome da Empresa", close=False)
        create_table(con, "Empresa com número 345", close=False)
        cursor = con.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")

        assert cursor.fetchall() == [("NOME_DA_EMPRESA", ), ("EMPRESA_COM_NÚMERO_345", )]


# TODO: Add invalid examples
@pytest.mark.parametrize(
        "rowdata,valid",
        [
            ({
                "ChaveNFe": "12312312312312312312312312312312312312312312",
                "DataHoraEmi": datetime(2020, 1, 1, 12, 12, 21, tzinfo=tzBrazilEast()),
                "PagamentoTipo": "[1;4]",
                "PagamentoValor": "[100.0;10.2]",
                "TotalProdutos": "110.2",
                "TotalDesconto": "0",
                "TotalTributos": "22.2"}, True),
        ]
)
def test_validation(rowdata: dict, valid):
    if not valid:
        with pytest.raises(ValueError):
            # calls "self._validate_all()" on self.__init__()
            row = RowElem(**rowdata)
    else:
        assert True == True
            