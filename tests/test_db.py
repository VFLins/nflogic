import pytest
import sqlite3
from pathlib import Path
import os
from datetime import datetime, timedelta, tzinfo

from nflogic.parse import FactParser, FactRowElem
from nflogic.db import (
    fmt_tablename,
    create_fact_table,
    insert_fact_row,
    processed_keys,
)


TEST_DIR = os.path.split(os.path.realpath(__file__))[0]
TEST_PARSER_INPUTS = {
    "v4_buy": {"path": str(Path(TEST_DIR, "test_xml_v4.xml")), "buy": True},
    "v4_sell": {"path": str(Path(TEST_DIR, "test_xml_v4.xml")), "buy": False},
}


class tzBrazilEast(tzinfo):

    def utcoffset(self, dt: datetime | None = None) -> timedelta:
        return timedelta(hours=-3) + self.dst(dt)

    def dst(self, dt: datetime | None = None):
        return timedelta(0)

    def tzname(self, dt: datetime | None) -> str | None:
        return "Brazil/East"


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
def test_fmt_tablename(name: str, expect: str):
    assert fmt_tablename(name) == expect


def test_create_fact_table():
    with sqlite3.connect(":memory:") as con:
        create_fact_table(tablename="Nome da Empresa", con=con, close=False)
        create_fact_table(tablename="Empresa com número 345", con=con, close=False)
        cursor = con.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        assert cursor.fetchall() == [("NOME_DA_EMPRESA",), ("EMPRESA_COM_NÚMERO_345",)]


def test_processed_keys():
    """Test processed_keys() function."""
    with sqlite3.connect(":memory:") as con:
        parser = FactParser(TEST_PARSER_INPUTS["v4_sell"])
        parser.parse()
        tablename = fmt_tablename(parser.name)
        insert_fact_row(row=parser.data[0], tablename=tablename, con=con, close=False)
        keys = processed_keys(tablename=tablename, con=con, close=False)
        assert keys == ["26240811122233344455550010045645641789789784"]


def not_test_insert_fact_row_fail():
    """Test fail cases of insert_fact_row() fail."""
    with sqlite3.connect(":memory:") as con:
        parser = FactParser(TEST_PARSER_INPUTS["v4_buy"])
        parser.parse()
        with pytest.raises(ValueError):
            insert_fact_row(
                row=parser.data[0], tablename=parser.name, con=con, close=False
            )
