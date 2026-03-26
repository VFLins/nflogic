import os
import pytest
import asyncio
import aiosqlite
from pathlib import Path

from nflogic.api.parse import FactParser, FactRowElem, ParserInput
from nflogic.api.db import fmt_tablename, insert_fact_row, DB_DIR


SCRIPT_PATH = os.path.realpath(__file__)
SCRIPT_DIR = os.path.split(SCRIPT_PATH)[0]


@pytest.fixture(scope="function")
def temporary_db_path() -> Path:
    dirpath = Path(DB_DIR, "temporary_test_db.sqlite")
    yield dirpath
    dirpath.unlink()


def test_tablename():
    parser_inp: ParserInput = {
        "path": os.path.join(SCRIPT_DIR, "test_xml_v4.xml"),
        "buy": False,
    }
    parser = FactParser(parser_inp)
    if parser.name is None:
        raise ValueError("Fetched a parser that could not get it's name.")
    tname = fmt_tablename(parser.name)
    assert tname == "VENDA_FORNECEDOR"


@pytest.mark.asyncio
async def test_insert_fact_row(temporary_db_path: Path):
    parser_inp: ParserInput = {
        "path": os.path.join(SCRIPT_DIR, "test_xml_v4.xml"),
        "buy": False,
    }
    parser = FactParser(parser_inp)
    parser.parse()
    if parser.name is None:
        raise ValueError("Fetched a parser that could not get it's name.")
    async with aiosqlite.connect(temporary_db_path) as con:
        row, tablename = parser.data[0], fmt_tablename(parser.name)
        await insert_fact_row(row=row, tablename=tablename, db_path=temporary_db_path)
        cur = await con.execute(f"SELECT * FROM {tablename}")
        res = await cur.fetchall()
    assert len(res) == 1
    assert res[0][1:] == row.values
