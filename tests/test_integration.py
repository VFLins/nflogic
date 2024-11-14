import os
import sqlite3

from nflogic.parse import FactParser, FactRowElem
from nflogic.db import gen_tablename, insert_row


SCRIPT_PATH = os.path.realpath(__file__)
SCRIPT_DIR = os.path.split(SCRIPT_PATH)[0]


def test_tablename():
    parser_inp = {
        "path": os.path.join(SCRIPT_DIR, "test_xml_v4.xml"),
        "buy": False,
    }
    p = FactParser(parser_inp)
    tname = gen_tablename(p.name)
    assert tname == "VENDA_FORNECEDOR"


def test_insert_row():
    parser_inp = {
        "path": os.path.join(SCRIPT_DIR, "test_xml_v4.xml"),
        "buy": False,
    }
    with sqlite3.connect(":memory:") as con:
        p = FactParser(parser_inp)
        p.parse()
        if p.erroed():
            raise p.err[-1]
        row = p.data
        insert_row(parser=p, con=con, close=False)
        cur = con.cursor()
        cur.execute(f"SELECT * FROM {gen_tablename(p.name)}")
        res = cur.fetchall()

    assert len(res) == 1
    assert res[0][1:] == row.values
