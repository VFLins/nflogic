import os
import sqlite3

from nflogic.parse import FactParser
from nflogic.db import gen_tablename, RowElem, insert_row


SCRIPT_PATH = os.path.realpath(__file__)
SCRIPT_DIR = os.path.split(SCRIPT_PATH)[0]


def test_tablename():
    p = FactParser(os.path.join(SCRIPT_DIR, "test_xml_v4.xml"))
    tname = gen_tablename(p.name)
    assert tname == "IMPERADOR_AUGUSTO_MERCEARIA"


def test_insert_row():
    with sqlite3.connect(":memory:") as con:
        p = FactParser(os.path.join(SCRIPT_DIR, "test_xml_v4.xml"))
        p.parse()
        if p.erroed:
            raise p.err
        row = RowElem(**p.data)
        insert_row(parser=p, con=con, close=False)
        cur = con.cursor()
        cur.execute(f"SELECT * FROM {gen_tablename(p.name)}")
        res = cur.fetchall()

    assert len(res) == 1
    assert res[0][1:] == row.values
