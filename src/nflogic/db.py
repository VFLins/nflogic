from datetime import datetime
import sqlite3
import re
import os

from nflogic.parse import FactParser


# CONSTANTS
###############

SCRIPT_PATH = os.path.realpath(__file__)
SCRIPT_DIR = os.path.split(SCRIPT_PATH)[0]
DB_DIR = os.path.join(SCRIPT_DIR, "database")
DB_PATH = os.path.join(DB_DIR, "db.sqlite")

os.makedirs(DB_DIR, exist_ok=True)


# DB HANDLING
###############


def gen_tablename(name: str):
    """Transforms strings to a format that SQLite would accept as a table name:

    1. Removes all leading numbers,
    2. Removes all special characters,
    3. Replace spaces by underscores,
    4. All letters uppercased.

    **Args**
        name (str): The name to be formatted.

    **Returns** str
        The formatted `name`.
    """

    return re.sub(r"[^\w\s]", "", re.sub(r"^\d+", "", name)).replace(" ", "_").upper()


def processed_keys(
    tablename: str,
    con: sqlite3.Connection = sqlite3.connect(DB_PATH),
    close: bool = False,
):
    """Read `tablename` and returns all keys present in table.

    **Args**
        con (sqlite3.Connection): Connection to desired database.
        tablename (str): Name of the table that will be read.
        close (bool): Should close the connection `con` after the operation completes?

    **Returns** `List[str]`
        List of all corresponding keys.

    **Raises**
        `sqlite3.OperationalError` if table doesn't exist.
    """

    tablename = gen_tablename(tablename)
    create_table(con, tablename)

    dbcur = con.cursor()
    dbcur.execute(f"SELECT ChaveNFe FROM {tablename}")
    output = dbcur.fetchall()

    if close:
        con.close()

    return [elem[0] for elem in output]


def create_table(con: sqlite3.Connection, tablename: str, close: bool = False):
    """Create table with the provided name formatted by `nflogic.db.gen_tablename()`.
    Should *not* be called directly. *Does nothing if:*
    - table already exists
    - invalid name

    **Args**
        con (sqlite3.Connection): Connection to desired database.
        tablename (str): Name of the table that will be created.
        close (bool): Should close the connection `con` after the operation completes?

    **Returns** None
    """
    dbcur = con.cursor()
    dbcur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {gen_tablename(tablename)} (
            Id INTEGER PRIMARY KEY,
            ChaveNFe TEXT NOT NULL UNIQUE,
            DataHoraEmi TEXT,
            PagamentoTipo TEXT,
            PagamentoValor TEXT,
            TotalProdutos REAL,
            TotalDesconto REAL,
            TotalTributos REAL
        );
        """
    )
    if close:
        con.close()


def insert_row(
    parser: FactParser,
    con: sqlite3.Connection = sqlite3.connect(DB_PATH),
    close: bool = False,
):
    """
    Inserts a row of data on the database associated with `con` the table
    name will be collected from `parser` and formatted by `gen_tablename()`.

    **Args**
        parser (nflogic.parse.FactParser): Parser that holds the data that will be inserted.
        con (sqlite3.Connection): Connection to desired database;
        close (bool): Should close the connection `con` after the operation completes?

    **Returns** None

    **Raises**
        `ValueError` if *parser* doesn't hold data.
        `sqlite3.OperationalError` if table doesn't exist.
    """
    if not parser.data:
        raise ValueError(f"Parser with inputs '{parser.INPUTS}' doesn't have any data to insert.")

    tablename = gen_tablename(parser.name)
    create_table(con, tablename=tablename)

    dbcur = con.cursor()
    dbcur.execute(
        f"""INSERT INTO {gen_tablename(tablename)} (
            ChaveNFe,
            DataHoraEmi,
            PagamentoTipo,
            PagamentoValor,
            TotalProdutos,
            TotalDesconto,
            TotalTributos
        ) VALUES (?,?,?,?,?,?,?);""",
        parser.data.values,
    )
    con.commit()

    if close:
        con.close()
