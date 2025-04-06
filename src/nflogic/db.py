from datetime import datetime
import sqlite3
import re
import os

from nflogic.parse import FactParser, FullParser, TransacRowElem, FactRowElem


# CONSTANTS
###############

SCRIPT_PATH = os.path.realpath(__file__)
SCRIPT_DIR = os.path.split(SCRIPT_PATH)[0]
DB_DIR = os.path.join(SCRIPT_DIR, "database")
DB_PATH = os.path.join(DB_DIR, "db.sqlite")

os.makedirs(DB_DIR, exist_ok=True)


# DB HANDLING
###############


def fmt_tablename(name: str):
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

    tablename = fmt_tablename(tablename)
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
        CREATE TABLE IF NOT EXISTS {fmt_tablename(tablename)} (
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


def insert_transac_row(
        row: TransacRowElem,
        tablename: str,
        con: sqlite3.Connection = sqlite3.connect(DB_PATH),
        close: bool = False,
    ):
    """
    Inserts a row of data from a TransacRowElem.

    **Args**
        Row (nflogic.parse.TransacRowElem): Element that holds the data that will be inserted;
        tablename (str): Name of the table where the data will be inserted into;
        con (sqlite3.Connection): Connection to desired database;
        close (bool): Should close the connection after the operation completes?

    **Returns** `None`

    **Raises**
        `ValueError` if *row* doesn't hold data.
        `sqlite3.OperationalError` if table doesn't exist.
    """
    create_table(con, tablename=tablename)

    dbcur = con.cursor()
    dbcur.execute(
        f"""INSERT INTO {fmt_tablename(tablename)} (
            ChaveNFe,
            CodProduto,
            CodBarras,
            CodNCM,
            CodCEST,
            CodCFOP,
            QuantComercial,
            QuantTributavel,
            UnidComercial,
            UnidTributavel,
            DescricaoProd,
            ValorUnitario,
            BaseCalcPIS,
            ValorPIS,
            BaseCalcCOFINS,
            ValorCOFINS,
            BaseCalcRetidoICMS,
            ValorRetidoICMS,
            ValorSubstitutoICMS,
            BaseCalcEfetivoICMS,
            ValorEfetivoICMS,
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""",
        row.values,
    )
    con.commit()
    if close:
        con.close()


def insert_fact_row(
        row: FactRowElem,
        tablename: str,
        con: sqlite3.Connection = sqlite3.connect(DB_PATH),
        close: bool = False,
    ):
    """
    Inserts a row of data from a FactRowElem.

    **Args**
        Row (nflogic.parse.FactRowElem): Element that holds the data that will be inserted;
        tablename (str): Name of the table where the data will be inserted into;
        con (sqlite3.Connection): Connection to desired database;
        close (bool): Should close the connection after the operation completes?

    **Returns** `None`

    **Raises**
        `ValueError` if *row* doesn't hold data.
        `sqlite3.OperationalError` if table doesn't exist.
    """
    create_table(con, tablename=tablename)

    dbcur = con.cursor()
    dbcur.execute(
        f"""INSERT INTO {fmt_tablename(tablename)} (
            ChaveNFe,
            DataHoraEmi,
            PagamentoTipo,
            PagamentoValor,
            TotalProdutos,
            TotalDesconto,
            TotalTributos
        ) VALUES (?,?,?,?,?,?,?);""",
        row.values,
    )
    con.commit()
    if close:
        con.close()


def insert_rows(
        parser: FactParser | FullParser,
        con: sqlite3.Connection = sqlite3.connect(DB_PATH),
        close: bool = False,
    ):
    """
    Inserts all data from a parser into the database pointed by `con`.
    The tables's names is generated from `parser.name`.

    **Args**
        Row (nflogic.parse.FactRowElem): Element that holds the data that will be inserted;
        tablename (str): Name of the table where the data will be inserted into;
        con (sqlite3.Connection): Connection to desired database;
        close (bool): Should close the connection after the operation completes?

    **Returns** `None`

    **Raises**
        `ValueError` if *row* doesn't hold data.
        `sqlite3.OperationalError` if table doesn't exist.
    """
    fact_tablename = fmt_tablename(parser.name)
    transac_tablename = f"ITENS_{fact_tablename}"

    for row in parser.data:
        if type(row) is FactRowElem:
            insert_fact_row(row=row, tablename=fact_tablename, con=con, close=False)
        if type(row) is TransacRowElem:
            insert_transac_row(row=row, tablename=transac_tablename, con=con, close=False)
    if close:
        con.close()
