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


# UTILS
###############


def fmt_tablename(name: str):
    """Transforms strings to a format that SQLite would accept as a table name:

    1. Removes all leading numbers,
    2. Removes all special characters,
    3. Replace spaces by underscores,
    4. All letters uppercased.

    :param name: The name to be formatted.
    :return: The formatted `name` as `str`.
    """

    return re.sub(r"[^\w\s]", "", re.sub(r"^\d+", "", name)).replace(" ", "_").upper()


# READ DATA
###############


def fact_row_exists(
    row: FactRowElem,
    tablename: str,
    con: sqlite3.Connection = sqlite3.connect(DB_PATH),
    close: bool = False,
) -> bool:
    """
    Checks if data of a `FactRowElem` is already present in the database.

    :param row: Element that holds the data that will be checked;
    :param tablename: Name of the table where the data will be looked for;
    :param con: Connection to desired database;
    :param close: Should close the connection after the operation completes?
    :return: `bool`
    :raise: `ValueError` if *row* doesn't hold data.
    """
    dbcur = con.cursor()
    try:
        dbcur.execute(
            f"SELECT count(*) FROM {tablename} WHERE ChaveNFe=?;",
            [row.values[0]],
        )
    except sqlite3.OperationalError:
        return False
    finally:
        if close:
            con.close()
    res = dbcur.fetchone()[0]
    return bool(res)


def transac_row_exists(
    row: TransacRowElem,
    parent_tablename: str,
    con: sqlite3.Connection = sqlite3.connect(DB_PATH),
    close: bool = False,
) -> bool:
    """
    Checks if data of a `TransacRowElem` is already present in the database.

    :param row: Element that holds the data that will be checked;
    :param parent_tablename: Name fact table related to the data of interest;
    :param con: Connection to desired database;
    :param close: Should close the connection after the operation completes?

    :return: `bool` False if table or row doesn't exist, True otherwise.

    :raise: `ValueError` if *row* doesn't hold data.
    """
    child_tablename = f"ITENS_{parent_tablename}"
    dbcur = con.cursor()
    try:
        dbcur.execute(
            f"""
            SELECT count(*) FROM {child_tablename}
            WHERE
                ChaveNFe=?
                AND CodProduto=?
                AND CodBarras=?
                AND CodNCM=?
                AND CodCEST=?
                AND CodCFOP=?
                AND QuantComercial=?
                AND QuantTributavel=?
                AND UnidComercial=?
                AND UnidTributavel=?
                AND DescricaoProd=?
                AND ValorUnitario=?
                AND BaseCalcPIS=?
                AND ValorPIS=?
                AND BaseCalcCOFINS=?
                AND ValorCOFINS=?
                AND BaseCalcRetidoICMS=?
                AND ValorRetidoICMS=?
                AND ValorSubstitutoICMS=?
                AND BaseCalcEfetivoICMS=?
                AND ValorEfetivoICMS=?;
            """,
            row.values,
        )
    except sqlite3.OperationalError:
        return False
    finally:
        if close:
            con.close()
    res = dbcur.fetchone()[0]
    return bool(res)


def all_rows_in_db(
    parser: FactParser | FullParser,
    con: sqlite3.Connection = sqlite3.connect(DB_PATH),
    close: bool = False,
) -> bool:
    """
    Checks if ALL rows of a parser are present in the database. Returns `True`
    if parser doesn't hold any data.

    :param parser: Parser object holding data;
    :param con: Connection to desired database;
    :param close: Should close the connection after the operation completes?
    :return: `bool`
    :raise: `sqlite3.OperationalError` if table doesn't exist.
    """
    if len(parser.data) == 0:
        return True
    tablename = fmt_tablename(parser.name)
    for row in parser.data:
        if type(row) is FactRowElem:
            if not fact_row_exists(row=row, tablename=tablename, con=con):
                return False
        if type(row) is TransacRowElem:
            if not transac_row_exists(row=row, parent_tablename=tablename, con=con):
                return False
    if close:
        con.close()
    return True


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
    dbcur = con.cursor()
    dbcur.execute(f"SELECT ChaveNFe FROM {tablename}")
    output = dbcur.fetchall()
    if close:
        con.close()
    return [elem[0] for elem in output]


# CREATE/INSERT DATA
###############


def create_fact_table(tablename: str, con: sqlite3.Connection, close: bool = False):
    """Create *fact table* with the provided name formatted by `nflogic.db.gen_tablename()`.
    Should *not* be called directly. Does nothing if table already exists.

    **Args**
        tablename (str): Name of the table that will be created.
        con (sqlite3.Connection): Connection to desired database.
        close (bool): Should close the connection `con` after the operation completes?

    **Returns** `None`
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
    con.commit()
    if close:
        con.close()


def create_transac_table(con: sqlite3.Connection, parent_tablename: str, close: bool = False):
    """Create *transaction table* with the provided name formatted by
    `nflogic.db.gen_tablename()`. Should *not* be called directly.
    Does nothing if table already exists

    **Args**
        tablename (str): Name of it's parent table name.
        con (sqlite3.Connection): Connection to desired database.
        close (bool): Should close the connection `con` after the operation completes?

    **Returns** None
    """
    child_tablename = f"ITENS_{fmt_tablename(parent_tablename)}"
    dbcur = con.cursor()
    dbcur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {child_tablename} (
            Id INTEGER PRIMARY KEY,
            ChaveNFe TEXT NOT NULL,
            CodProduto TEXT,
            CodBarras TEXT,
            CodNCM TEXT,
            CodCEST TEXT,
            CodCFOP TEXT,
            QuantComercial REAL,
            QuantTributavel REAL,
            UnidComercial TEXT,
            UnidTributavel TEXT,
            DescricaoProd TEXT,
            ValorUnitario REAL,
            BaseCalcPIS REAL,
            ValorPIS REAL,
            BaseCalcCOFINS REAL,
            ValorCOFINS REAL,
            BaseCalcRetidoICMS REAL,
            ValorRetidoICMS REAL,
            ValorSubstitutoICMS REAL,
            BaseCalcEfetivoICMS REAL,
            ValorEfetivoICMS REAL,
            FOREIGN KEY (ChaveNFe) REFERENCES {parent_tablename}(ChaveNFe)
        );
        """
    )
    con.commit()
    if close:
        con.close()


def insert_transac_row(
    row: TransacRowElem,
    parent_tablename: str,
    con: sqlite3.Connection = sqlite3.connect(DB_PATH),
    close: bool = False,
):
    """
    Inserts a row of data from a TransacRowElem.

    **Args**
        Row (nflogic.parse.TransacRowElem): Element that holds the data that will be inserted;
        tablename (str): Name of it's parent tablename;
        con (sqlite3.Connection): Connection to desired database;
        close (bool): Should close the connection after the operation completes?

    **Returns** `None`

    **Raises**
        `ValueError` if *row* doesn't hold data.
        `sqlite3.OperationalError` if table doesn't exist.
    """
    child_tablename = f"ITENS_{parent_tablename}"
    create_transac_table(con, parent_tablename=parent_tablename)

    dbcur = con.cursor()
    dbcur.execute(
        f"""INSERT INTO {child_tablename} (
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
                ValorEfetivoICMS
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
        tablename (str): Name of the table where the data will be inserted into, will be
          formatted before insertion;
        con (sqlite3.Connection): Connection to desired database;
        close (bool): Should close the connection after the operation completes?

    **Returns** `None`

    **Raises**
        `ValueError` if *row* doesn't hold data.
        `sqlite3.OperationalError` if table doesn't exist.
    """
    create_fact_table(tablename, con=con)

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
    tablename = fmt_tablename(parser.name)
    for row in parser.data:
        if type(row) is FactRowElem:
            if not fact_row_exists(row=row, tablename=tablename, con=con):
                insert_fact_row(row=row, tablename=tablename, con=con, close=False)
        if type(row) is TransacRowElem:
            if not transac_row_exists(row=row, parent_tablename=tablename, con=con):
                insert_transac_row(
                    row=row, parent_tablename=tablename, con=con, close=False
                )
    if close:
        con.close()
