from datetime import datetime
from typing import List
import sqlite3
import re
import os


# CONSTANTS
###############

SCRIPT_PATH = os.path.realpath(__file__)
SCRIPT_DIR = os.path.split(SCRIPT_PATH)[0]
DB_PATH = r"C:\databases\pdv.sqlite"


def gen_tablename(name: str):
    """Transforms strings to a format that SQLite would accept as a table name:

    1. Removes all leading numbers,
    2. Removes all special characters,
    3. Replace spaces by underscores,
    4. All letters uppercased.
    """

    return re.sub(r"[^\w\s]", "", re.sub(r"^\d+", "", name)).replace(" ", "_").upper()


def create_table(con: sqlite3.Connection, tablename: str, close: bool = False):
    """Create table with the provided name formatted by `nflogic.db.gen_tablename()`.

    **Does nothing if:**
    - table already exists
    - invalid name
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


class KeyType(str):
    def __init__(self) -> None:
        super().__init__()


class DTType(str):
    def __init__(self) -> None:
        super().__init__()


class ListOfNumbers(str):
    def __init__(self) -> None:
        super().__init__()


class FloatCoercible(str):
    def __init__(self) -> None:
        super().__init__()


class RowElem:
    def __init__(
        self,
        ChaveNFe: KeyType,
        DataHoraEmi: DTType,
        PagamentoTipo: ListOfNumbers,
        PagamentoValor: ListOfNumbers,
        TotalProdutos: FloatCoercible,
        TotalDesconto: FloatCoercible,
        TotalTributos: FloatCoercible,
    ):

        self.ChaveNFe = ChaveNFe
        self.DataHoraEmi = DataHoraEmi
        self.PagamentoTipo = PagamentoTipo
        self.PagamentoValor = PagamentoValor
        self.TotalProdutos = TotalProdutos
        self.TotalDesconto = TotalDesconto
        self.TotalTributos = TotalTributos

        self._validate_all()

    def _valid_key(self, key):
        if type(key) == str and len(key) == 44 and key.isdigit():
            return True
        return False

    def _valid_dt(self, dt):
        return type(dt) == datetime

    def _valid_list_of_numbers(self, string):
        pattern = r"^[0-9;.]+$"
        return bool(re.match(pattern, string))

    def _valid_float(self, floating_point):
        try:
            _ = float(floating_point)
            return True
        except ValueError:
            return False

    def _validate_all(self):
        types = self.__init__.__annotations__

        for var in types.keys():
            value = getattr(self, var)

            if types[var] == KeyType:
                if not self._valid_key(value):
                    raise ValueError(f"Invalid value in {var}: {value}")

            if types[var] == DTType:
                if not self._valid_dt(value):
                    raise ValueError(f"Invalid value in {var}: {value}")

            if types[var] == ListOfNumbers:
                if not self._valid_list_of_numbers(value):
                    raise ValueError(f"Invalid value in {var}: {value}")

            if types[var] == FloatCoercible:
                if not self._valid_float(value):
                    raise ValueError(f"Invalid value in {var}: {value}")


def insert_row(con: sqlite3.Connection, row: RowElem, table: str):
    """Inserts a `RowElem` as a row to `table`."""
    if not type(row) == RowElem:
        raise TypeError(f"Expected type `RowElem`, got {type(row)}")

    create_table(con, table=table)

    dbcur = con.cursor()
    dbcur.execute(
        f"""
        INSERT INTO {table} (
            ChaveNFe,
            DataHoraEmi,
            PagamentoTipo,
            PagamentoValor,
            TotalProdutos,
            TotalDesconto,
            TotalTributos
        ) VALUES (
            {row.ChaveNFe},
            {row.DataHoraEmi},
            {row.PagamentoTipo},
            {row.PagamentoValor},
            {row.TotalProdutos},
            {row.TotalDesconto},
            {row.TotalTributos}
        );
        """
    )
    con.close()
