# import sqlite3
import aiosqlite
from pathlib import Path
import re
import os

from nflogic.api.parse import FactParser, FullParser, TransacRowElem, FactRowElem

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
    """Transfoma um texto para torná-lo aceitável como nome de tabela pelo SQLite:

    1. Remove números do começo,
    2. Remove caracteres especiais,
    3. Transforma todos os espaços por *underscore*,
    4. Todas as letras maiúsculas.

    :param name: O nome que será formatado.
    :return: Uma *string* com `name` modificado.
    """

    return re.sub(r"[^\w\s]", "", re.sub(r"^\d+", "", name)).replace(" ", "_").upper()


# READ DATA
###############


async def fact_row_exists(
    row: FactRowElem,
    tablename: str,
    db_path: str | Path = DB_PATH,
) -> bool:
    """Verifica se os dados de uma `FactRowElem` já está presente em uma tabela no
    banco de dados.

    .. note:: É possível que exista mais de um documento com a mesma "ChaveNFe"
        Para garantir que não se façam dois ou mais registros apontando para a mesma
        transação, esta função apenas verifica se o valor de "ChaveNFe" já foi
        registrado na tabela indicada.

    :param row: Objeto `nflogic.api.parse.FactRowElem` com os dados que serão
        consultados, o valor da coluna "ChaveNFe" deve estar definido adequadamente.
    :param tablename: Nome da tabela onde procurar por uma linha idêntica. Não é
        tratado por `fmt_tablename` internamente.
    :param db_path: Caminho para o arquivo de banco de dados onde a consulta será
        realizada.

    :return: Valor *booleano* indicando se esta linha já está presente no banco de
        dados.
    :raises ValueError: Se os dados em **row** estão ausentes ou inválidos.
    """
    con = await aiosqlite.connect(db_path)
    try:
        cur = await con.execute(
            f"SELECT count(*) FROM {tablename} WHERE ChaveNFe=?;",
            [row.values[0]],
        )
        res = await cur.fetchone()[0]
    except aiosqlite.OperationalError:
        return False
    finally:
        await cur.close()
        await con.close()
    return bool(res)


async def transac_row_exists(
    row: TransacRowElem,
    parent_tablename: str,
    db_path: str | Path = DB_PATH,
) -> bool:
    """Verifica se os dados de `nflogic.api.parse.TransacRowElem` já está presente em
    uma tabela no banco de dados.

    :param row: Objeto `nflogic.api.parse.TransacRowElem` com os dados que serão
        consultados, o valor da coluna 'ChaveNFe' deve estar definido adequadamente.
    :param parent_tablename: Nome da tabela *fato* relacionada à tabela *transação*
        relevante no banco de dados. Não é tratado internamente por `fmt_tablename`.
    :param db_path: Caminho para o arquivo de banco de dados onde a consulta será
        realizada.

    :return: Valor *booleano* indicando se esta linha já está presente no banco de
        dados.
    :raises ValueError: Se os dados em **row** estão ausentes ou inválidos.
    """
    child_tablename = f"ITENS_{parent_tablename}"
    con = await aiosqlite.connect(db_path)
    try:
        cur = await con.execute(
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
        res = cur.fetchone()[0]
    except aiosqlite.OperationalError:
        return False
    finally:
        await cur.close()
        await con.close()
    return bool(res)


async def all_rows_in_db(
    parser: FactParser | FullParser,
    db_path: str | Path = DB_PATH,
) -> bool:
    """Verifica se todas as linhas de dados de um **parser** já foram adicionadas ao
    banco de dados.

    :param parser: Objeto `.parse.FactParser` ou `.parse.FullParser` com dados já
        processados através de `.parse.FactParser.parse()` ou
        `.parse.FullParser.parse()`.
    :param db_path: Caminho para o arquivo de banco de dados onde a consulta será
        realizada.

    :return: Valor *booleano* indicando se todas as linhas já estão presentes, também
        pode retornar `True` se o **parser** não tiver nenhuma linha de dados.
    :raise sqlite3.OperationalError: Se a tabela não existir.
    :raise ValueError: Se o *parser* não tiver um atributo `name` válido.
    """
    if len(parser.data) == 0:
        return True
    if parser.name is None:
        raise ValueError("`parser.name` must be defined before checking for it's rows.")
    tablename = fmt_tablename(parser.name)
    for row in parser.data:
        if type(row) is FactRowElem:
            if not await fact_row_exists(row=row, tablename=tablename, db_path=db_path):
                return False
        if type(row) is TransacRowElem:
            if not await transac_row_exists(row=row, parent_tablename=tablename, db_path=db_path):
                return False
    return True


async def processed_keys(
    tablename: str,
    db_path: str | Path = DB_PATH,
) -> list[str]:
    """Cria uma lista com todas as chaves já registradas em uma tabela.

    :param tablename: Nome da tabela onde procurar por uma linha idêntica. Tratado por
        `fmt_tablename` internamente.
    :param db_path: Caminho para o arquivo de banco de dados onde a consulta será
        realizada.

    :return: Uma *lista* de todas as "ChaveNFe" correspondentes como *strings*.
    :raises sqlite3.OperationalError: Se a tabela não existe.
    """
    tablename = fmt_tablename(tablename)
    con = await aiosqlite.connect(db_path)
    try:
        cur = await con.execute(f"SELECT ChaveNFe FROM {tablename}")
        res = await cur.fetchall()
    finally:
        await cur.close()
        await con.close()
    return [elem[0] for elem in res]


# CREATE/INSERT DATA
###############


async def create_fact_table(
    tablename: str,
    db_path: str | Path = DB_PATH,
):
    """Cria uma tabela *fato* com o nome fornecido formatado por `fmt_tablename()`.
    Não faz nada se a tabela já existir.

    .. warning:: Função feita para uso interno
        O uso direto desta função deve ser evitado, mesmo assim foi disponibilizado
        publicamente na API.

    :param tablename: Nome da tabela que deseja criar.
    :param db_path: Caminho para o arquivo de banco de dados onde a consulta será
        realizada.
    """
    con = await aiosqlite.connect(db_path)
    try:
        cur = await con.execute(f"""
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
            """)
        await con.commit()
    finally:
        await cur.close()
        await con.close()


async def create_transac_table(
    parent_tablename: str,
    db_path: str | Path = DB_PATH,
):
    """Cria uma tabela *transação* com o nome fornecido formatado por
    `fmt_tablename()`. Não faz nada se a tabela já existir.

    .. warning:: Função feita para uso interno
        O uso direto desta função deve ser evitado, mesmo assim foi disponibilizado
        publicamente na API.

    :param tablename: Nome da tabela que deseja criar.
    :param db_path: Caminho para o arquivo de banco de dados onde a consulta será
        realizada.
    """
    child_tablename = f"ITENS_{fmt_tablename(parent_tablename)}"
    con = await aiosqlite.connect(db_path)
    try:
        cur = await con.execute(f"""
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
            """)
        await con.commit()
    finally:
        await cur.close()
        await con.close()


async def insert_transac_row(
    row: TransacRowElem,
    parent_tablename: str,
    db_path: str | Path = DB_PATH,
):
    """Insere os dados fornecidos à uma tabela *transação* no banco de dados.

    :param row: Objeto `.parse.TransacRowElem` com os dados que devem ser inseridos.
    :param parent_tablename: Nome da tabela *fato* relacionada à tabela *transação*
        relevante no banco de dados. Não é tratado internamente por `fmt_tablename`.
    :param db_path: Caminho para o arquivo de banco de dados onde a consulta será
        realizada.

    :raises ValueError: Se algum dado necessário estiver ausente.
    :raises sqlite3.OperationalError: Se a tabela não existe.
    """
    child_tablename = f"ITENS_{parent_tablename}"
    await create_transac_table(parent_tablename=parent_tablename, db_path=db_path)
    con = await aiosqlite.connect(db_path)
    try:
        cur = await con.execute(
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
        await con.commit()
    finally:
        await cur.close()
        await con.close()


async def insert_fact_row(
    row: FactRowElem,
    tablename: str,
    db_path: str | Path = DB_PATH,
):
    """Insere os dados fornecidos à uma tabela *fato* no banco de dados.

    :param row: Objeto `.parse.TransacRowElem` com os dados que devem ser inseridos.
    :param tablename: Nome da tabela *fato* que receberá os dados. Tratado internamente
        por `fmt_tablename`.
    :param db_path: Caminho para o arquivo de banco de dados onde a consulta será
        realizada.

    :raises ValueError: Se algum dado necessário estiver ausente.
    :raises sqlite3.OperationalError: Se a tabela não existe.
    """
    await create_fact_table(tablename, db_path=db_path)
    con = await aiosqlite.connect(db_path)
    try:
        cur = await con.execute(
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
        await con.commit()
    finally:
        await cur.close()
        await con.close()


async def insert_rows(
    parser: FactParser | FullParser,
    db_path: str | Path = DB_PATH,
):
    """
    Insere todos os dados de um `.parse.FactParser` ou `.parse.FullParser` no banco de
    dados. O nome da tabela é obtido do atributo `.parse.BaseParser.name` deste parser.

    :param parser: Objeto `.parse.FactParser` ou `.parse.FullParser` com dados já
        processados através de `.parse.FactParser.parse()` ou
        `.parse.FullParser.parse()`.
    :param db_path: Caminho para o arquivo de banco de dados onde a consulta será
        realizada.

    :raise ValueError: Se uma linha de dados não tiver todos os dados necessários.
    :raise ValueError: Se o *parser* não tiver um atributo `name` válido.
    :raise sqlite3.OperationalError: Se a tabela não existe.
    """
    if parser.name is None:
        raise ValueError("`parser.name` must be defined before checking for it's rows.")
    tablename = fmt_tablename(parser.name)
    for row in parser.data:
        if type(row) is FactRowElem:
            if not await fact_row_exists(row=row, tablename=tablename, db_path=db_path):
                await insert_fact_row(row=row, tablename=tablename, db_path=db_path)
        if type(row) is TransacRowElem:
            if not await transac_row_exists(row=row, parent_tablename=tablename, db_path=db_path):
                await insert_transac_row(row=row, parent_tablename=tablename, db_path=db_path)
