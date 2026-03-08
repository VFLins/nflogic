import os
import sqlite3
import pandas as pd
from . import cache, parse

# CONSTANTS
###############

SCRIPT_PATH = os.path.realpath(__file__)
"""@private Path to this script file."""

SCRIPT_DIR = os.path.split(SCRIPT_PATH)[0]
"""@private Directory where nflogic's script files are located."""

CACHE_DIR = os.path.join(SCRIPT_DIR, "cache")
"""Diretório onde os arquivos de cache são armazenados, não alterar."""

DB_PATH = os.path.join(SCRIPT_DIR, "data", "main.sqlite")
"""Caminho para o arquivo do banco de dados, não alterar."""


# FEATURES
###############


def xml_files_in_dir(dir_path: str) -> list[str]:
    """Lista os arquivos XML no diretório fornecido.

    :param dir_path: Diretório onde procurar pelos arquivos.
    :return: Uma lista com os caminhos completos para todos os arquivos com a extensão
        .xml em `dir_path`.
    """
    return [
        os.path.join(dir_path, filename)
        for filename in os.listdir(dir_path)
        if os.path.splitext(filename)[1] == ".xml"
    ]


def rebuild_errors(cachename: str) -> pd.DataFrame:
    """
    Cria um *data frame* com os erros recuperados de um *cache*.

    :param cachename: Nome do arquivo de cache de onde os erros serão obtidos.
    :return: `pandas.DataFrame`
    - Colunas:
        - **Inputs** `.parse.ParserInput` usado para inicialização do *parser*.
        - **ErrorType** Lista de erros obtidos por tipo, último erro levantado no final.
        - **ErrorMessage** Lista de mensagens de erro, ordenada junto com ErrorType.
        com os respectivos tipos [`.parse.ParserInput`, `list`, `list`].
    :raises KeyError: Se `cachename` não existir, use `.cache.get_cachenames()` para
        verificar os nomes possíveis.
    """
    if cachename not in cache.get_cachenames():
        raise KeyError("Not valid cachename.")

    df_columns = ["Inputs", "ErrorType", "ErrorMessage"]
    errors_df = pd.DataFrame(columns=df_columns)
    c = cache.CacheHandler(cachename)

    def new_row_err(parser_err, parser_inputs):
        err_types = [type(err) for err in parser_err]
        err_msgs = [str(err) for err in parser_err]
        row_data = [parser_inputs, err_types, err_msgs]
        return pd.DataFrame([row_data], columns=df_columns)

    for inputs in c.data:
        # capture init error
        p = parse.FactParser(inputs)
        if p.erroed():
            new_row_errors_df = new_row_err(p.err, p.INPUTS)
            errors_df = pd.concat([errors_df, new_row_errors_df], ignore_index=True)
            continue
        # capture parse/validation error
        p.parse()
        if p.erroed():
            new_row_errors_df = new_row_err(p.err, p.INPUTS)
            errors_df = pd.concat([errors_df, new_row_errors_df], ignore_index=True)
    return errors_df


def summary_err_types(errdf: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma os dados em `errdf` agrupando-os por tipo, obtenha um *data frame*
    compatível usando `rebuild_errors()`.

    Os tipos de erros são agregados com base na etapa da *pipeline* de processamento do
    arquivo, sendo "InitFail" para erros levantados durante a inicialização do
    *parser*, "ParserFail" para erros levantados ao ler o documento, e "ValidationFail"
    para erros verificando os dados obtidos.

    :param errdf: Um *data frame* do `pandas`
    :return: `pandas.DataFrame`
    - Índice: ["InitFail", "ParseFail", "ValidationFail"]
    - Colunas:
        - **Count** Contagem de erros encontrados para cada tipo no índice.
    """
    errdf["InitFail"] = tuple(
        map(lambda x: parse.ParserInitError in x, errdf["ErrorType"])
    )
    errdf["ParseFail"] = tuple(
        map(lambda x: parse.ParserParseError in x, errdf["ErrorType"])
    )
    errdf["ValidationFail"] = tuple(
        map(lambda x: parse.ParserValidationError in x, errdf["ErrorType"])
    )
    summary = errdf.groupby(["InitFail", "ParseFail", "ValidationFail"])[
        ["InitFail"]
    ].count()
    summary.columns = ["Count"]
    return pd.DataFrame(summary)


def parse_dir(
    dir_path: str,
    buy: bool,
    full_parse: bool = False,
    ignore_cached_errors: bool = True,
    con: sqlite3.Connection = cache.db.sqlite3.connect(cache.db.DB_PATH),
):
    """
    Tenta processar todos os arquivos XML em `dir_path`.

    :param dir_path: Caminho para a pasta com os arquivos desejados.
    :param buy: Booleano indicando se as notas fiscais nesta pasta devem ser
        processadas como notas de venda.
    :param ignore_init_errors: Booleano indicando se os arquivos que falharam em outras
        execuções deve ser ignorados.
    :param con: Um objeto `sqlite3.Connection`, entregando a conexão para o banco de
        dados onde os dados coletados serão armazenados.
    """
    # TODO: Open a database connection at the beginning and close at the end of each run
    try:
        nfes = xml_files_in_dir(dir_path=dir_path)
        new_parser_inputs = cache.get_not_processed_inputs(
            filepaths=nfes,
            buy=buy,
            ignore_fails=ignore_cached_errors,
            full_parse=full_parse,
        )
        man = cache.ParserManipulator(full_parse=full_parse, con=con)
        for parser_input in new_parser_inputs:
            man.add_parser(parser_input)
            print(
                f"This might take a while... {man.n_parsed} files processed.", end="\r"
            )
    except KeyboardInterrupt:
        pass
    msgs = [f"{man.n_parsed} xml files processed in {dir_path}"]
    if man.n_parsed > 0:
        msgs = msgs + [
            f"{man.n_failed} failed",
            f"{man.n_skipped} already in the database completely or partially",
        ]
    print(*msgs, sep="\n")


def parse_cache(
    cachename: str,
    full_parse: bool = False,
    con: sqlite3.Connection = cache.db.sqlite3.connect(cache.db.DB_PATH),
):
    """Tenta processar todos os arquivos listados em um cache de arquivos que falharam
    anteriormente.

    :param cachename: Nome do arquivo de *cache*.
    :param full_parse: Booleano indicando se deve tentar processar todos os dados do
        arquivo, se `False`, deve coletar apenas as informações de pagamento e ignorar
        os produtos e serviços.
    :param con: Um objeto `sqlite3.Connection`, entregando a conexão para o banco de
        dados onde os dados coletados serão armazenados.
    """
    try:
        fails_cache = cache.CacheHandler(cachename, full_parse)
        man = cache.ParserManipulator(full_parse, con=con)
        for parser_input in fails_cache.data:
            man.add_parser(parser_input)
            print(
                f"This might take a while... {man.n_parsed} files processed.", end="\r"
            )
    except KeyboardInterrupt:
        pass
    msgs = [f"{man.n_parsed} xml files processed from {cachename}.cache"]
    if man.n_parsed > 0:
        msgs = msgs + [
            f"{man.n_failed} could not be recovered",
            f"{man.n_recovered} removed from cache, and are now in the database",
            f"{man.n_skipped} removed from cache, and were already in the database",
        ]
    print(*msgs, sep="\n")


if __name__ == "__main__":
    pass
