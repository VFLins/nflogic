import pickle
import logging
from pathlib import Path
import os

from nflogic.api.parse import ParserInput, FactParser, FullParser, ParserInitError
from nflogic.api import db

SCRIPT_PATH = os.path.split(os.path.realpath(__file__))[0]
CACHE_PATH = os.path.join(SCRIPT_PATH, "cache")
LOG_PATH = os.path.join(SCRIPT_PATH, "log")
LOG_FILE = os.path.join(SCRIPT_PATH, "log", f"{__name__}.log")

for directory in [CACHE_PATH, LOG_PATH]:
    os.makedirs(directory, exist_ok=True)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

os.makedirs(os.path.split(LOG_FILE)[0], exist_ok=True)
loghandler = logging.FileHandler(filename=LOG_FILE)
logformat = logging.Formatter(fmt="%(asctime)s %(levelname)s :: %(message)s")
loghandler.setFormatter(logformat)
loghandler.setLevel(logging.INFO)
log.addHandler(loghandler)
log.propagate = False


def _get_success_cachename(full_parse: bool) -> str:
    """@public Utilitário que retorna o nome de cache de arquivos processados com
    sucesso.

    :param full_parse: Valor *booleano* indicando se deve obter o cache de sucesso em
        ambas as tabelas ou apenas na tabela *fato*.

    :return str: Uma *string* com o nome do arquivo de cache.
    """
    if full_parse:
        return "__both_table_success__"
    return "__fact_table_success__"


def valid_cachename(cachename: str) -> bool:
    """Verifica se um arquivo de cache existe *e* seu conteúdo é válido.

    :param cachename: Nome do arquivo sem extensão, use `get_cachenames` para ver os
        nomes disponíveis.

    :return: Resposta à verificação como um valor *booleano*.
    """
    cachefile_path = os.path.join(CACHE_PATH, f"{cachename}.cache")
    if not os.path.isfile(cachefile_path):
        return False
    c = CacheHandler(cachename, False)
    return c.is_valid()


def get_cachenames() -> list[str]:
    """Lista os nomes de cache disponíveis.

    :return: Uma *lista* dos nomes como *strings*."""
    cachenames = []
    for f in os.listdir(CACHE_PATH):
        fullpath = os.path.join(CACHE_PATH, f)
        no_ext_filename = os.path.splitext(f)[0]
        if os.path.isfile(fullpath) and valid_cachename(no_ext_filename):
            cachenames.append(no_ext_filename)
    return cachenames


def get_not_processed_inputs(
    filepaths: list[str],
    buy: bool,
    ignore_fails: bool,
    full_parse: bool,
) -> list[ParserInput]:
    """Cria um gerador de `.parse.ParserInput` que ainda não foram processados e
    registrados com sucesso no banco de dados.

    :param filepaths: *lista* de caminhos como *string* para os arquivos usados para o
        gerador. Estes valores farão parte da chave "path" dos `.parse.ParserInput`
        entregues pelo gerador.
    :param buy: Valor da chave "buy" dos `.parse.ParserInput` gerados. Indica se os
        arquivos devem ser processados como notas de compra ou de venda.
    :param ignore_fails: Valor *booleano* indicando se os *inputs* que falharam antes
        devem ser incluídos no gerador.
    :param full_parse: Valor *booleano*, se verdadeiro, indica que serão usados os
        dados do cache dos arquivos foram processados pelo `.parse.FullParser`, ou
        pelo `.parse.FactParser` caso contrário.
    """
    success_cache = CacheHandler(_get_success_cachename(full_parse), full_parse)
    ignore_data = success_cache.data
    if ignore_fails:
        fail_cache = CacheHandler("__could_not_parse_xml__", full_parse)
        ignore_data = ignore_data + fail_cache.data
    return (
        {"path": file, "buy": buy}
        for file in filepaths
        if {"path": file, "buy": buy} not in ignore_data
    )


def _save_successfull_fileparse(parser: FactParser | FullParser):
    """Registrar este *parser* no cache de processamentos realizados com sucesso.
    Se o *parser* tiver algum erro, não será registrado no cache.

    :param parser: Objeto .parse.FactParser ou .parse.FullParser com dados já
        processados através de .parse.FactParser.parse() ou .parse.FullParser.parse().
    """
    expected_types = [FactParser, FullParser]
    if not isinstance(parser, (FactParser, FullParser)):
        raise ValueError(
            f"Parser should be one of {expected_types=}, got {type(parser)}."
        )
    if not parser._parsed():
        return
    full_parse = True if type(parser) is FullParser else False
    cachename = _get_success_cachename(full_parse=full_parse)
    success_cache = CacheHandler(cachename=cachename, full_parse=full_parse)
    if parser.INPUTS not in success_cache.data:
        success_cache.add(parser.INPUTS)


def _save_failed_parser_init(parser: FactParser | FullParser):
    """Registra este parser no cache de *parser* que não puderam ser inicializados.
    Não fará nada se não encontrar um `.parse.ParserInitError` neste *parser*.

    :param parser: Objeto .parse.FactParser ou .parse.FullParser com dados já
        processados através de .parse.FactParser.parse() ou .parse.FullParser.parse().
    """
    expected_types = [FactParser, FullParser]
    if type(parser) not in expected_types:
        raise ValueError(
            f"Parser should be one of {expected_types=}, got {type(parser)}."
        )
    if ParserInitError not in parser.err:
        return
    full_parse = False
    if type(parser) is FullParser:
        full_parse = True
    fail_cache = CacheHandler("__could_not_parse_xml__", full_parse)
    if parser.INPUTS not in fail_cache.data:
        fail_cache.add(parser.INPUTS)


class KeyAlreadyProcessedError(Exception):
    """Erro que indica que um arquivo já foi processado antes."""

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class KeyNotFoundError(Exception):
    """Erro que indica que uma chave não pôde ser encontrada no arquivo."""

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class CacheHandler:
    def __init__(self, cachename: str, full_parse: bool = False) -> None:
        """Lida com registros do trabalho de *parsers* no cache, guardando os inputs
        para evitar que arquivos sejam processados sem necessidade.

        :param cachename: Nome do arquivo de cache sem extensão.
        :param full_parse: Valor *booleano* que sinaliza que o *parser* usado é
            `.parse.FullParser` quando verdadeiro, ou `.parse.FactParser` caso
            contrário.
        """
        if not cachename:
            self.cachename = "COULD_NOT_GET_NAME"
        else:
            # Remove bars to avoid creating an exotic path
            transform = str.maketrans("", "", r"\/")
            cachename = cachename.translate(transform)
            self.cachename = db.fmt_tablename(cachename)
        if full_parse:
            self.cachename = f"FULL {cachename}"

        self.cachefile: Path = Path(CACHE_PATH, f"{cachename}.cache")
        """Caminho para o arquivo de cache refletido por este `CacheHandler`."""

        self.data: list[ParserInput] = self._load()
        """*Lista* de `.parse.ParserInput` registrados neste cache."""

    def _load(self) -> list[ParserInput]:
        self.cachefile.touch(exist_ok=True)  # Should ensure file exists on every read.
        with open(self.cachefile, "rb") as cache:
            try:
                output: list = pickle.load(cache)
            except EOFError:
                output = []
            return output

    def _heal(self) -> None:
        size_diff = len(self._load()) - len(self.data)
        if size_diff == 0:
            return
        log.warning(
            f"Inconsistente cache sizes, starting '_heal' method from {self.cachename}.cache"
        )

        if size_diff < 0:
            # recreate file and write contents from memory
            Path(self.cachefile).touch(exist_ok=True)
            with open(self.cachefile, "wb") as cache:
                pickle.dump(obj=self.data, file=cache)
            log.info(f"Restored {abs(size_diff)} items to {self.cachename}.cache")

        else:
            # read file contents
            self.data = self._load()
            log.info(f"Restored {size_diff} items from {self.cachename}.cache")

    def _check_item(self, item: ParserInput):
        for param, typ in ParserInput.__annotations__.items():
            if type(item[param]) is not typ:
                raise TypeError(f"{item} is not of `ParserInput` type.")

    def _first_invalid_elem(self) -> ParserInput | None:
        """Returns the first item in `self.data` that is not a `nflogic.cache.ParserInput`."""
        for idx, elem in enumerate(self.data):
            if not isinstance(elem, dict):
                print(f"self.data[{idx}] is not dict-like")
                return elem
            try:
                _, _ = elem["path"], elem["buy"]
            except KeyError:
                print(f"self.data[{idx}] doesn't include required keys")
                return elem
            if type(elem["path"]) != str:
                print(f"Found key self.data[{idx}]['path']={elem['path']} not str")
                return elem
            if type(elem["buy"]) != bool:
                print(f"Found key self.data[{idx}]['buy']={elem['buy']} not bool")
                return elem
        return None

    def is_valid(self) -> bool:
        """Testa se a estrutura de seus próprios dados `CacheHandler.data` está
        formatada adequadamente como uma lista de `.parse.ParserInput`.
        """
        if type(self.data) != list:
            print("self.data is not list")
            return False
        invalid_elem = self._first_invalid_elem()
        if invalid_elem:
            return False
        return True

    def add(self, item: ParserInput) -> None:
        """Adiciona um `.parse.ParserInput` ao cache.

        :raises `KeyAlreadyProcessedError`: Se já estiver registrado.
        """
        self._check_item(item=item)
        if item in self._load():
            raise KeyAlreadyProcessedError(f"{item} já está na lista")
        self._heal()
        with open(self.cachefile, "wb") as cache:
            pickle.dump(obj=self.data + [item], file=cache)
        self.data = self._load()

    def rm(self, item: ParserInput) -> None:
        """Remove um `.parse.ParserInput` do cache.

        :raises `KeyNotFoundError`: Se não estiver registrado.
        """
        if item not in self.data:
            file_name = os.path.split(self.cachefile)[1]
            raise KeyNotFoundError(f"Arquivo não foi registrado em {file_name}")
        self._heal()
        with open(self.cachefile, "wb") as cache:
            idx = self.data.index(item)
            _ = self.data.pop(idx)
            pickle.dump(obj=self.data, file=cache)
        self.data = self._load()


class ParserManipulator:
    def __init__(
        self,
        full_parse: bool = True,
        ignore_cached_errors: bool = True,
        con: db.sqlite3.Connection = db.sqlite3.connect(db.DB_PATH),
    ):
        """Handles workflow of data collection, and cache registry of multiple parsers.
        also keep count of:

        :param full_parse: Whether the parsers should get all data from the documents
          or just the payment info.
        :param ignore_cached_errors: Wether documents that failed to process before
          should be ignored.
        :param con: A `sqlite3.Connection` object, indicating which database to connect.
        """
        self.full_parse = full_parse
        """Valor *booleano* indicando o tipo de *parser* produzido por este
        `ParserManipulator`.
        """

        self.n_parsed = 0
        """Número de documentos processados."""

        self.n_failed = 0
        """Número de documentos processados com erro."""

        self.n_skipped = 0
        """Número de documentos não processados por já estar presente no banco de
        dados.
        """

        self.n_recovered = 0
        """Número de documentos que já foram processados com erro anteriormente, mas
        que foram processados com sucesso desta vez.
        """

        self.con = con
        """Objeto `sqlite3.Connection` conectado ao banco de dados onde a
            consulta será realizada.
        """

    def add_parser(self, parser_input: ParserInput, close: bool = False):
        """Cria, testa e registra os dados de um *parser*.

        :param parser_input: Parâmetros usados para criar o *parser*.
        :param close: Valor booleano indicando se a conexão com o banco de dados deve ser
            fechada ao final desta consulta.
        """
        parser = self._test_return_parser(parser_input)
        self.n_parsed = self.n_parsed + 1
        if parser.erroed():
            self._handle_cache_registry(parser=parser)
        if db.all_rows_in_db(parser, con=self.con, close=False):
            self.n_skipped = self.n_skipped + 1
            _save_successfull_fileparse(parser)
            return
        self._handle_parser_success(parser=parser, con=self.con, close=close)

    def _get_parser(self, parser_input: ParserInput) -> FactParser | FullParser:
        """Produz um *parser* com base no valor atual de
        `ParserManipulator.full_parse`.

        :param parser_input: Parâmetros usados para criar o *parser*.

        :return `.parse.FactParser` | `.parse.FullParser`: *Parser produzido.
        """
        if self.full_parse:
            return FullParser(parser_input)
        else:
            return FactParser(parser_input)

    def _test_return_parser(self, parser_input: ParserInput) -> FactParser | FullParser:
        """
        Creates a parser and returns it, add +1 to `self.n_fails` if it erroed.
        """
        parser = self._get_parser(parser_input)
        if parser.erroed():
            _save_failed_parser_init(parser)
            return parser
        parser.parse()
        if parser.erroed():
            cache_handler = CacheHandler(parser.name, self.full_parse)
            if parser.INPUTS not in cache_handler.data:
                cache_handler.add(parser.INPUTS)
        return parser

    def _remove_successful_parser_from_cache(self, parser: FactParser | FullParser):
        """Removes parser inputs from all possible cache files, does nothing if
        parser erroed or didn't parse. Add +1 to `self.n_recovered` if it was
        removed from any cache file.
        """
        self.n_recovered = self.n_recovered + 1
        if (len(parser.err) > 0) or (len(parser.data) == 0):
            return
        init_cache, parse_cache = self._get_cache_handlers(parser=parser)
        if parser.INPUTS in init_cache.data:
            init_cache.rm(parser.INPUTS)
        if parser.INPUTS in parse_cache.data:
            parse_cache.rm(parser.INPUTS)

    def _add_failed_parser_to_cache(self, parser: FactParser | FullParser):
        """Adds parser inputs to all possible cache files, does nothing if parser
        parsed and doesn't hold any errors. Add +1 to `self.n_failed` if it was added
        to any cache file.
        """
        self.n_failed = self.n_failed + 1
        if (len(parser.err) == 0) or (len(parser.data) > 0):
            return
        init_cache, parse_cache = self._get_cache_handlers(parser=parser)
        err_cls = [type(err) for err in parser.err]
        if (parser.INPUTS not in init_cache.data) and (ParserInitError in err_cls):
            init_cache.add(parser.INPUTS)
        has_non_init_err = any(type(err) != ParserInitError for err in parser.err)
        if (parser.INPUTS not in parse_cache.data) and has_non_init_err:
            parse_cache.add(parser.INPUTS)

    def _get_cache_handlers(self, parser: FactParser | FullParser | None):
        """Return cache handlers for the provided parser, the first to handle
        initialization errors, and the second for any other error type.
        """
        init_fail_cache = CacheHandler("__could_not_parse_xml__", self.full_parse)
        parse_fail_cache = CacheHandler(parser.name, self.full_parse)
        return init_fail_cache, parse_fail_cache

    def _handle_parser_success(
        self,
        parser: FactParser | FullParser,
        con: db.sqlite3.Connection = db.sqlite3.connect(db.DB_PATH),
        close: bool = False,
    ):
        db.insert_rows(parser=parser, con=con, close=close)
        _save_successfull_fileparse(parser)
        self._remove_successful_parser_from_cache(parser)

    def _handle_cache_registry(self, parser: FactParser | FullParser):
        init_fail_cache, parse_fail_cache = self._get_cache_handlers(parser=parser)
        in_init_fail_cache: bool = parser.INPUTS in init_fail_cache.data
        in_parse_fail_cache: bool = parser.INPUTS in parse_fail_cache.data
        if in_init_fail_cache or in_parse_fail_cache:
            self._remove_successful_parser_from_cache(parser=parser)
        else:
            self._add_failed_parser_to_cache(parser=parser)
