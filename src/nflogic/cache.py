import pickle
import logging
from pathlib import Path
import os

from nflogic.parse import ParserInput


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


def valid_cachename(cachename: str) -> bool:
    """Verifies if a cachename exists *and* is valid. Return the answer as a boolean value."""
    cachefile_path = os.path.join(CACHE_PATH, f"{cachename}.cache")
    file_exists = os.path.isfile(cachefile_path)
    c = CacheHandler(cachename)
    return file_exists and c.is_valid()


def get_cachenames() -> list[str]:
    """Returns a list of available cache names."""
    cachenames = []

    for f in os.listdir(CACHE_PATH):
        fullpath = os.path.join(CACHE_PATH, f)
        no_ext_filename = os.path.splitext(f)[0]

        if os.path.isfile(fullpath) and valid_cachename(no_ext_filename):
            cachenames.append(no_ext_filename)

    return cachenames


class KeyAlreadyProcessedError(Exception):
    """Indica que um arquivo já foi processado antes."""

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class KeyNotFoundError(Exception):
    """Indica que uma chave procurada não foi encontrada."""

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class CacheHandler:
    def __init__(self, cachename: str) -> None:
        self.cachename = cachename
        self.cachefile = os.path.join(CACHE_PATH, f"{cachename}.cache")
        self.data = self._load()

    def _load(self) -> list:
        Path(self.cachefile).touch(exist_ok=True)
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
            if type(item[param]) != typ:
                raise TypeError(f"{item} is not of `ParserInput` type.")

    def _first_invalid_elem(self) -> ParserInput | None:
        """Returns the first item in `self.data` that is not a `nflogic.cache.ParserInput`."""
        for idx, elem in enumerate(self.data):
            if type(elem) != dict:
                print(f"self.data[{idx}] is not dict")
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
        """
        Test wether the structure of `self.data` is formatted as a list of `nflogic.cache.ParserInput`.
        """
        if type(self.data) != list:
            print("self.data is not list")
            return False
        invalid_elem = self._first_invalid_elem()
        if invalid_elem:
            return False
        return True

    def add(self, item: ParserInput) -> None:
        """Adds `ParserInput` element to cache, raises `KeyAlreadyProcessedError` if already on cache."""
        self._check_item(item=item)

        if item in self._load():
            raise KeyAlreadyProcessedError(f"{item} já está na lista")

        self._heal()

        with open(self.cachefile, "wb") as cache:
            pickle.dump(obj=self.data + [item], file=cache)

        self.data = self._load()

    def rm(self, item: ParserInput) -> None:
        """Removes `ParserInput` element from cache, raises `KeyNotFoundError` if not found."""
        if item not in self.data:
            file_name = os.path.split(self.cachefile)[1]
            raise KeyNotFoundError(f"Arquivo não foi registrado em {file_name}")

        self._heal()

        with open(self.cachefile, "wb") as cache:
            idx = self.data.index(item)
            _ = self.data.pop(idx)
            pickle.dump(obj=self.data, file=cache)

        self.data = self._load()
