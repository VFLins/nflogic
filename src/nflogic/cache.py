import pickle
import logging
from pathlib import Path
from os import path, makedirs


SCRIPT_PATH = path.split(path.realpath(__file__))[0]
CACHE_PATH = path.join(SCRIPT_PATH, "cache")
LOG_PATH = path.join(SCRIPT_PATH, "log")
LOG_FILE = path.join(SCRIPT_PATH, "log", f"{__name__}.log")

for directory in [CACHE_PATH, LOG_PATH]:
    makedirs(directory, exist_ok=True)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

makedirs(path.split(LOG_FILE)[0], exist_ok=True)
loghandler = logging.FileHandler(filename=LOG_FILE)
logformat = logging.Formatter(fmt="%(asctime)s %(levelname)s :: %(message)s")
loghandler.setFormatter(logformat)
loghandler.setLevel(logging.INFO)
log.addHandler(loghandler)
log.propagate = False


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
        self.cachefile = path.join(CACHE_PATH, f"{cachename}.cache")
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
            f"Inconsistente cache sizes, starting '_heal' method from {self.cachename}.log"
        )

        if size_diff < 0:
            # recreate file and write contents from memory
            Path(self.cachefile).touch(exist_ok=True)
            with open(self.cachefile, "wb") as cache:
                pickle.dump(obj=self.data, file=cache)
            log.info(f"Restored {abs(size_diff)} items to {self.cachename}.log")

        else:
            # read file contents
            self.data = self._load()
            log.info(f"Restored {size_diff} items from {self.cachename}.log")

    def add(self, item) -> None:
        if item in self._load():
            raise KeyAlreadyProcessedError(f"{item} já está na lista")

        self._heal()

        with open(self.cachefile, "wb") as cache:
            pickle.dump(obj=self.data + [item], file=cache)

        self.data = self._load()

    def rm(self, item) -> None:
        if item not in self.data:
            file_name = path.split(self.cachefile)[1]
            raise KeyNotFoundError(f"Arquivo não foi registrado em {file_name}")

        self._heal()

        with open(self.cachefile, "wb") as cache:
            idx = self.data.index(item)
            _ = self.data.pop(idx)
            pickle.dump(obj=self.data, file=cache)

        self.data = self._load()
