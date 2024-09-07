import pickle
from pathlib import Path
from os import path, makedirs


SCRIPT_PATH = path.split(path.realpath(__file__))[0]
CACHE_PATH = path.join(SCRIPT_PATH, "cache")
CACHE_TYPES = ["passed", "failed", "keys"]

makedirs(CACHE_PATH, exist_ok=True)


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
        self.cachefile = path.join(CACHE_PATH, cachename + ".cache")
        self.data = self._load()


    def _load(self) -> list:
        Path(self.cachefile).touch(exist_ok=True)
        with open(self.cachefile, "rb") as cache:
            try:
                return pickle.load(cache)
            except EOFError:
                return []


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


    def _heal(self) -> None:
        # TODO: Warn inconsistent sizes
        size_diff = len(self._load()) - len(self.data)
        if size_diff == 0:
            return
        
        if size_diff < 0:
            Path(self.cachefile).touch(exist_ok=True)
            with open(self.cachefile, "wb") as cache:
                pickle.dump(obj=self.data , file=cache)
            # TODO: Info how many items were restored from memory to cache

        else:
            self.data = self._load()
            # TODO: Info how many items were restored from cache to memory