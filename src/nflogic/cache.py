import pickle
from os import path, makedirs


SCRIPT_PATH = path.split(path.realpath(__file__))[0]
CACHE_PATH = path.join(SCRIPT_PATH, "cache")
CACHE_TYPES = ["passed", "failed", "keys"]

makedirs(CACHE_PATH, exist_ok=True)


class AlreadyProcessedError(Exception):
    """Indica que um arquivo já foi processado antes."""
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class CacheHandler:
    def __init__(self, cachename: str) -> None:
        self.cachefile = path.join(CACHE_PATH, cachename + ".cache")
        if not path.exists(self.cachefile):
            with open(self.cachefile, "x"): pass
        self.data = self.load()

    def load(self) -> list:
        with open(self.cachefile, "rb") as cache:
            try:
                return pickle.load(cache)
            except EOFError:
                return []

    def rm(self, item) -> None:
        cached = self.load()

        if item not in cached:
            raise FileNotFoundError(f"Arquivo não foi registrado em {self.cachefile}")

        if len(self.data) != len(cached):
            self._heal()

        with open(self.cachefile, "wb") as cache:
            idx = self.data.index(item)
            _ = self.data.pop(idx)
            pickle.dump(obj=self.data, file=cache)

        self.data = self.load()


    def add(self, item) -> None:
        cached = self.load()

        if item in cached:
            raise AlreadyProcessedError("Este arquivo já foi processado antes")

        if len(self.data) != len(cached):
            self._heal()

        with open(self.cachefile, "wb") as cache:
            pickle.dump(obj=self.data + [item], file=cache)

        self.data = self.load()

    def _heal(self) -> None:
        # TODO: Warn inconsistent sizes
        cached = self.load()
        count = 0
        for item in cached:
            if item not in self.data:
                self.data.append(item)
                count = count + 1
        # TODO: Info how many items were restored from cache to memory