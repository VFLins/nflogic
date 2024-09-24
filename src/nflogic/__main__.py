import os
from nflogic.cache import CacheHandler
from nflogic.parse import DictParser
from nflogic import db


# CONSTANTS
###############

SCRIPT_PATH = os.path.realpath(__file__)
SCRIPT_DIR = os.path.split(SCRIPT_PATH)[0]
CACHE_FILE = os.path.join(SCRIPT_DIR, "processed_files.cache")
DB_PATH = os.path.join(SCRIPT_DIR, "data", "main.sqlite")


# RUN
###############


def run_failed(path: str):
    pass


def run(path: str, retry_failed=False):
    # 1. [DONE] get list of processable files
    # 2. [DONE] get list of already processed (success and fail)
    # 3. [DONE] parse
    # 4. [DONE] insert data
    # 5. save list of already processed (success and fail)

    nfes = [
        os.path.join(path, filename)
        for filename in os.listdir(path)
        if (len(filename) > 10) and (".xml" in filename)
    ]

    success = CacheHandler("success")
    fail = CacheHandler("fail")

    ignore_keys = success.data
    if not retry_failed:
        ignore_keys = ignore_keys + fail.data

    for file in nfes:
        parser = DictParser(file)
        if parser.key in ignore_keys:
            # TODO: Info pulando arquivo já processado
            continue

        try:
            parser.parse()
            if not parser.rowdata:
                raise Exception("no data available")
            # TODO: handle tablename
            db.insert_row(row = parser.rowdata, tablename = "test", close = True)
            success.add(parser.key)

        except Exception:
            # TODO: add exception management
            fail.add(parser.key)


if __name__ == "__main__":
    pass
