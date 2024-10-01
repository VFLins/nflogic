import os
from nflogic.cache import CacheHandler
from nflogic.parse import FactParser
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


def run(path: str, buy: bool, retry_failed=False):
    # 1. [DONE] get list of processable files
    # 2. [DONE] get list of already processed (success and fail)
    # 3. [DONE] parse
    # 4. [DONE] insert data
    # 5. [DONE] save list of already processed (success and fail)

    nfes = [
        os.path.join(path, filename)
        for filename in os.listdir(path)
        if (len(filename) > 10) and (".xml" in filename)
    ]

    # TODO: fix exception when `retry_failed=True`

    for file in nfes:
        parser = FactParser(file, buy)
        failed = CacheHandler(parser.name)
        if parser.erroed:
            print(str(parser.err))
            failed.add(parser.path)

        ignore_keys = db.processed_keys(parser.name)

        if not retry_failed and parser.path in failed.data:
            # TODO: Info pulando arquivo que já deu erro
            continue

        if parser.key in ignore_keys:
            # TODO: Info pulando arquivo já processado
            continue

        try:
            parser.parse()
            if not parser.data:
                raise Exception(f"Could not fetch data from {parser.path}")
            db.insert_row(parser=parser, close=False)
            if retry_failed:
                failed.rm(parser.path)

        except Exception as err:
            print(str(err))
            # TODO: add exception management
            failed.add(parser.path)


if __name__ == "__main__":
    pass
