import os
from nflogic import db, cache, parse


# CONSTANTS
###############

SCRIPT_PATH = os.path.realpath(__file__)
SCRIPT_DIR = os.path.split(SCRIPT_PATH)[0]
DB_PATH = os.path.join(SCRIPT_DIR, "data", "main.sqlite")


# FEATURES
###############


def _diagnose():
    pass


def run(path: str, buy: bool, retry_failed: bool = False):
    # 1. [DONE] get list of processable files
    # 2. [DONE] get list of already processed (success and fail)
    # 3. [DONE] parse
    # 4. [DONE] insert data
    # 5. [DONE] save list of already processed (success and fail)

    # TODO: Open a database connection at the beginning and close at the end of each run
    nfes = [
        os.path.join(path, filename)
        for filename in os.listdir(path)
        if ".xml" in filename.lower()
    ]

    # TODO: fix exception when `retry_failed=True`

    for file in nfes:
        parser = parse.FactParser({"path": file, "buy": buy})
        failed = cache.CacheHandler(parser.name)
        parser.parse()
        if parser.erroed:
            try:
                failed.add(parser.INPUTS)
            except cache.KeyAlreadyProcessedError:
                # TODO: Info pulando arquivo que já deu erro
                pass
            finally:
                continue

        if not retry_failed and parser.INPUTS in failed.data:
            # TODO: Info pulando arquivo que já deu erro
            continue

        if parser.get_key() in db.processed_keys(parser.name):
            # TODO: Info pulando arquivo já processado
            continue

        try:
            db.insert_row(parser=parser, close=False)
            if retry_failed:
                failed.rm(parser.INPUTS)

        except Exception as err:
            print(str(err))
            # TODO: add exception management
            failed.add(parser.INPUTS)


if __name__ == "__main__":
    pass
