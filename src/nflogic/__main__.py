import os
import pandas as pd
from nflogic import db, cache, parse


# CONSTANTS
###############

SCRIPT_PATH = os.path.realpath(__file__)
SCRIPT_DIR = os.path.split(SCRIPT_PATH)[0]
DB_PATH = os.path.join(SCRIPT_DIR, "data", "main.sqlite")


# FEATURES
###############


def diagnose():
    cachefiles = os.listdir(cache.CACHE_PATH)
    cacheoptions = [f[:-6] for f in cachefiles if f[-6:] == ".cache"]

    cacheoptions_df = pd.DataFrame({"Cache name": cacheoptions})
    print(cacheoptions_df)
    cacheid = int(input("Insert the id number of the desired cache file: "))
    cacheselec = cacheoptions_df.iloc[cacheid, 0]

    errors_df = pd.DataFrame({"Input": [], "Error type": [], "Error message": []})
    c = cache.CacheHandler(cacheselec)
    for inputs in c.data:
        parser = parse.FactParser(inputs)
        parser.parse()

        if parser.erroed:
            new_row_errors_df = pd.DataFrame(
                {
                    "Input": [parser.INPUTS],
                    "Error type": [str(type(parser.err))],
                    "Error message": [str(parser.err)],
                }
            )
            errors_df = pd.concat([errors_df, new_row_errors_df], ignore_index=True)
    errors_count_df = errors_df.groupby(["Error type", "Error message"])["Input"].count()
    print(errors_count_df)
    return errors_df


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
        try:
            parser = parse.FactParser({"path": file, "buy": buy})
        except:
            continue
        fails_cache = cache.CacheHandler(parser.name)
        parser.parse()
        if parser.erroed:
            try:
                fails_cache.add(parser.INPUTS)
            except cache.KeyAlreadyProcessedError:
                # TODO: Info pulando arquivo que já deu erro
                pass
            finally:
                continue

        if not retry_failed and parser.INPUTS in fails_cache.data:
            # TODO: Info pulando arquivo que já deu erro
            continue

        if parser.get_key() in db.processed_keys(parser.name):
            # TODO: Info pulando arquivo já processado
            continue

        try:
            db.insert_row(parser=parser, close=False)
            if retry_failed:
                fails_cache.rm(parser.INPUTS)

        except Exception as err:
            print(str(err))
            # TODO: add exception management
            fails_cache.add(parser.INPUTS)


if __name__ == "__main__":
    pass
