import os
import pandas as pd
from nflogic import db, cache, parse


# CONSTANTS
###############

SCRIPT_PATH = os.path.realpath(__file__)
SCRIPT_DIR = os.path.split(SCRIPT_PATH)[0]
CACHE_DIR = os.path.join(SCRIPT_DIR, "cache")
DB_PATH = os.path.join(SCRIPT_DIR, "data", "main.sqlite")


# FEATURES
###############


def rebuild_errors(cachename: str) -> pd.DataFrame:
    """
    Creates a data frame with all the errors rebuilt from the given cache.

    **Args**
        cachename: name of the cache to retrieve errors from.

    **Returns**
        `pandas.DataFrame` with column names "Inputs", "ErrorType" and "ErrorMessage"

    **Raises**
        `KeyError` if `cachename` doesn't exist, use `nflogic.cache.get_cachenames()` to check available cache names.
    """
    df_columns = ["Inputs", "ErrorType", "ErrorMessage"]
    errors_df = pd.DataFrame(columns=df_columns)
    c = cache.CacheHandler(cachename)
    for inputs in c.data:
        p = parse.FactParser(inputs)
        p.parse()
        if p.erroed:
            new_row_errors_df = pd.DataFrame(
                [[p.INPUTS, str(type(p.err)), str(p.err)]],
                columns=df_columns,
            )
            errors_df = pd.concat([errors_df, new_row_errors_df], ignore_index=True)
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
