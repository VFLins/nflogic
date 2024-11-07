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


def check_cachename(cachename: str) -> bool:
    """Verifies if a cachename exists and return the answer as a boolean value."""
    cachefile_path = os.path.join(SCRIPT_DIR, "cache", f"{cachename}.cache")
    file_exists = os.path.isfile(cachefile_path)
    cache = cache.CacheHandler(cachename)
    return file_exists and cache.is_valid()


def get_cachenames() -> list[str]:
    """"""


def rebuild_errors(cachename: str):
    """"""



def diagnose(display_summary: bool = True):
    """Prompts the user to select a cache file, and then recreates all errors stored in the selected file, storing information about all of them in a `pandas.DataFrame`.
    
    If i isn't able to recreate the error, will give `ERROR TYPE = "<class 'NoneType'>"` and `ERROR MESSAGE = "None"`.

    **Args**
        display_summary (bool): Should print a brief summary of the errors captured to stdout?

    **Returns** `pandas.DataFrame`
        Data frame with information on all errors caputred.
    """
    cachefiles = os.listdir(cache.CACHE_PATH)
    cacheoptions = [f[:-6] for f in cachefiles if f[-6:] == ".cache"]

    cacheoptions_df = pd.DataFrame({"Cache name": cacheoptions})
    print(cacheoptions_df)
    cacheid = int(input("Insert the id number of the desired cache file: "))
    cacheselec = cacheoptions_df.iloc[cacheid, 0]

    df_columns = ["INPUTS", "ERROR TYPE", "ERROR MESSAGE", "ERROR CONTEXT"]
    errors_df = pd.DataFrame(columns=df_columns)
    c = cache.CacheHandler(cacheselec)
    for inputs in c.data:
        parser = parse.FactParser(inputs)
        parser.parse()

        if parser.erroed:
            err_raised = parser.err
            err_context = "parse"

        else:
            try:
                _ = db.RowElem(**parser.data)
                err_raised = None
                err_context = None
            except Exception as err:
                err_raised = err
                err_context = "data validation"

        new_row_errors_df = pd.DataFrame(
            [[parser.INPUTS, str(type(err_raised)), str(err_raised), err_context]],
            columns=df_columns,
        )
        errors_df = pd.concat([errors_df, new_row_errors_df], ignore_index=True)

    if display_summary:
        errors_count_df = (
            errors_df[["ERROR TYPE", "ERROR MESSAGE", "ERROR CONTEXT"]]
            .value_counts()
            .rename("COUNT")
            .reset_index()
        )

        print(
            "\n==== Summary of errors retrieved ====",
            errors_count_df.to_string(index=False),
            "========== end of summary ===========",
            sep="\n\n",
        )

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
